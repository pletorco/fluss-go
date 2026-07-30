package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestRequestRoundTrip(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	connection, err := New(client, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	go func() {
		id := readRequestID(t, server)
		body, _ := proto.Marshal(&fmsg.ApiVersionsResponse{})
		writeResponse(t, server, 0, id, body)
	}()
	response, err := connection.Request(context.Background(), apiVersionsRequest(t))
	if err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if _, ok := response.Message().(*fmsg.ApiVersionsResponse); !ok {
		t.Fatalf("response type = %T", response.Message())
	}
}

func TestRemoteError(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	connection, err := New(client, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	go func() {
		id := readRequestID(t, server)
		code := int32(7)
		message := "denied"
		body, _ := proto.Marshal(&fmsg.ErrorResponse{ErrorCode: &code, ErrorMessage: &message})
		writeResponse(t, server, 1, id, body)
	}()
	_, err = connection.Request(context.Background(), apiVersionsRequest(t))
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != 7 {
		t.Fatalf("Request() error = %v, want remote error", err)
	}
}

func TestCanceledRequestDoesNotConsumeLateResponse(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	connection, err := New(client, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	go func() {
		firstID := readRequestID(t, server)
		time.Sleep(30 * time.Millisecond)
		body, _ := proto.Marshal(&fmsg.ApiVersionsResponse{})
		writeResponse(t, server, 0, firstID, body)
		id := readRequestID(t, server)
		writeResponse(t, server, 0, id, body)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = connection.Request(ctx, apiVersionsRequest(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Request() error = %v", err)
	}
	if _, err := connection.Request(context.Background(), apiVersionsRequest(t)); err != nil {
		t.Fatalf("second Request() error = %v", err)
	}
}

func FuzzHandleFrame(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{1, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, frame []byte) {
		connection := &Connection{pending: make(map[int32]chan result)}
		_ = connection.handleFrame(frame)
	})
}

func apiVersionsRequest(t *testing.T) *fmsg.MessageRequest {
	t.Helper()
	request, err := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if err != nil {
		t.Fatal(err)
	}
	message := request.Message().(*fmsg.ApiVersionsRequest)
	message.ClientSoftwareName = proto.String("test")
	message.ClientSoftwareVersion = proto.String("test")
	return request
}

func readRequestID(t *testing.T, conn net.Conn) int32 {
	t.Helper()
	var size [4]byte
	if _, err := io.ReadFull(conn, size[:]); err != nil {
		t.Error(err)
		return 0
	}
	frame := make([]byte, binary.BigEndian.Uint32(size[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Error(err)
		return 0
	}
	return int32(binary.BigEndian.Uint32(frame[4:8]))
}

func writeResponse(t *testing.T, conn net.Conn, responseType byte, id int32, body []byte) {
	t.Helper()
	frame := make([]byte, 4+responseHeaderSize+len(body))
	binary.BigEndian.PutUint32(frame, uint32(responseHeaderSize+len(body)))
	frame[4] = responseType
	binary.BigEndian.PutUint32(frame[5:], uint32(id))
	copy(frame[9:], body)
	if _, err := conn.Write(frame); err != nil {
		t.Error(err)
	}
}
