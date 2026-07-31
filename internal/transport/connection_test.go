package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

var errTestRequest = errors.New("test request failure")
var errTestResponse = errors.New("test response failure")

type testRequest struct {
	body        []byte
	marshalErr  error
	newResponse func() fmsg.Response
}

func (r *testRequest) APIKey() fmsg.APIKey        { return fmsg.APIKeyApiVersions }
func (r *testRequest) Version() int16             { return 0 }
func (r *testRequest) SetVersion(int16) error     { return nil }
func (r *testRequest) NewResponse() fmsg.Response { return r.newResponse() }
func (r *testRequest) Marshal() ([]byte, error)   { return r.body, r.marshalErr }

type testResponse struct {
	unmarshalErr error
}

func (r *testResponse) APIKey() fmsg.APIKey    { return fmsg.APIKeyApiVersions }
func (r *testResponse) Version() int16         { return 0 }
func (r *testResponse) Unmarshal([]byte) error { return r.unmarshalErr }
func (r *testResponse) Message() proto.Message { return nil }

type writeConn struct {
	write func([]byte) (int, error)
}

func (c writeConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c writeConn) Write(value []byte) (int, error) { return c.write(value) }
func (writeConn) Close() error                      { return nil }
func (writeConn) LocalAddr() net.Addr               { return testAddr("local") }
func (writeConn) RemoteAddr() net.Addr              { return testAddr("remote") }
func (writeConn) SetDeadline(time.Time) error       { return nil }
func (writeConn) SetReadDeadline(time.Time) error   { return nil }
func (writeConn) SetWriteDeadline(time.Time) error  { return nil }

type partialBlockingWriteConn struct {
	partial      chan struct{}
	interrupted  chan struct{}
	partialOnce  sync.Once
	interruptOne sync.Once
}

func (c *partialBlockingWriteConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *partialBlockingWriteConn) Write(value []byte) (int, error) {
	written := 0
	c.partialOnce.Do(func() {
		written = len(value) / 2
		close(c.partial)
	})
	if written != 0 {
		return written, nil
	}
	<-c.interrupted
	return 0, os.ErrDeadlineExceeded
}
func (*partialBlockingWriteConn) Close() error                { return nil }
func (*partialBlockingWriteConn) LocalAddr() net.Addr         { return testAddr("local") }
func (*partialBlockingWriteConn) RemoteAddr() net.Addr        { return testAddr("remote") }
func (*partialBlockingWriteConn) SetDeadline(time.Time) error { return nil }
func (*partialBlockingWriteConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *partialBlockingWriteConn) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		return nil
	}
	interrupt := func() {
		c.interruptOne.Do(func() { close(c.interrupted) })
	}
	if delay := time.Until(deadline); delay > 0 {
		time.AfterFunc(delay, interrupt)
	} else {
		interrupt()
	}
	return nil
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

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

func TestRequestRejectsInvalidInputBeforeWriting(t *testing.T) {
	connection := &Connection{
		maxFrame: requestHeaderSize, sem: make(chan struct{}, 1),
		pending: make(map[int32]chan result), done: make(chan struct{}),
	}
	if _, err := connection.Request(context.Background(), nil); !errors.Is(err, fmsg.ErrInvalidArgument) {
		t.Fatalf("nil Request() error = %v", err)
	}
	request := &testRequest{marshalErr: errTestRequest, newResponse: func() fmsg.Response { return &testResponse{} }}
	if _, err := connection.Request(context.Background(), request); !errors.Is(err, errTestRequest) {
		t.Fatalf("marshal Request() error = %v", err)
	}
	request.marshalErr = nil
	request.body = []byte{1}
	if _, err := connection.Request(context.Background(), request); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized Request() error = %v", err)
	}
}

func TestRequestStopsWhileWaitingForCapacity(t *testing.T) {
	connection := &Connection{
		maxFrame: defaultMaxFrame, sem: make(chan struct{}, 1),
		pending: make(map[int32]chan result), done: make(chan struct{}),
	}
	connection.sem <- struct{}{}
	request := &testRequest{newResponse: func() fmsg.Response { return &testResponse{} }}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connection.Request(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Request() error = %v", err)
	}

	close(connection.done)
	if _, err := connection.Request(context.Background(), request); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Request() error = %v", err)
	}
}

func TestRequestReportsWriteAndResponseFailures(t *testing.T) {
	t.Run("short write", func(t *testing.T) {
		connection := &Connection{
			conn:     writeConn{write: func([]byte) (int, error) { return 0, nil }},
			maxFrame: defaultMaxFrame, sem: make(chan struct{}, 1),
			pending: make(map[int32]chan result), done: make(chan struct{}),
		}
		request := &testRequest{newResponse: func() fmsg.Response { return &testResponse{} }}
		if _, err := connection.Request(context.Background(), request); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("Request() error = %v", err)
		}
	})

	for _, test := range []struct {
		name     string
		response func() fmsg.Response
		want     error
	}{
		{name: "missing constructor", response: func() fmsg.Response { return nil }, want: ErrMalformed},
		{name: "decode failure", response: func() fmsg.Response {
			return &testResponse{unmarshalErr: errTestResponse}
		}, want: errTestResponse},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			connection, err := New(client, Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			go func() {
				id := readRequestID(t, server)
				writeResponse(t, server, 0, id, nil)
			}()
			request := &testRequest{newResponse: test.response}
			if _, err := connection.Request(context.Background(), request); !errors.Is(err, test.want) {
				t.Fatalf("Request() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRequestCancellationInterruptsBlockedWrite(t *testing.T) {
	for _, test := range []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		want    error
	}{
		{
			name: "cancel",
			context: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			want: context.Canceled,
		},
		{
			name: "deadline",
			context: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 20*time.Millisecond)
			},
			want: context.DeadlineExceeded,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &partialBlockingWriteConn{
				partial: make(chan struct{}), interrupted: make(chan struct{}),
			}
			connection := &Connection{
				conn: conn, maxFrame: defaultMaxFrame, sem: make(chan struct{}, 1),
				pending: make(map[int32]chan result), done: make(chan struct{}),
			}
			ctx, cancel := test.context()
			defer cancel()
			completed := make(chan error, 1)
			go func() {
				_, err := connection.Request(ctx, apiVersionsRequest(t))
				completed <- err
			}()
			<-conn.partial
			if errors.Is(test.want, context.Canceled) {
				cancel()
			}
			select {
			case err := <-completed:
				if !errors.Is(err, test.want) {
					t.Fatalf("Request() error = %v, want %v", err, test.want)
				}
			case <-time.After(time.Second):
				t.Fatal("Request did not stop after context completion")
			}
			if _, err := connection.Request(
				context.Background(), apiVersionsRequest(t),
			); !errors.Is(err, test.want) {
				t.Fatalf("request after interrupted frame error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRequestClearsSuccessfulWriteDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	connection, err := New(client, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	go func() {
		for range 2 {
			id := readRequestID(t, server)
			body, _ := proto.Marshal(&fmsg.ApiVersionsResponse{})
			writeResponse(t, server, 0, id, body)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := connection.Request(ctx, apiVersionsRequest(t)); err != nil {
		t.Fatalf("deadline Request() error = %v", err)
	}
	if _, err := connection.Request(context.Background(), apiVersionsRequest(t)); err != nil {
		t.Fatalf("request after cleared deadline error = %v", err)
	}
}

func TestRequestReportsReaderFailureToPendingCall(t *testing.T) {
	client, server := net.Pipe()
	connection, err := New(client, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	go func() {
		_ = readRequestID(t, server)
		_ = server.Close()
	}()
	request := &testRequest{newResponse: func() fmsg.Response { return &testResponse{} }}
	if _, err := connection.Request(context.Background(), request); !errors.Is(err, io.EOF) {
		t.Fatalf("Request() error = %v, want EOF", err)
	}
}

func TestRegisterHandlesClosureCollisionAndIDWrap(t *testing.T) {
	connection := &Connection{pending: make(map[int32]chan result)}
	connection.nextID = math.MaxInt32
	connection.pending[1] = make(chan result, 1)
	id, _, err := connection.register()
	if err != nil || id != 2 {
		t.Fatalf("register() = %d, %v, want 2, nil", id, err)
	}
	connection.unregister(id)
	if _, found := connection.pending[id]; found {
		t.Fatal("unregister() retained request")
	}

	connection.closed = true
	if _, _, err := connection.register(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed register() error = %v", err)
	}
	connection.closeErr = errTestRequest
	if _, _, err := connection.register(); !errors.Is(err, errTestRequest) {
		t.Fatalf("failed register() error = %v", err)
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

func TestConnectionRejectsInvalidInput(t *testing.T) {
	if _, err := New(nil, Config{}); !errors.Is(err, fmsg.ErrInvalidArgument) {
		t.Fatalf("New(nil) error = %v", err)
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	if _, err := New(left, Config{MaxFrameSize: 1}); !errors.Is(err, fmsg.ErrInvalidArgument) {
		t.Fatalf("New(invalid limits) error = %v", err)
	}
}

func TestReadFrameAndHandleFrameFailures(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	connection := &Connection{conn: left, maxFrame: 8, pending: make(map[int32]chan result)}
	go func() {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], 9)
		_, _ = right.Write(header[:])
	}()
	if _, err := connection.readFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("readFrame() error = %v", err)
	}
	left.Close()
	if _, err := connection.readFrame(); err == nil {
		t.Fatal("readFrame() closed connection error = nil")
	}
	left, right = net.Pipe()
	defer left.Close()
	defer right.Close()
	connection.conn = left
	go func() {
		var header [4]byte
		_, _ = right.Write(header[:])
	}()
	if _, err := connection.readFrame(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("readFrame() zero-size error = %v", err)
	}
	for _, frame := range [][]byte{
		nil,
		{99},
		{0},
		{1, 0},
		{2, 0xff},
	} {
		if err := connection.handleFrame(frame); err == nil {
			t.Fatalf("handleFrame(%v) error = nil", frame)
		}
	}
	if got := (&RemoteError{Code: 3}).Error(); got != "transport: remote error 3" {
		t.Fatalf("RemoteError error = %q", got)
	}
	if got := (&RemoteError{Code: 3, Message: "remote"}).Error(); got != "transport: remote error 3: remote" {
		t.Fatalf("RemoteError message = %q", got)
	}
}

func TestReadFrameRejectsTruncatedBody(t *testing.T) {
	left, right := net.Pipe()
	connection := &Connection{conn: left, maxFrame: 8}
	go func() {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], 3)
		_, _ = right.Write(header[:])
		_, _ = right.Write([]byte{1})
		_ = right.Close()
	}()
	if _, err := connection.readFrame(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("readFrame() error = %v, want unexpected EOF", err)
	}
	_ = left.Close()
}

func TestConnectionErrAndRepeatedFailure(t *testing.T) {
	connection := &Connection{
		conn:    writeConn{write: func(value []byte) (int, error) { return len(value), nil }},
		pending: make(map[int32]chan result), done: make(chan struct{}),
	}
	if err := connection.err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("err() = %v", err)
	}
	connection.fail(errTestRequest)
	connection.fail(errors.New("replacement"))
	if err := connection.err(); !errors.Is(err, errTestRequest) {
		t.Fatalf("err() = %v, want original failure", err)
	}
}

func TestRequestCompletionOwnershipPreventsBlockingFailure(t *testing.T) {
	t.Run("response before failure", func(t *testing.T) {
		connection := newCompletionTestConnection()
		id, resultCh, err := connection.register()
		if err != nil {
			t.Fatal(err)
		}
		connection.deliver(id, result{body: []byte("response")})
		failed := make(chan struct{})
		go func() {
			connection.fail(errTestRequest)
			close(failed)
		}()
		select {
		case <-failed:
		case <-time.After(time.Second):
			t.Fatal("failure blocked after response delivery")
		}
		outcome := <-resultCh
		if string(outcome.body) != "response" || outcome.err != nil {
			t.Fatalf("outcome = %#v", outcome)
		}
	})

	t.Run("failure before response", func(t *testing.T) {
		connection := newCompletionTestConnection()
		id, resultCh, err := connection.register()
		if err != nil {
			t.Fatal(err)
		}
		connection.fail(errTestRequest)
		delivered := make(chan struct{})
		go func() {
			connection.deliver(id, result{body: []byte("late")})
			close(delivered)
		}()
		select {
		case <-delivered:
		case <-time.After(time.Second):
			t.Fatal("late delivery blocked after failure")
		}
		outcome := <-resultCh
		if !errors.Is(outcome.err, errTestRequest) || outcome.body != nil {
			t.Fatalf("outcome = %#v", outcome)
		}
	})
}

func TestRequestCompletionRace(t *testing.T) {
	for range 1_000 {
		connection := newCompletionTestConnection()
		id, resultCh, err := connection.register()
		if err != nil {
			t.Fatal(err)
		}
		var completed sync.WaitGroup
		completed.Add(3)
		go func() {
			defer completed.Done()
			connection.deliver(id, result{body: []byte("response")})
		}()
		go func() {
			defer completed.Done()
			connection.unregister(id)
		}()
		go func() {
			defer completed.Done()
			connection.fail(errTestRequest)
		}()
		done := make(chan struct{})
		go func() {
			completed.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("completion race blocked")
		}
		select {
		case <-resultCh:
			select {
			case duplicate := <-resultCh:
				t.Fatalf("duplicate result = %#v", duplicate)
			default:
			}
		default:
		}
	}
}

func newCompletionTestConnection() *Connection {
	return &Connection{
		conn: writeConn{write: func(value []byte) (int, error) {
			return len(value), nil
		}},
		pending: make(map[int32]chan result),
		done:    make(chan struct{}),
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
	firstRead := make(chan struct{})
	releaseLate := make(chan struct{})
	go func() {
		firstID := readRequestID(t, server)
		close(firstRead)
		<-releaseLate
		body, _ := proto.Marshal(&fmsg.ApiVersionsResponse{})
		writeResponse(t, server, 0, firstID, body)
		id := readRequestID(t, server)
		writeResponse(t, server, 0, id, body)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, requestErr := connection.Request(ctx, apiVersionsRequest(t))
		result <- requestErr
	}()
	<-firstRead
	cancel()
	if err = <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Request() error = %v", err)
	}
	close(releaseLate)
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
