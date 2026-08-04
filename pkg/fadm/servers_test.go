package fadm

import (
	"context"
	"errors"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestGetServerNodes(t *testing.T) {
	requester := &fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		if request.APIKey() != fmsg.APIKeyGetMetadata {
			t.Fatalf("API key = %d", request.APIKey())
		}
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.MetadataRequest)
		if len(message.GetTablePath()) != 0 || len(message.GetPartitionsPath()) != 0 {
			t.Fatalf("metadata request = %#v", message)
		}
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		response.Message().(*fmsg.MetadataResponse).CoordinatorServer = adminServerNode(7, "coordinator", 9123, "")
		response.Message().(*fmsg.MetadataResponse).TabletServers = []*fmsg.PbServerNode{
			adminServerNode(3, "tablet-b", 9124, "rack-b"),
			adminServerNode(2, "tablet-a", 9123, "rack-a"),
		}
		return response, nil
	}}
	nodes, err := newClient(requester).GetServerNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 || nodes[0].ServerType != fgo.Coordinator || nodes[0].ID != 7 ||
		nodes[1].ServerType != fgo.TabletServer || nodes[1].ID != 2 || nodes[1].Rack != "rack-a" ||
		nodes[2].ID != 3 {
		t.Fatalf("nodes = %#v", nodes)
	}
	nodes[0].Host = "changed"
	again, err := newClient(requester).GetServerNodes(context.Background())
	if err != nil || again[0].Host != "coordinator" {
		t.Fatalf("second result = %#v, %v", again, err)
	}
}

func TestGetServerNodesErrors(t *testing.T) {
	sentinel := errors.New("metadata failed")
	cases := []struct {
		name        string
		coordinator func(context.Context, fmsg.Request) (fmsg.Response, error)
		target      error
	}{
		{
			name: "request",
			coordinator: func(context.Context, fmsg.Request) (fmsg.Response, error) {
				return nil, sentinel
			},
			target: sentinel,
		},
		{
			name: "response",
			coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
				response, _ := fmsg.NewResponse(fmsg.APIKeyListTables, request.Version())
				return response, nil
			},
		},
		{
			name: "missing coordinator",
			coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
				response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
				return response, nil
			},
			target: fgo.ErrMetadata,
		},
		{
			name: "invalid tablet",
			coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
				response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
				metadata := response.Message().(*fmsg.MetadataResponse)
				metadata.CoordinatorServer = adminServerNode(1, "coordinator", 9123, "")
				metadata.TabletServers = []*fmsg.PbServerNode{adminServerNode(2, "", 9123, "")}
				return response, nil
			},
			target: fgo.ErrMetadata,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := newClient(&fakeRequester{coordinator: test.coordinator}).GetServerNodes(context.Background())
			if test.target != nil && !errors.Is(err, test.target) {
				t.Fatalf("error = %v, want %v", err, test.target)
			}
			if test.target == nil && err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func adminServerNode(id int32, host string, port int32, rack string) *fmsg.PbServerNode {
	node := &fmsg.PbServerNode{NodeId: proto.Int32(id), Host: proto.String(host), Port: proto.Int32(port)}
	if rack != "" {
		node.Rack = proto.String(rack)
	}
	return node
}
