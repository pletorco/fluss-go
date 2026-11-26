package fadm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestWaitRebalanceValidatesStatusAndReturnsTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status *int32
		err    error
	}{
		{"failed", proto.Int32(2), nil},
		{"completed", proto.Int32(3), nil},
		{"canceled", proto.Int32(4), nil},
		{"timeout", proto.Int32(5), nil},
		{"missing", nil, fgo.ErrValidation},
		{"unknown", proto.Int32(6), fgo.ErrValidation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admin := newClient(&fakeRequester{coordinator: func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
				if request.APIKey() != fmsg.APIKeyListRebalanceProgress {
					t.Fatalf("unexpected coordinator request: %v", request.APIKey())
				}
				response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
				message := response.Message().(*fmsg.ListRebalanceProgressResponse)
				message.RebalanceId, message.RebalanceStatus = proto.String("id"), tc.status
				return response, nil
			}})
			got, err := admin.WaitRebalance(t.Context(), "id", time.Millisecond)
			if !errors.Is(err, tc.err) || tc.err == nil && got.Status != *tc.status {
				t.Fatalf("WaitRebalance = %+v, %v; want status %v error %v", got, err, tc.status, tc.err)
			}
		})
	}
}
