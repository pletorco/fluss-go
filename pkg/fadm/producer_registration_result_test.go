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

func TestRegisterProducerOffsetsRequiresAnExplicitResult(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  *int32
		created bool
		err     error
	}{
		{"created", proto.Int32(0), true, nil},
		{"already exists", proto.Int32(1), false, nil},
		{"missing", nil, false, fgo.ErrValidation},
		{"unknown", proto.Int32(2), false, fgo.ErrValidation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			admin := newClient(&fakeRequester{coordinator: func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
				requests++
				if req.APIKey() != fmsg.APIKeyRegisterProducerOffsets {
					t.Fatalf("unexpected registration side effect: %v", req.APIKey())
				}
				resp, _ := fmsg.NewResponse(req.APIKey(), req.Version())
				resp.Message().(*fmsg.RegisterProducerOffsetsResponse).Result = tc.result
				return resp, nil
			}})
			created, err := admin.RegisterProducerOffsets(t.Context(), "producer", []ProducerTableOffsets{{
				TableID: 1, Offsets: []ProducerBucketOffset{{PartitionID: -1, Bucket: 0, Offset: 2}},
			}}, time.Minute)
			if created != tc.created || !errors.Is(err, tc.err) || requests != 1 {
				t.Fatalf("register created=%v err=%v requests=%d", created, err, requests)
			}
		})
	}
}
