package fmsg_test

import (
	"fmt"
	"log"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func ExampleNewRequest() {
	request, err := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("api=%d version=%d\n", request.APIKey(), request.Version())
	// Output:
	// api=1000 version=0
}

func ExampleLookupAPIKey() {
	metadata, ok := fmsg.LookupAPIKey(fmsg.APIKeyFetchLog)
	fmt.Printf("%s: versions %d-%d, public=%t, found=%t\n",
		metadata.Name,
		metadata.MinVersion,
		metadata.MaxVersion,
		metadata.Public,
		ok,
	)
	// Output:
	// FETCH_LOG: versions 0-0, public=true, found=true
}
