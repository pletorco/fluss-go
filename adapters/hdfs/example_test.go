package hdfs_test

import (
	"context"
	"fmt"

	flusshdfs "github.com/pletorco/fluss-go/adapters/hdfs"
)

func ExampleNew() {
	reader, err := flusshdfs.New(func(
		context.Context,
		flusshdfs.OpenRequest,
	) (flusshdfs.File, error) {
		return nil, fmt.Errorf("example opener")
	})
	fmt.Printf("%T %v\n", reader, err)
	// Output: *hdfs.Reader <nil>
}
