package s3_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	fluss3 "github.com/pletorco/fluss-go/adapters/s3"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func ExampleNewFromConfig() {
	// Anonymous credentials are appropriate only for a local emulator that
	// explicitly permits them. Production code should use an aws.Config loaded
	// through the official credential chain.
	config := aws.Config{
		Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
	}
	reader := fluss3.NewFromConfig(config, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String("http://localhost:9000")
		options.UsePathStyle = true
	})
	option := fgo.WithRemoteFileReader(reader, fgo.RemoteFileReadConfig{})

	fmt.Printf("%T\n", option)
	// Output: fgo.Option
}
