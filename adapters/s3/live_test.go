package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

type staticCredentials struct {
	accessKey string
	secretKey string
}

func (p staticCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID: p.accessKey, SecretAccessKey: p.secretKey,
		Source: "fluss-go S3 integration test",
	}, nil
}

func TestS3AdapterLive(t *testing.T) {
	uri := os.Getenv("FLUSS_GO_S3_TEST_URI")
	sizeText := os.Getenv("FLUSS_GO_S3_TEST_SIZE")
	if uri == "" || sizeText == "" {
		t.Skip("set FLUSS_GO_S3_TEST_URI and FLUSS_GO_S3_TEST_SIZE")
	}
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil || size <= 0 {
		t.Fatalf("invalid FLUSS_GO_S3_TEST_SIZE %q", sizeText)
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	credentials := aws.CredentialsProvider(aws.AnonymousCredentials{})
	if accessKey := os.Getenv("AWS_ACCESS_KEY_ID"); accessKey != "" {
		credentials = staticCredentials{
			accessKey: accessKey, secretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}
	}
	options := []func(*awss3.Options){}
	if endpoint := os.Getenv("FLUSS_GO_S3_ENDPOINT"); endpoint != "" {
		options = append(options, func(config *awss3.Options) {
			config.BaseEndpoint = aws.String(endpoint)
			config.UsePathStyle = true
		})
	}
	reader := NewFromConfig(aws.Config{
		Region: region, Credentials: aws.NewCredentialsCache(credentials),
	}, options...)
	data, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: uri, ExpectedSize: size, MaxBytes: size,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expected := os.Getenv("FLUSS_GO_S3_TEST_SHA256"); expected != "" {
		sum := sha256.Sum256(data)
		if actual := hex.EncodeToString(sum[:]); actual != expected {
			t.Fatalf("SHA-256 = %s, want %s", actual, expected)
		}
	}
}
