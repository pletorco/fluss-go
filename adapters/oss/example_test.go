package oss_test

import (
	"fmt"

	aliyunoss "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	flussoss "github.com/pletorco/fluss-go/adapters/oss"
)

func ExampleNewFromConfig() {
	config := aliyunoss.LoadDefaultConfig().WithRegion("cn-hangzhou")
	reader := flussoss.NewFromConfig(config)
	fmt.Println(reader != nil)
	// Output: true
}
