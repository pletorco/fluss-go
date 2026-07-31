// Package fmsg contains the Apache Fluss 0.9.1 wire messages and protocol
// registry.
//
// Most applications should use package fgo or fadm instead. This package is the
// lower-level boundary for transports, protocol tooling, and integrations that
// need generated protobuf messages. [NewRequest] and [NewResponse] validate an
// [APIKey] and negotiated version before pairing protocol metadata with a
// generated message.
//
// Generated protobuf declarations follow the schema pinned in
// third_party/apache-fluss/FlussApi.proto. The public API and error registries
// cover keys 1000 through 1059 from Apache Fluss 0.9.1. Unknown protobuf fields
// are retained when decoding responses, but APIs introduced by later Fluss
// versions are not supported until the pinned protocol inputs are updated.
//
// Package documentation marks the generated Descriptor methods as deprecated.
// Those methods are compatibility shims emitted by protoc-gen-go; the message
// types themselves and the fmsg protocol API are not deprecated. New reflection
// code should use each message's ProtoReflect method.
//
// The public Go API is experimental before v1. Applications should pin a
// release and review the project changelog before upgrading.
package fmsg
