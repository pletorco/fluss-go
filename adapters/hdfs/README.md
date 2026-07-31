# HDFS remote-file adapter

Package `adapters/hdfs` validates Fluss HDFS locations and exact byte ranges
around an application-owned HDFS client. It does not implement Hadoop RPC,
WebHDFS, Kerberos, or delegation-token decoding. Applications supply an
`hdfs.OpenFunc` that opens a read-only file with a maintained internal or
vendor-supported HDFS client, then configure the reader with
`fgo.WithRemoteFileReader`.

The opener receives a validated authority, absolute path, context, and a
private clone of the optional Fluss filesystem token. The clone remains valid
until the stream closes and is then cleared. An opener that retains credentials
beyond the call must make its own copy. Token bytes must never be included in
errors or logs.

The adapter verifies the opened file's advertised size, exposes only the
requested `io.SectionReader` range, closes the file on context cancellation,
preserves opener, read, and close errors, and implements both
`fgo.RemoteFileReader` and `fgo.RemoteFileStreamReader`.

## Dependency review

The commonly used `github.com/colinmarc/hdfs/v2` client was reviewed at
v2.4.0. It is MIT licensed and supports Hadoop 2.2+ and 3.x plus Kerberos, but
its latest release is from 2023 and its repository explicitly asks for new
maintainers. It also does not expose a supported API for injecting the opaque
Hadoop delegation token supplied by Fluss. Other available native and WebHDFS
Go clients are older or have the same maintenance and token limitations.

For those reasons fluss-go does not force an unmaintained HDFS module into the
shared dependency graph. `OpenFunc` is the reviewed boundary: protocol,
authentication, failover, and retry behavior remain owned by an external HDFS
implementation, while this package owns Fluss range, size, cancellation,
resource, and token-lifetime rules.

## Live test

`task test:hdfs` reads a file from an existing HDFS filesystem mounted on the
test host. The test is skipped unless these variables are configured:

```sh
export FLUSS_HDFS_LIVE=1
export FLUSS_HDFS_URI=hdfs://nameservice/path/to/object
export FLUSS_HDFS_MOUNT_FILE=/mnt/hdfs/path/to/object
export FLUSS_HDFS_EXPECTED_SHA256=<lowercase-hex-digest>
task test:hdfs
```

The test never writes or deletes data. Cluster provisioning, mount security,
Kerberos, and token acquisition remain environment responsibilities.
