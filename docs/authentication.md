# Secure client connections

Apache Fluss 0.9.1 provides native `PLAINTEXT` and `SASL/PLAIN` client
listeners, but it does not provide a native TLS listener. `fluss-go` can use
TLS when every coordinator and tablet endpoint is exposed through an
application- or infrastructure-owned TLS TCP terminator. Production
credentials should normally use TLS and SASL PLAIN together so the PLAIN
exchange is protected in transit.

| Deployment path | Client options |
| --- | --- |
| Native Fluss plaintext | `WithBootstrapServers` |
| TLS terminator to Fluss plaintext | `WithBootstrapServers`, `WithTLSConfig` |
| Native Fluss SASL PLAIN | `WithBootstrapServers`, `WithAuthenticator` |
| TLS terminator to Fluss SASL PLAIN | all three options |

SASL PLAIN without TLS provides authentication but does not encrypt the
credentials or subsequent traffic. Use that mode only on a transport whose
confidentiality is provided and verified elsewhere.

## TLS and SASL PLAIN

Load trusted roots explicitly, set `ServerName` to the name in the server
certificate, and keep certificate verification enabled:

<!-- go-source: internal/docexamples/snippets_test.go tlsAndSASL -->
```go
caPEM, err := os.ReadFile(os.Getenv("FLUSS_CA_FILE"))
if err != nil {
	return err
}
roots := x509.NewCertPool()
if !roots.AppendCertsFromPEM(caPEM) {
	return errors.New("Fluss CA file contains no certificates")
}

tlsConfig := &tls.Config{
	MinVersion: tls.VersionTLS12,
	RootCAs:    roots,
	ServerName: os.Getenv("FLUSS_TLS_SERVER_NAME"),
}
client, err := fgo.Open(
	ctx,
	fgo.WithBootstrapServers("coordinator.example:9123"),
	fgo.WithTLSConfig(tlsConfig),
	fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(
		os.Getenv("FLUSS_SASL_USERNAME"),
		os.Getenv("FLUSS_SASL_PASSWORD"),
	)),
)
```

`WithTLSConfig` clones the supplied configuration, and every managed
coordinator or tablet connection performs its own TLS handshake. Do not set
`InsecureSkipVerify` in production. The configured roots and `ServerName` must
validate every advertised endpoint. A deployment may use one shared
`ServerName` only when every terminating endpoint presents a certificate valid
for that DNS name; otherwise use certificates whose identities match the
addresses selected by the deployment. fluss-go preserves standard
`crypto/tls` and `crypto/x509` verification and does not rewrite identities.

Metadata returned by Fluss contains tablet addresses. Every advertised tablet
address must therefore lead to a TLS terminator as well as the coordinator
seed; terminating only the seed permits routed data calls to bypass TLS.

`SASLPlainAuthenticator` is a factory rather than a shared mechanism instance.
Each connection receives separate credential buffers, and closing that
authenticator clears those buffers. Custom `AuthenticatorFactory`
implementations must provide the same connection-local ownership and must not
retain challenges, responses, or secrets in returned errors.

Keep credentials in a secret manager or another injected source. Do not put
them in source, command-line arguments, metrics, or logs. Authentication and
token error strings intentionally omit credentials, but application wrappers
must preserve that property.

## Authentication failures

All authentication failures match `fgo.ErrAuthentication`. Use
`errors.As` to inspect `AuthenticationError.Retriable`; a retriable result means
a fresh connection may repeat authentication, not that the same credentials
will eventually become valid:

<!-- go-source: internal/docexamples/snippets_test.go classifyAuthentication -->
```go
if err != nil {
	var authentication *fgo.AuthenticationError
	switch {
	case errors.As(err, &authentication) && authentication.Retriable:
		log.Printf("retry authentication through the bounded connection policy: %v", authentication)
	case errors.Is(err, fgo.ErrAuthentication):
		log.Printf("reject authentication configuration: %v", err)
	default:
		log.Printf("non-authentication connection failure: %v", err)
	}
}
```

An invalid username, password, mechanism, or ACL is not made safe by an
unbounded retry loop. Rotate or correct the configuration, then open a new
client. A canceled connection attempt returns the causal context error when
cancellation wins.

## Verification

`task test:integration` starts digest-pinned Apache Fluss 0.9.1 plaintext and
SASL PLAIN clusters. A digest-pinned HAProxy test dependency terminates TLS for
the coordinator and every advertised tablet. The task generates an ephemeral
CA, certificate, and passwords, removes them on exit, and redacts credentials
from diagnostics. It verifies routed admin and data operations, TLS with SASL,
unknown authorities, hostname and validity failures, protocol mismatches, and
canceled handshakes. Deployments must still test their own PKI, terminator, and
advertised-name configuration.
