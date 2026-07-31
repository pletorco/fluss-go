# Secure client connections

`fluss-go` can use plaintext, TLS, SASL PLAIN, or TLS and SASL PLAIN
together. Production credentials should normally use the combined mode so the
PLAIN exchange is protected in transit.

| Server listener | Client options |
| --- | --- |
| Plaintext | `WithSeedBrokers` |
| TLS | `WithSeedBrokers`, `WithTLSConfig` |
| SASL PLAIN | `WithSeedBrokers`, `WithAuthenticator` |
| TLS and SASL PLAIN | all three options |

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
	fgo.WithSeedBrokers("coordinator.example:9123"),
	fgo.WithTLSConfig(tlsConfig),
	fgo.WithAuthenticator(fgo.PlainAuthenticator(
		os.Getenv("FLUSS_SASL_USERNAME"),
		os.Getenv("FLUSS_SASL_PASSWORD"),
	)),
)
```

`WithTLSConfig` clones the supplied configuration, and every managed
coordinator or tablet connection performs its own TLS handshake. Do not set
`InsecureSkipVerify` in production. The configured roots and `ServerName` must
validate every advertised server address.

`PlainAuthenticator` is a factory rather than a shared mechanism instance.
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

`task test:integration` starts a digest-pinned Apache Fluss 0.9.1 SASL PLAIN
cluster, generates an ephemeral password, and redacts it from diagnostics. The
suite verifies successful authentication, credential rejection, ACL
authorization, connection reuse, and tablet requests. TLS configuration is
also covered by client and connection-manager unit tests; deployments must
add certificate-chain tests for their own PKI and advertised server names.
