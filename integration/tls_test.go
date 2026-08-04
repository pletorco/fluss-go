//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func TestFluss091TLSIntegration(t *testing.T) {
	requireEnvironment(t)
	coordinator := net.JoinHostPort("127.0.0.1", "19323")
	tlsConfig := trustedTLSConfig(t)
	t.Run("certificate verified routed data", func(t *testing.T) {
		testTLSRoutedData(t, coordinator, tlsConfig)
	})
	t.Run("TLS and SASL PLAIN", func(t *testing.T) { testTLSSASL(t, tlsConfig) })
	t.Run("certificate failures", func(t *testing.T) {
		testTLSCertificateFailures(t, coordinator, tlsConfig)
	})
	t.Run("protocol mismatches", func(t *testing.T) {
		testTLSProtocolMismatches(t, coordinator, tlsConfig)
	})
	t.Run("canceled handshake", func(t *testing.T) { testTLSCanceledHandshake(t, tlsConfig) })
}

func testTLSRoutedData(t *testing.T, coordinator string, tlsConfig *tls.Config) {
	t.Helper()
	client := openClient(t, []string{coordinator}, fgo.WithTLSConfig(tlsConfig))
	defer client.Close()
	admin, err := fadm.New(client)
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("go_tls_it_%d", time.Now().UnixNano())
	logPath := fgo.TablePath{Database: database, Table: "events"}
	kvPath := fgo.TablePath{Database: database, Table: "users"}
	if err := admin.CreateDatabase(context.Background(), database, fadm.DatabaseDescriptor{
		Comment: "fluss-go TLS integration",
	}, false); err != nil {
		t.Fatal(err)
	}
	defer dropTLSDatabase(t, admin, database, "TLS")()
	createTables(t, admin, logPath, kvPath)
	assertTLSAdvertisedTablets(t, client, logPath)
	testLogData(t, client, logPath)
	testKVData(t, client, kvPath)
}

func testTLSSASL(t *testing.T, tlsConfig *tls.Config) {
	t.Helper()
	address := net.JoinHostPort("127.0.0.1", "19423")
	client := openClient(
		t, []string{address}, fgo.WithTLSConfig(tlsConfig),
		fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(
			os.Getenv("FLUSS_SASL_USERNAME"), os.Getenv("FLUSS_SASL_PASSWORD"),
		)),
	)
	defer client.Close()
	admin, err := fadm.New(client)
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("go_tls_sasl_it_%d", time.Now().UnixNano())
	path := fgo.TablePath{Database: database, Table: "events"}
	if err := admin.CreateDatabase(context.Background(), database, fadm.DatabaseDescriptor{}, false); err != nil {
		t.Fatal(err)
	}
	defer dropTLSDatabase(t, admin, database, "TLS SASL")()
	if err := admin.CreateTable(context.Background(), path, fadm.TableDescriptor{
		Schema: fgo.Schema{Columns: []fgo.Column{
			{Name: "id", Type: fgo.IntType},
			{Name: "message", Type: fgo.StringType},
		}},
		BucketCount: 1,
	}, false); err != nil {
		t.Fatal(err)
	}
	waitForTableReady(t, admin, path)
	testLogData(t, client, path)
}

func dropTLSDatabase(t *testing.T, admin *fadm.Client, database, label string) func() {
	t.Helper()
	return func() {
		if err := admin.DropDatabase(context.Background(), database, true, true); err != nil {
			t.Errorf("cleanup %s database: %v", label, err)
		}
	}
}

func testTLSCertificateFailures(t *testing.T, coordinator string, tlsConfig *tls.Config) {
	t.Helper()
	unknown := tlsConfig.Clone()
	unknown.RootCAs = x509.NewCertPool()
	err := tlsOpenError(coordinator, unknown)
	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(err, &unknownAuthority) {
		t.Fatalf("unknown CA error = %T: %v", err, err)
	}
	wrongName := tlsConfig.Clone()
	wrongName.ServerName = "wrong.fluss.test"
	err = tlsOpenError(coordinator, wrongName)
	var hostname x509.HostnameError
	if !errors.As(err, &hostname) {
		t.Fatalf("hostname mismatch error = %T: %v", err, err)
	}
	expired := tlsConfig.Clone()
	expired.Time = func() time.Time { return time.Now().Add(48 * time.Hour) }
	err = tlsOpenError(coordinator, expired)
	var invalid x509.CertificateInvalidError
	if !errors.As(err, &invalid) || invalid.Reason != x509.Expired {
		t.Fatalf("certificate validity error = %T: %v", err, err)
	}
}

func testTLSProtocolMismatches(t *testing.T, coordinator string, tlsConfig *tls.Config) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers(coordinator), fgo.WithConnectTimeout(time.Second))
	if client != nil {
		_ = client.Close()
	}
	if err == nil {
		t.Fatal("plaintext client unexpectedly negotiated through TLS endpoint")
	}
	err = tlsOpenError(net.JoinHostPort("127.0.0.1", "19523"), tlsConfig)
	var recordHeader tls.RecordHeaderError
	if !errors.As(err, &recordHeader) {
		t.Fatalf("TLS client to plaintext endpoint error = %T: %v", err, err)
	}
}

func testTLSCanceledHandshake(t *testing.T, tlsConfig *tls.Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialed := make(chan struct{})
	var once sync.Once
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	go func() {
		<-dialed
		cancel()
	}()
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers(net.JoinHostPort("127.0.0.1", "19327")),
		fgo.WithTLSConfig(tlsConfig),
		fgo.WithDialContext(func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, dialErr := dialer.DialContext(ctx, network, address)
			once.Do(func() { close(dialed) })
			return connection, dialErr
		}),
	)
	if client != nil {
		_ = client.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled TLS handshake error = %v", err)
	}
}

func trustedTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	ca, err := os.ReadFile(os.Getenv("FLUSS_TLS_CA_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("integration CA has no certificate")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: os.Getenv("FLUSS_TLS_SERVER_NAME"),
	}
}

func tlsOpenError(address string, config *tls.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers(address),
		fgo.WithTLSConfig(config),
		fgo.WithConnectTimeout(time.Second),
	)
	if client != nil {
		_ = client.Close()
	}
	return err
}

func assertTLSAdvertisedTablets(t *testing.T, client *fgo.Client, path fgo.TablePath) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: path})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"127.0.0.1:19324": false,
		"127.0.0.1:19325": false,
		"127.0.0.1:19326": false,
	}
	for _, bucket := range buckets {
		if _, ok := want[bucket.Leader.Address]; !ok {
			t.Fatalf("bucket %d bypasses TLS terminator through %s", bucket.BucketID, bucket.Leader.Address)
		}
		want[bucket.Leader.Address] = true
	}
	for address, seen := range want {
		if !seen {
			t.Fatalf("advertised TLS tablet %s was not routed", address)
		}
	}
}
