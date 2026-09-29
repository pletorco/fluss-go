package fgo

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/internal/transport"
)

// Option configures a [Client].
type Option func(*config) error

type config struct {
	bootstrapServers  []string
	name              string
	version           string
	dialContext       DialContextFunc
	tlsConfig         *tls.Config
	authFactory       AuthenticatorFactory
	connectTimeout    time.Duration
	limits            transport.Config
	retry             RetryPolicy
	observer          MetricsObserver
	tokens            securityTokenSettings
	dynamicPartitions *DynamicPartitionCreationConfig
	snapshotProvider  SnapshotBatchProvider
	remoteFiles       remoteFileSettings
}

// RetryPolicy bounds automatic retries of safe, read-only requests.
type RetryPolicy struct {
	// MaxAttempts includes the initial request and must be positive.
	MaxAttempts int
	// Backoff returns the delay before the numbered retry attempt.
	Backoff func(attempt int) time.Duration
}

// WithBootstrapServers sets coordinator bootstrap addresses.
func WithBootstrapServers(bootstrapServers ...string) Option {
	return func(c *config) error {
		if len(bootstrapServers) == 0 {
			return fmt.Errorf("%w: no bootstrap servers", ErrInvalidConfig)
		}
		c.bootstrapServers = append([]string(nil), bootstrapServers...)
		return nil
	}
}

// WithClientSoftware sets the software name and version sent during API negotiation.
func WithClientSoftware(name, version string) Option {
	return func(c *config) error {
		if name == "" || version == "" {
			return fmt.Errorf("%w: client software name and version are required", ErrInvalidConfig)
		}
		c.name, c.version = name, version
		return nil
	}
}

// WithDialContext replaces the network dialer.
func WithDialContext(dial DialContextFunc) Option {
	return func(c *config) error {
		if dial == nil {
			return fmt.Errorf("%w: nil dialer", ErrInvalidConfig)
		}
		c.dialContext = dial
		return nil
	}
}

// WithTLSConfig enables TLS using a clone of tlsConfig.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *config) error {
		if tlsConfig == nil {
			return fmt.Errorf("%w: nil TLS config", ErrInvalidConfig)
		}
		c.tlsConfig = tlsConfig.Clone()
		return nil
	}
}

// WithAuthenticator enables authentication with a fresh mechanism instance
// for each server connection.
func WithAuthenticator(factory AuthenticatorFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return fmt.Errorf("%w: nil authenticator", ErrInvalidConfig)
		}
		c.authFactory = factory
		return nil
	}
}

// WithConnectTimeout bounds each connection attempt.
func WithConnectTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: non-positive connect timeout", ErrInvalidConfig)
		}
		c.connectTimeout = timeout
		return nil
	}
}

// WithTransportLimits sets request and response frame limits.
func WithTransportLimits(limits transport.Config) Option {
	return func(c *config) error {
		if limits.MaxFrameSize != 0 && limits.MaxFrameSize < 5 {
			return fmt.Errorf("%w: maximum frame size must be at least 5 bytes", ErrInvalidConfig)
		}
		if limits.MaxInFlight < 0 {
			return fmt.Errorf("%w: negative maximum in-flight requests", ErrInvalidConfig)
		}
		c.limits = limits
		return nil
	}
}

// WithRetryPolicy configures bounded retries for safe read-only requests.
// Mutations are not blindly retried.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(c *config) error {
		if policy.MaxAttempts < 1 {
			return fmt.Errorf("%w: retry attempts must be positive", ErrInvalidConfig)
		}
		if policy.Backoff == nil {
			policy.Backoff = func(int) time.Duration { return 0 }
		}
		c.retry = policy
		return nil
	}
}

// WithMetricsObserver registers a synchronous bounded-cardinality event
// observer. Observer panics are isolated from client operations.
func WithMetricsObserver(observer MetricsObserver) Option {
	return func(c *config) error {
		if observer == nil {
			return fmt.Errorf("%w: nil metrics observer", ErrInvalidConfig)
		}
		c.observer = observer
		return nil
	}
}
