// Package fgo provides the Apache Fluss data-plane client.
package fgo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/internal/transport"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed         = errors.New("fgo: client closed")
	ErrUnsupportedAPI = errors.New("fgo: server does not support API")
	ErrInvalidConfig  = errors.New("fgo: invalid client configuration")
	ErrAuthentication = errors.New("fgo: authentication failed")
)

type DialContextFunc func(context.Context, string, string) (net.Conn, error)

// Authenticator performs one Fluss Authenticate challenge exchange. An instance belongs to one
// server connection and must not be shared by concurrent connections.
type Authenticator interface {
	Protocol() string
	HasInitialResponse() bool
	Authenticate(context.Context, []byte) ([]byte, error)
	Complete() bool
	Close() error
}

// AuthenticatorFactory creates a fresh authenticator for each server connection.
type AuthenticatorFactory func() (Authenticator, error)

// AuthenticationError reports whether a failed authentication exchange may be retried on a new
// connection. Its message intentionally never includes authentication tokens or credentials.
type AuthenticationError struct {
	Err       error
	Retriable bool
}

func (e *AuthenticationError) Error() string {
	if e == nil || e.Err == nil {
		return ErrAuthentication.Error()
	}
	return fmt.Sprintf("%s: %v", ErrAuthentication, e.Err)
}

func (e *AuthenticationError) Unwrap() error { return e.Err }

func (e *AuthenticationError) Is(target error) bool { return target == ErrAuthentication }

type Option func(*config) error

type config struct {
	seeds       []string
	name        string
	version     string
	dialContext DialContextFunc
	tlsConfig   *tls.Config
	authFactory AuthenticatorFactory
	timeout     time.Duration
	limits      transport.Config
}

func WithSeedBrokers(seeds ...string) Option {
	return func(c *config) error {
		if len(seeds) == 0 {
			return fmt.Errorf("%w: no seed brokers", ErrInvalidConfig)
		}
		c.seeds = append([]string(nil), seeds...)
		return nil
	}
}

func WithClientIdentity(name, version string) Option {
	return func(c *config) error {
		if name == "" || version == "" {
			return fmt.Errorf("%w: client identity is required", ErrInvalidConfig)
		}
		c.name, c.version = name, version
		return nil
	}
}

func WithDialContext(dial DialContextFunc) Option {
	return func(c *config) error {
		if dial == nil {
			return fmt.Errorf("%w: nil dialer", ErrInvalidConfig)
		}
		c.dialContext = dial
		return nil
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *config) error {
		if tlsConfig == nil {
			return fmt.Errorf("%w: nil TLS config", ErrInvalidConfig)
		}
		c.tlsConfig = tlsConfig.Clone()
		return nil
	}
}

func WithAuthenticator(factory AuthenticatorFactory) Option {
	return func(c *config) error {
		if factory == nil {
			return fmt.Errorf("%w: nil authenticator", ErrInvalidConfig)
		}
		c.authFactory = factory
		return nil
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return fmt.Errorf("%w: non-positive dial timeout", ErrInvalidConfig)
		}
		c.timeout = timeout
		return nil
	}
}

func WithTransportLimits(limits transport.Config) Option {
	return func(c *config) error {
		c.limits = limits
		return nil
	}
}

type Client struct {
	requester fmsg.Requester
	close     func() error

	mu       sync.RWMutex
	closed   bool
	versions map[fmsg.APIKey]int16
}

func Open(ctx context.Context, options ...Option) (*Client, error) {
	cfg := config{name: "fluss-go", version: "dev", timeout: 10 * time.Second}
	for _, option := range options {
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	if len(cfg.seeds) == 0 {
		return nil, fmt.Errorf("%w: seed brokers are required", ErrInvalidConfig)
	}
	if cfg.dialContext == nil {
		dialer := net.Dialer{Timeout: cfg.timeout}
		cfg.dialContext = dialer.DialContext
	}
	conn, err := cfg.dialContext(ctx, "tcp", cfg.seeds[0])
	if err != nil {
		return nil, fmt.Errorf("fgo: dial seed: %w", err)
	}
	if cfg.tlsConfig != nil {
		tlsConn := tls.Client(conn, cfg.tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("fgo: TLS handshake: %w", err)
		}
		conn = tlsConn
	}
	requester, err := transport.New(conn, cfg.limits)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	close := func() error { return requester.Close() }
	client := newClient(requester, close)
	if err := client.negotiate(ctx, cfg.name, cfg.version); err != nil {
		_ = client.Close()
		return nil, err
	}
	if cfg.authFactory != nil {
		auth, err := cfg.authFactory()
		if err != nil {
			_ = client.Close()
			return nil, authenticationError(err, false)
		}
		client.close = closeAll(requester.Close, auth.Close)
		if err := client.authenticate(ctx, auth); err != nil {
			_ = client.Close()
			return nil, err
		}
	}
	return client, nil
}

func newClient(requester fmsg.Requester, close func() error) *Client {
	return &Client{requester: requester, close: close, versions: make(map[fmsg.APIKey]int16)}
}

func (c *Client) Requester() fmsg.Requester { return c }

func (c *Client) Request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	version, ok := c.versions[request.APIKey()]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedAPI, request.APIKey())
	}
	if err := request.SetVersion(version); err != nil {
		return nil, err
	}
	return c.requester.Request(ctx, request)
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	if c.close != nil {
		return c.close()
	}
	return nil
}

func (c *Client) negotiate(ctx context.Context, name, version string) error {
	request, err := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.ApiVersionsRequest)
	message.ClientSoftwareName = proto.String(name)
	message.ClientSoftwareVersion = proto.String(version)
	response, err := c.requester.Request(ctx, request)
	if err != nil {
		return fmt.Errorf("fgo: negotiate API versions: %w", err)
	}
	versions, ok := response.Message().(*fmsg.ApiVersionsResponse)
	if !ok {
		return fmt.Errorf("fgo: negotiate API versions: unexpected response %T", response.Message())
	}
	negotiated := make(map[fmsg.APIKey]int16)
	for _, server := range versions.ApiVersions {
		api, known := fmsg.LookupAPIKey(fmsg.APIKey(server.GetApiKey()))
		if !known || server.GetMaxVersion() < int32(api.MinVersion) || server.GetMinVersion() > int32(api.MaxVersion) {
			continue
		}
		max := min(int32(api.MaxVersion), server.GetMaxVersion())
		negotiated[api.Key] = int16(max)
	}
	if _, ok := negotiated[fmsg.APIKeyApiVersions]; !ok {
		return fmt.Errorf("%w: API_VERSIONS", ErrUnsupportedAPI)
	}
	c.mu.Lock()
	c.versions = negotiated
	c.mu.Unlock()
	return nil
}

func (c *Client) authenticate(ctx context.Context, auth Authenticator) error {
	if auth == nil || auth.Protocol() == "" {
		return authenticationError(fmt.Errorf("invalid authenticator"), false)
	}

	var token []byte
	var err error
	if auth.HasInitialResponse() {
		token, err = auth.Authenticate(ctx, nil)
		if err != nil {
			return authenticationError(err, false)
		}
		if token == nil {
			return authenticationError(fmt.Errorf("initial response is missing"), false)
		}
	}

	for step := 0; step < 16; step++ {
		response, err := c.authenticateRequest(ctx, auth.Protocol(), token)
		if err != nil {
			return authenticationError(err, isRetriableAuthenticationError(err))
		}
		if response.Challenge == nil {
			if auth.Complete() {
				return nil
			}
			return authenticationError(fmt.Errorf("server completed exchange before authenticator completed"), false)
		}

		token, err = auth.Authenticate(ctx, append([]byte(nil), response.Challenge...))
		if err != nil {
			return authenticationError(err, false)
		}
		if token == nil {
			if auth.Complete() {
				return nil
			}
			return authenticationError(fmt.Errorf("authenticator returned no response before completion"), false)
		}
	}
	return authenticationError(fmt.Errorf("authentication exchange exceeded 16 steps"), false)
}

func (c *Client) authenticateRequest(ctx context.Context, protocol string, token []byte) (*fmsg.AuthenticateResponse, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	version, ok := c.versions[fmsg.APIKeyAuthenticate]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: AUTHENTICATE", ErrUnsupportedAPI)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAuthenticate, 0)
	if err != nil {
		return nil, err
	}
	if err := request.SetVersion(version); err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.AuthenticateRequest)
	message.Protocol = proto.String(protocol)
	message.Token = append([]byte(nil), token...)
	response, err := c.requester.Request(ctx, request)
	if err != nil {
		return nil, err
	}
	authenticateResponse, ok := response.Message().(*fmsg.AuthenticateResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected authentication response %T", response.Message())
	}
	return authenticateResponse, nil
}

func authenticationError(err error, retriable bool) error {
	if err == nil {
		return nil
	}
	return &AuthenticationError{Err: err, Retriable: retriable}
}

func isRetriableAuthenticationError(err error) bool {
	var authenticationError *AuthenticationError
	return errors.As(err, &authenticationError) && authenticationError.Retriable
}

func closeAll(closers ...func() error) func() error {
	return func() error {
		var result error
		for _, close := range closers {
			if close == nil {
				continue
			}
			if err := close(); err != nil && result == nil {
				result = err
			}
		}
		return result
	}
}

func min(left, right int32) int32 {
	if left < right {
		return left
	}
	return right
}
