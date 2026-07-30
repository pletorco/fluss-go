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
)

type DialContextFunc func(context.Context, string, string) (net.Conn, error)

type Authenticator func(context.Context, []byte) (protocol string, token []byte, err error)

type Option func(*config) error

type config struct {
	seeds       []string
	name        string
	version     string
	dialContext DialContextFunc
	tlsConfig   *tls.Config
	auth        Authenticator
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

func WithAuthenticator(auth Authenticator) Option {
	return func(c *config) error {
		if auth == nil {
			return fmt.Errorf("%w: nil authenticator", ErrInvalidConfig)
		}
		c.auth = auth
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
	client := newClient(requester, requester.Close)
	if err := client.negotiate(ctx, cfg.name, cfg.version); err != nil {
		_ = client.Close()
		return nil, err
	}
	if cfg.auth != nil {
		if err := client.authenticate(ctx, cfg.auth); err != nil {
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
	request, err := fmsg.NewRequest(fmsg.APIKeyAuthenticate, 0)
	if err != nil {
		return err
	}
	response, err := c.Request(ctx, request)
	if err != nil {
		return fmt.Errorf("fgo: authentication challenge: %w", err)
	}
	challenge, ok := response.Message().(*fmsg.AuthenticateResponse)
	if !ok {
		return fmt.Errorf("fgo: authentication challenge: unexpected response")
	}
	protocol, token, err := auth(ctx, append([]byte(nil), challenge.Challenge...))
	if err != nil {
		return fmt.Errorf("fgo: authenticate: %w", err)
	}
	request, err = fmsg.NewRequest(fmsg.APIKeyAuthenticate, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.AuthenticateRequest)
	message.Protocol = proto.String(protocol)
	message.Token = append([]byte(nil), token...)
	if _, err := c.Request(ctx, request); err != nil {
		return fmt.Errorf("fgo: authenticate: %w", err)
	}
	return nil
}

func min(left, right int32) int32 {
	if left < right {
		return left
	}
	return right
}
