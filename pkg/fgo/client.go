// Package fgo provides the Apache Fluss data-plane client.
package fgo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
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
	if e != nil && e.Retriable {
		return ErrAuthentication.Error() + " (retriable)"
	}
	return ErrAuthentication.Error()
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
	retry       RetryPolicy
	observer    MetricsObserver
}

// RetryPolicy bounds automatic retries of safe, read-only requests.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     func(attempt int) time.Duration
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

func WithMetricsObserver(observer MetricsObserver) Option {
	return func(c *config) error {
		if observer == nil {
			return fmt.Errorf("%w: nil metrics observer", ErrInvalidConfig)
		}
		c.observer = observer
		return nil
	}
}

type Client struct {
	requester fmsg.Requester
	close     func() error
	manager   *connectionManager
	router    *Router
	serverID  int32
	address   string
	role      ServerRole
	observer  MetricsObserver

	mu         sync.RWMutex
	closed     bool
	versions   map[fmsg.APIKey]int16
	serverType int32
}

func Open(ctx context.Context, options ...Option) (*Client, error) {
	cfg := config{name: "fluss-go", version: "dev", timeout: 10 * time.Second, retry: RetryPolicy{MaxAttempts: 1, Backoff: func(int) time.Duration { return 0 }}}
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
	manager := newConnectionManager(cfg)
	client, err := manager.bootstrap(ctx)
	if err != nil {
		_ = manager.Close()
		return nil, err
	}
	client.manager = manager
	client.router = NewRouter(Node{ID: client.serverID, Address: client.address, Role: Coordinator}, client.fetchTableMetadata).
		WithPhysicalMetadataFetcher(client.fetchPartitionMetadata)
	return client, nil
}

func newClient(requester fmsg.Requester, close func() error) *Client {
	return &Client{requester: requester, close: close, versions: make(map[fmsg.APIKey]int16)}
}

func (c *Client) Requester() fmsg.Requester { return c }

func (c *Client) Request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	if c.manager != nil {
		return c.manager.request(ctx, Node{ID: c.serverID, Address: c.address, Role: c.role}, request)
	}
	return c.request(ctx, request)
}

// RequestTo sends a raw request to the connection for node. It is intended for protocol helpers;
// higher-level clients select the appropriate coordinator or tablet server from metadata.
func (c *Client) RequestTo(ctx context.Context, node Node, request fmsg.Request) (fmsg.Response, error) {
	if c.manager == nil {
		return nil, fmt.Errorf("%w: client does not manage server connections", ErrClosed)
	}
	return c.manager.request(ctx, node, request)
}

// RequestCoordinator sends an administrative request to the currently advertised coordinator.
func (c *Client) RequestCoordinator(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	if c.router == nil {
		return c.Request(ctx, request)
	}
	return c.RequestTo(ctx, c.router.Coordinator(), request)
}

// RequestBucket sends a bucket-scoped request to its current tablet leader. A stale metadata
// response causes one cache invalidation and one bounded reroute.
func (c *Client) RequestBucket(ctx context.Context, path PhysicalTablePath, bucket int32, request fmsg.Request) (fmsg.Response, error) {
	if c.router == nil {
		return nil, fmt.Errorf("%w: client does not manage metadata", ErrClosed)
	}
	node, err := c.router.RoutePhysical(ctx, path, bucket)
	if err != nil {
		return nil, err
	}
	response, err := c.RequestTo(ctx, node, request)
	if !errors.Is(err, ErrMetadata) {
		return response, err
	}
	c.router.InvalidatePhysical(path)
	node, refreshErr := c.router.RoutePhysical(ctx, path, bucket)
	if refreshErr != nil {
		return nil, refreshErr
	}
	return c.RequestTo(ctx, node, request)
}

func (c *Client) request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	var requestErr error
	if c.observer != nil {
		started := time.Now()
		defer func() {
			observeMetric(c.observer, MetricEvent{
				Kind: MetricRequest, Operation: MetricOperationRPC, APIKey: request.APIKey(),
				ServerRole: c.role, Duration: time.Since(started),
				Failed: requestErr != nil, ErrorClass: metricErrorClass(requestErr),
			})
		}()
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		requestErr = ErrClosed
		return nil, requestErr
	}
	version, ok := c.versions[request.APIKey()]
	c.mu.RUnlock()
	if !ok {
		requestErr = fmt.Errorf("%w: %d", ErrUnsupportedAPI, request.APIKey())
		return nil, requestErr
	}
	if err := request.SetVersion(version); err != nil {
		requestErr = err
		return nil, requestErr
	}
	response, err := c.requester.Request(ctx, request)
	if err != nil {
		requestErr = serverError(err, request.APIKey(), c.address)
		return nil, requestErr
	}
	return response, nil
}

func (c *Client) Close() error {
	if c.manager != nil {
		return c.manager.Close()
	}
	return c.shutdown()
}

func (c *Client) shutdown() error {
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
	c.serverType = versions.GetServerType()
	c.mu.Unlock()
	return nil
}

func (c *Client) authenticate(ctx context.Context, auth Authenticator) error {
	if auth == nil || auth.Protocol() == "" {
		return authenticationError(fmt.Errorf("invalid authenticator"), false)
	}
	token, err := initialAuthenticationToken(ctx, auth)
	if err != nil {
		return err
	}
	for step := 0; step < 16; step++ {
		challenge, complete, err := c.authenticationChallenge(ctx, auth, token)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		token, complete, err = authenticationResponseToken(ctx, auth, challenge)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
	}
	return authenticationError(fmt.Errorf("authentication exchange exceeded 16 steps"), false)
}

func initialAuthenticationToken(ctx context.Context, auth Authenticator) ([]byte, error) {
	if !auth.HasInitialResponse() {
		return nil, nil
	}
	token, err := auth.Authenticate(ctx, nil)
	if err != nil {
		return nil, authenticationError(err, false)
	}
	if token == nil {
		return nil, authenticationError(fmt.Errorf("initial response is missing"), false)
	}
	return token, nil
}

func (c *Client) authenticationChallenge(ctx context.Context, auth Authenticator, token []byte) ([]byte, bool, error) {
	response, err := c.authenticateRequest(ctx, auth.Protocol(), token)
	if err != nil {
		classified := serverError(err, fmsg.APIKeyAuthenticate, c.address)
		return nil, false, authenticationError(classified, isRetriableAuthenticationError(classified))
	}
	// Fluss SASL/PLAIN returns a present, empty final challenge. The Java 0.9.1
	// client treats any server response received after local completion as success.
	if auth.Complete() {
		return nil, true, nil
	}
	if response.Challenge != nil {
		return append([]byte(nil), response.Challenge...), false, nil
	}
	return nil, false, authenticationError(fmt.Errorf("server completed exchange before authenticator completed"), false)
}

func authenticationResponseToken(ctx context.Context, auth Authenticator, challenge []byte) ([]byte, bool, error) {
	token, err := auth.Authenticate(ctx, challenge)
	if err != nil {
		return nil, false, authenticationError(err, false)
	}
	if token != nil {
		return token, false, nil
	}
	if auth.Complete() {
		return nil, true, nil
	}
	return nil, false, authenticationError(fmt.Errorf("authenticator returned no response before completion"), false)
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
	if errors.As(err, &authenticationError) {
		return authenticationError.Retriable
	}
	var serverError *ServerError
	return errors.As(err, &serverError) && serverError.Retriable &&
		serverError.Code == fmsg.ErrorCodeRetriableAuthenticateException
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

func shouldReplaceConnection(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrClosed) || errors.Is(err, transport.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}
