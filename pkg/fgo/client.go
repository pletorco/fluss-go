package fgo

import (
	"context"
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

// Client lifecycle, protocol support, configuration, and authentication errors.
var (
	ErrClosed         = errors.New("fgo: client closed")
	ErrUnsupportedAPI = errors.New("fgo: server does not support API")
	ErrInvalidConfig  = errors.New("fgo: invalid client configuration")
	ErrAuthentication = errors.New("fgo: authentication failed")
)

// Client owns negotiated coordinator and tablet connections.
// A Client is safe for concurrent use. Close child resources before closing it.
type Client struct {
	requester        fmsg.Requester
	close            func() error
	manager          *connectionManager
	router           *Router
	serverID         int32
	address          string
	serverType       ServerType
	observer         MetricsObserver
	tokenManager     *securityTokenManager
	partitionCreator *dynamicPartitionCreator
	snapshotProvider SnapshotBatchProvider
	remoteFiles      remoteFileSettings
	schemas          *schemaCache

	mu           sync.RWMutex
	closed       bool
	versions     map[fmsg.APIKey]int16
	serverTypeID int32
}

// Open connects to a coordinator, negotiates protocol versions, and returns a
// shared client.
func Open(ctx context.Context, options ...Option) (*Client, error) {
	cfg := config{name: "fluss-go", version: "dev", connectTimeout: 10 * time.Second, retry: RetryPolicy{MaxAttempts: 1, Backoff: func(int) time.Duration { return 0 }}}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil client option", ErrInvalidConfig)
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	if len(cfg.bootstrapServers) == 0 {
		return nil, fmt.Errorf("%w: bootstrap servers are required", ErrInvalidConfig)
	}
	if cfg.dialContext == nil {
		dialer := net.Dialer{Timeout: cfg.connectTimeout}
		cfg.dialContext = dialer.DialContext
	}
	manager := newConnectionManager(cfg)
	bootstrap, err := manager.bootstrap(ctx)
	if err != nil {
		_ = manager.Close()
		return nil, err
	}
	client := newClient(nil, nil)
	client.manager = manager
	client.serverID = bootstrap.serverID
	client.address = bootstrap.address
	client.serverType = bootstrap.serverType
	client.observer = cfg.observer
	client.snapshotProvider = cfg.snapshotProvider
	client.remoteFiles = cfg.remoteFiles
	client.router = NewRouter(ServerNode{ID: client.serverID, Address: client.address, ServerType: Coordinator}, client.fetchTableMetadata).
		WithPhysicalMetadataFetcher(client.fetchPartitionMetadata)
	if cfg.dynamicPartitions != nil {
		client.partitionCreator = newDynamicPartitionCreator(client, *cfg.dynamicPartitions)
	}
	if cfg.tokens.enabled {
		provider := cfg.tokens.provider
		if provider == nil {
			provider = clientSecurityTokenProvider{client: client}
		}
		client.tokenManager = newSecurityTokenManager(provider, cfg.tokens.config, cfg.tokens.receivers)
		client.tokenManager.Start()
	}
	return client, nil
}

func newClient(requester fmsg.Requester, close func() error) *Client {
	client := newPhysicalClient(requester, close)
	client.schemas = newSchemaCache(defaultSchemaCacheEntries, client.fetchSchema)
	return client
}

func newPhysicalClient(requester fmsg.Requester, close func() error) *Client {
	return &Client{requester: requester, close: close, versions: make(map[fmsg.APIKey]int16)}
}

// Requester exposes the low-level protocol requester implemented by the client.
func (c *Client) Requester() fmsg.Requester { return c }

// Request sends a coordinator-scoped protocol request.
func (c *Client) Request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: nil request", fmsg.ErrInvalidArgument)
	}
	if c.manager != nil {
		if err := c.ensureOpen(); err != nil {
			return nil, err
		}
		return c.manager.request(ctx, ServerNode{ID: c.serverID, Address: c.address, ServerType: c.serverType}, request)
	}
	return c.request(ctx, request)
}

// RequestTo sends a raw request to the connection for node. It is intended for protocol helpers;
// higher-level clients select the appropriate coordinator or tablet server from metadata.
func (c *Client) RequestTo(ctx context.Context, node ServerNode, request fmsg.Request) (fmsg.Response, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: nil request", fmsg.ErrInvalidArgument)
	}
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
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
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if c.router == nil {
		return nil, fmt.Errorf("%w: client does not manage metadata", ErrClosed)
	}
	node, bucketCount, err := c.router.routePhysical(ctx, path, bucket)
	if err != nil {
		return nil, err
	}
	setRoutingBucketCount(request, bucketCount)
	response, err := c.RequestTo(ctx, node, request)
	if !errors.Is(err, ErrMetadata) {
		if shouldReplaceConnection(err) {
			// A failed tablet connection may be the old leader after failover.
			// Invalidate the route for the caller's next attempt, but do not replay
			// a potentially applied mutation here.
			c.router.InvalidatePhysical(path)
		}
		return response, err
	}
	c.router.InvalidatePhysical(path)
	node, bucketCount, refreshErr := c.router.routePhysical(ctx, path, bucket)
	if refreshErr != nil {
		return nil, refreshErr
	}
	setRoutingBucketCount(request, bucketCount)
	return c.RequestTo(ctx, node, request)
}

func (c *Client) request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: nil request", fmsg.ErrInvalidArgument)
	}
	var requestErr error
	if c.observer != nil {
		started := time.Now()
		defer func() {
			observeMetric(c.observer, MetricEvent{
				Kind: MetricRequest, Operation: MetricOperationRPC, APIKey: request.APIKey(),
				ServerType: c.serverType, Duration: time.Since(started),
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

func (c *Client) ensureOpen() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClosed
	}
	return nil
}

// Close stops token refresh and closes all managed connections.
// Close is idempotent.
func (c *Client) Close() error {
	if !c.markClosed() {
		return nil
	}
	if c.tokenManager != nil {
		c.tokenManager.Stop()
	}
	if c.schemas != nil {
		c.schemas.close()
	}
	if c.manager != nil {
		return c.manager.Close()
	}
	return c.closeTransport()
}

func (c *Client) shutdown() error {
	if !c.markClosed() {
		return nil
	}
	return c.closeTransport()
}

func (c *Client) markClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.closed = true
	return true
}

func (c *Client) closeTransport() error {
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
	c.serverTypeID = versions.GetServerType()
	c.mu.Unlock()
	return nil
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
	if err == nil {
		return false
	}
	if errors.Is(err, ErrClosed) || errors.Is(err, transport.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}
