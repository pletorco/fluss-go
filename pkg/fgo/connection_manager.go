package fgo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/internal/transport"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

// ServerType identifies a Fluss RPC server advertised by ApiVersions.
type ServerType int32

// Fluss server types reported during API negotiation.
const (
	// UnknownServerType represents an unrecognized server type.
	UnknownServerType ServerType = -1
	Coordinator       ServerType = 1
	TabletServer      ServerType = 2
)

// ErrServerType reports that a connection negotiated an unexpected server type.
var ErrServerType = errors.New("fgo: unexpected server type")

type connectionKey struct {
	id         int32
	address    string
	serverType ServerType
}

type connectionManager struct {
	cfg config

	mu      sync.Mutex
	closed  bool
	clients map[connectionKey]*Client
	flights *coalescer[connectionKey, *Client]
}

func newConnectionManager(cfg config) *connectionManager {
	if cfg.retry.MaxAttempts == 0 {
		cfg.retry.MaxAttempts = 1
	}
	if cfg.retry.Backoff == nil {
		cfg.retry.Backoff = func(int) time.Duration { return 0 }
	}
	return &connectionManager{
		cfg:     cfg,
		clients: make(map[connectionKey]*Client),
		flights: newCoalescer[connectionKey, *Client](),
	}
}

func (m *connectionManager) bootstrap(ctx context.Context) (*Client, error) {
	var failures []error
	for _, address := range m.cfg.bootstrapServers {
		client, err := m.get(ctx, address, Coordinator)
		if err == nil {
			return client, nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", address, err))
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("fgo: bootstrap coordinator: %w", errors.Join(failures...))
}

func (m *connectionManager) get(ctx context.Context, address string, serverType ServerType) (*Client, error) {
	return m.getNode(ctx, ServerNode{ID: -1, Address: address, ServerType: serverType})
}

func (m *connectionManager) getNode(ctx context.Context, node ServerNode) (*Client, error) {
	address, serverType := node.Address, node.ServerType
	if address == "" {
		return nil, fmt.Errorf("%w: empty server address", ErrInvalidConfig)
	}
	if _, err := parseServerType(int32(serverType)); err != nil {
		return nil, err
	}
	key := connectionKey{id: node.ID, address: address, serverType: serverType}
	if client, found, err := m.cachedClient(key); found || err != nil {
		return client, err
	}
	return m.flights.Do(ctx, key, func(workCtx context.Context) (*Client, error) {
		return m.openManagedConnection(workCtx, key, node.ID)
	}, nil)
}

func (m *connectionManager) cachedClient(key connectionKey) (*Client, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, false, ErrClosed
	}
	if client := m.clients[key]; client != nil {
		return client, true, nil
	}
	return nil, false, nil
}

func (m *connectionManager) openManagedConnection(
	ctx context.Context,
	key connectionKey,
	serverID int32,
) (*Client, error) {
	if client, found, err := m.cachedClient(key); found || err != nil {
		return client, err
	}
	client, err := m.open(ctx, key.address, key.serverType)
	m.mu.Lock()
	if m.closed && err == nil {
		err = ErrClosed
	}
	if err == nil {
		client.serverID = serverID
		m.clients[key] = client
	}
	m.mu.Unlock()
	if err != nil && client != nil {
		_ = client.shutdown()
	}
	return client, err
}

func (m *connectionManager) open(ctx context.Context, address string, expectedServerType ServerType) (*Client, error) {
	started := metricStart(m.cfg.observer)
	conn, err := m.cfg.dialContext(ctx, "tcp", address)
	observeMetric(m.cfg.observer, MetricEvent{
		Kind: MetricRemoteIO, Operation: MetricOperationDial, ServerType: expectedServerType,
		Duration: metricDuration(started), Failed: err != nil, ErrorClass: metricErrorClass(err),
	})
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	if m.cfg.tlsConfig != nil {
		tlsConn := tls.Client(conn, m.cfg.tlsConfig.Clone())
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("TLS handshake: %w", err)
		}
		conn = tlsConn
	}
	requester, err := transport.New(conn, m.cfg.limits)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	client := newPhysicalClient(requester, requester.Close)
	client.observer = m.cfg.observer
	if err := client.negotiate(ctx, m.cfg.name, m.cfg.version); err != nil {
		_ = client.shutdown()
		return nil, err
	}
	serverType, err := parseServerType(client.serverTypeID)
	if err != nil {
		_ = client.shutdown()
		return nil, err
	}
	if serverType != expectedServerType {
		_ = client.shutdown()
		return nil, fmt.Errorf("%w: %s advertised %s, need %s", ErrServerType, address, serverType, expectedServerType)
	}
	client.address, client.serverType = address, serverType
	if m.cfg.authFactory == nil {
		return client, nil
	}
	auth, err := m.cfg.authFactory()
	if err != nil {
		_ = client.shutdown()
		return nil, authenticationError(err, false)
	}
	client.close = closeAll(requester.Close, auth.Close)
	if err := client.authenticate(ctx, auth); err != nil {
		_ = client.shutdown()
		return nil, err
	}
	return client, nil
}

func (m *connectionManager) remove(address string, serverType ServerType, client *Client) {
	key := connectionKey{id: client.serverID, address: address, serverType: serverType}
	m.mu.Lock()
	if m.clients[key] == client {
		delete(m.clients, key)
	}
	m.mu.Unlock()
}

func (m *connectionManager) request(ctx context.Context, node ServerNode, request fmsg.Request) (fmsg.Response, error) {
	for attempt := 1; attempt <= m.cfg.retry.MaxAttempts; attempt++ {
		client, err := m.getNode(ctx, node)
		if err != nil {
			return nil, err
		}
		response, err := client.request(ctx, request)
		if err == nil || !m.shouldRetry(ctx, request.APIKey(), err, attempt) {
			if shouldReplaceConnection(err) {
				m.remove(node.Address, node.ServerType, client)
				_ = client.shutdown()
			}
			return response, err
		}
		m.remove(node.Address, node.ServerType, client)
		_ = client.shutdown()
		delay := m.cfg.retry.Backoff(attempt)
		observeMetric(m.cfg.observer, MetricEvent{
			Kind: MetricRetry, Operation: MetricOperationRPC, APIKey: request.APIKey(),
			ServerType: node.ServerType, Duration: delay, Attempt: attempt + 1,
			Failed: true, ErrorClass: metricErrorClass(err),
		})
		if err := waitRetry(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("fgo: retry attempts exhausted")
}

func (m *connectionManager) shouldRetry(ctx context.Context, key fmsg.APIKey, err error, attempt int) bool {
	if ctx.Err() != nil || attempt >= m.cfg.retry.MaxAttempts || !safeToRetry(key) {
		return false
	}
	var server *ServerError
	return errors.As(err, &server) && server.Retriable || shouldReplaceConnection(err)
}

func safeToRetry(key fmsg.APIKey) bool {
	switch key {
	case fmsg.APIKeyApiVersions, fmsg.APIKeyGetTableInfo, fmsg.APIKeyListTables,
		fmsg.APIKeyListDatabases, fmsg.APIKeyDatabaseExists, fmsg.APIKeyTableExists,
		fmsg.APIKeyGetMetadata, fmsg.APIKeyGetTableSchema, fmsg.APIKeyListPartitionInfos,
		fmsg.APIKeyGetTableStats, fmsg.APIKeyListOffsets:
		return true
	default:
		return false
	}
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *connectionManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	clients := make([]*Client, 0, len(m.clients))
	for _, client := range m.clients {
		clients = append(clients, client)
	}
	m.mu.Unlock()
	m.flights.Close(ErrClosed)
	var result error
	for _, client := range clients {
		if err := client.shutdown(); err != nil && result == nil {
			result = err
		}
	}
	return result
}

func parseServerType(value int32) (ServerType, error) {
	serverType := ServerType(value)
	if serverType != Coordinator && serverType != TabletServer {
		return UnknownServerType, fmt.Errorf("%w: %d", ErrServerType, value)
	}
	return serverType, nil
}

// String returns a stable human-readable server type.
func (r ServerType) String() string {
	switch r {
	case Coordinator:
		return "coordinator"
	case TabletServer:
		return "tablet server"
	default:
		return "unknown"
	}
}
