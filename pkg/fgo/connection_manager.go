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

// ServerRole identifies a Fluss RPC server advertised by ApiVersions.
type ServerRole int32

// Fluss server roles reported during API negotiation.
const (
	// UnknownServerRole represents an unrecognized server role.
	UnknownServerRole ServerRole = -1
	Coordinator       ServerRole = 1
	TabletServer      ServerRole = 2
)

// ErrServerRole reports that a connection negotiated an unexpected server role.
var ErrServerRole = errors.New("fgo: unexpected server role")

type connectionKey struct {
	id      int32
	address string
	role    ServerRole
}

type connectionFlight struct {
	done   chan struct{}
	client *Client
	err    error
}

type connectionManager struct {
	cfg config

	mu      sync.Mutex
	closed  bool
	clients map[connectionKey]*Client
	flights map[connectionKey]*connectionFlight
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
		flights: make(map[connectionKey]*connectionFlight),
	}
}

func (m *connectionManager) bootstrap(ctx context.Context) (*Client, error) {
	var failures []error
	for _, address := range m.cfg.seeds {
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

func (m *connectionManager) get(ctx context.Context, address string, role ServerRole) (*Client, error) {
	return m.getNode(ctx, Node{ID: -1, Address: address, Role: role})
}

func (m *connectionManager) getNode(ctx context.Context, node Node) (*Client, error) {
	address, role := node.Address, node.Role
	if address == "" {
		return nil, fmt.Errorf("%w: empty server address", ErrInvalidConfig)
	}
	if _, err := serverRole(int32(role)); err != nil {
		return nil, err
	}
	key := connectionKey{id: node.ID, address: address, role: role}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if client := m.clients[key]; client != nil {
		m.mu.Unlock()
		return client, nil
	}
	if flight := m.flights[key]; flight != nil {
		m.mu.Unlock()
		select {
		case <-flight.done:
			return flight.client, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &connectionFlight{done: make(chan struct{})}
	m.flights[key] = flight
	m.mu.Unlock()

	client, err := m.open(ctx, address, role)

	m.mu.Lock()
	delete(m.flights, key)
	if m.closed && err == nil {
		err = ErrClosed
	}
	if err == nil {
		client.serverID = node.ID
		m.clients[key] = client
	}
	flight.client, flight.err = client, err
	close(flight.done)
	m.mu.Unlock()
	if err != nil && client != nil {
		_ = client.shutdown()
	}
	return client, err
}

func (m *connectionManager) open(ctx context.Context, address string, expectedRole ServerRole) (*Client, error) {
	started := metricStart(m.cfg.observer)
	conn, err := m.cfg.dialContext(ctx, "tcp", address)
	observeMetric(m.cfg.observer, MetricEvent{
		Kind: MetricRemoteIO, Operation: MetricOperationDial, ServerRole: expectedRole,
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
	client := newClient(requester, requester.Close)
	client.observer = m.cfg.observer
	if err := client.negotiate(ctx, m.cfg.name, m.cfg.version); err != nil {
		_ = client.shutdown()
		return nil, err
	}
	role, err := serverRole(client.serverType)
	if err != nil {
		_ = client.shutdown()
		return nil, err
	}
	if role != expectedRole {
		_ = client.shutdown()
		return nil, fmt.Errorf("%w: %s advertised %s, need %s", ErrServerRole, address, role, expectedRole)
	}
	client.address, client.role = address, role
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

func (m *connectionManager) remove(address string, role ServerRole, client *Client) {
	key := connectionKey{id: client.serverID, address: address, role: role}
	m.mu.Lock()
	if m.clients[key] == client {
		delete(m.clients, key)
	}
	m.mu.Unlock()
}

func (m *connectionManager) request(ctx context.Context, node Node, request fmsg.Request) (fmsg.Response, error) {
	for attempt := 1; attempt <= m.cfg.retry.MaxAttempts; attempt++ {
		client, err := m.getNode(ctx, node)
		if err != nil {
			return nil, err
		}
		response, err := client.request(ctx, request)
		if err == nil || !m.shouldRetry(ctx, request.APIKey(), err, attempt) {
			if shouldReplaceConnection(err) {
				m.remove(node.Address, node.Role, client)
				_ = client.shutdown()
			}
			return response, err
		}
		m.remove(node.Address, node.Role, client)
		_ = client.shutdown()
		delay := m.cfg.retry.Backoff(attempt)
		observeMetric(m.cfg.observer, MetricEvent{
			Kind: MetricRetry, Operation: MetricOperationRPC, APIKey: request.APIKey(),
			ServerRole: node.Role, Duration: delay, Attempt: attempt + 1,
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
	var result error
	for _, client := range clients {
		if err := client.shutdown(); err != nil && result == nil {
			result = err
		}
	}
	return result
}

func serverRole(value int32) (ServerRole, error) {
	role := ServerRole(value)
	if role != Coordinator && role != TabletServer {
		return UnknownServerRole, fmt.Errorf("%w: %d", ErrServerRole, value)
	}
	return role, nil
}

// String returns a stable human-readable server role.
func (r ServerRole) String() string {
	switch r {
	case Coordinator:
		return "coordinator"
	case TabletServer:
		return "tablet server"
	default:
		return "unknown"
	}
}
