package fgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

// Filesystem security-token availability errors.
var (
	ErrFileSystemSecurityToken       = errors.New("fgo: filesystem security token unavailable")
	ErrSecurityTokenProviderDisabled = errors.New("fgo: filesystem security token provider disabled")
)

// FileSystemSecurityToken contains temporary filesystem credentials.
// String and GoString always redact Token.
type FileSystemSecurityToken struct {
	// Schema identifies the filesystem credential format.
	Schema string
	// Token contains opaque secret bytes and must not be logged.
	Token []byte
	// ExpiresAt is the server expiration time.
	ExpiresAt time.Time
	// AdditionalInfo contains non-secret provider metadata.
	AdditionalInfo map[string]string
	// Revoked reports that the server invalidated the token.
	Revoked bool
}

// Clone returns a deep copy suitable for handing to another component.
func (t FileSystemSecurityToken) Clone() FileSystemSecurityToken {
	cloned := t
	cloned.Token = append([]byte(nil), t.Token...)
	cloned.AdditionalInfo = make(map[string]string, len(t.AdditionalInfo))
	for key, value := range t.AdditionalInfo {
		cloned.AdditionalInfo[key] = value
	}
	return cloned
}

// String returns a representation with token bytes redacted.
func (t FileSystemSecurityToken) String() string {
	return fmt.Sprintf(
		"FileSystemSecurityToken{Schema:%q Token:[REDACTED] ExpiresAt:%s Revoked:%t}",
		t.Schema, t.ExpiresAt.UTC().Format(time.RFC3339), t.Revoked,
	)
}

// GoString returns a redacted Go-syntax representation.
func (t FileSystemSecurityToken) GoString() string { return t.String() }

// FileSystemSecurityTokenProvider acquires temporary filesystem credentials.
type FileSystemSecurityTokenProvider interface {
	// AcquireFileSystemSecurityToken returns a new caller-owned token.
	AcquireFileSystemSecurityToken(context.Context) (FileSystemSecurityToken, error)
}

// FileSystemSecurityTokenProviderFunc adapts a function to
// [FileSystemSecurityTokenProvider].
type FileSystemSecurityTokenProviderFunc func(context.Context) (FileSystemSecurityToken, error)

// AcquireFileSystemSecurityToken calls f.
func (f FileSystemSecurityTokenProviderFunc) AcquireFileSystemSecurityToken(
	ctx context.Context,
) (FileSystemSecurityToken, error) {
	return f(ctx)
}

// FileSystemSecurityTokenReceiver receives a clone after each successful
// acquisition.
type FileSystemSecurityTokenReceiver interface {
	// ReceiveFileSystemSecurityToken receives a deep clone after acquisition.
	// Returning an error rejects publication and schedules a bounded retry.
	// Receivers run sequentially and should return promptly. A receiver may
	// close the client; shutdown stops waiting for the callback but cannot
	// forcibly stop callback code that remains blocked.
	ReceiveFileSystemSecurityToken(FileSystemSecurityToken) error
}

// FileSystemSecurityTokenReceiverFunc adapts a function to
// [FileSystemSecurityTokenReceiver].
type FileSystemSecurityTokenReceiverFunc func(FileSystemSecurityToken) error

// ReceiveFileSystemSecurityToken calls f with token.
func (f FileSystemSecurityTokenReceiverFunc) ReceiveFileSystemSecurityToken(
	token FileSystemSecurityToken,
) error {
	return f(token)
}

// FileSystemSecurityTokenRefreshConfig controls renewal timing and bounded
// retry backoff. Zero fields use documented defaults.
type FileSystemSecurityTokenRefreshConfig struct {
	// RenewalTimeRatio selects the fraction of token lifetime before renewal;
	// zero defaults to 0.75.
	RenewalTimeRatio float64
	// RenewalRetryBackoff is the initial acquisition retry delay; zero defaults to 1m.
	RenewalRetryBackoff time.Duration
	// MaxRenewalRetryBackoff bounds exponential retry delay; zero defaults to 1h.
	MaxRenewalRetryBackoff time.Duration
	// ClockSkew renews this much earlier than calculated; zero defaults to 30s.
	ClockSkew time.Duration
	// Jitter randomizes renewal delay as a fraction in [0, 1].
	Jitter float64

	clock  securityTokenClock
	random func() float64
}

func (c FileSystemSecurityTokenRefreshConfig) normalized() (FileSystemSecurityTokenRefreshConfig, error) {
	if c.RenewalTimeRatio == 0 {
		c.RenewalTimeRatio = 0.75
	}
	if c.RenewalRetryBackoff == 0 {
		c.RenewalRetryBackoff = time.Minute
	}
	if c.MaxRenewalRetryBackoff == 0 {
		c.MaxRenewalRetryBackoff = time.Hour
	}
	if c.ClockSkew == 0 {
		c.ClockSkew = 30 * time.Second
	}
	if c.RenewalTimeRatio <= 0 || c.RenewalTimeRatio >= 1 || c.RenewalRetryBackoff <= 0 ||
		c.MaxRenewalRetryBackoff < c.RenewalRetryBackoff || c.ClockSkew < 0 ||
		c.Jitter < 0 || c.Jitter > 1 {
		return FileSystemSecurityTokenRefreshConfig{}, fmt.Errorf(
			"%w: invalid filesystem security token refresh settings", ErrInvalidConfig,
		)
	}
	if c.clock == nil {
		c.clock = systemSecurityTokenClock{}
	}
	if c.random == nil {
		c.random = rand.Float64
	}
	return c, nil
}

type securityTokenSettings struct {
	enabled   bool
	config    FileSystemSecurityTokenRefreshConfig
	provider  FileSystemSecurityTokenProvider
	receivers []FileSystemSecurityTokenReceiver
}

// WithFileSystemSecurityTokenRefresh enables coordinator-backed token
// acquisition and optional receivers.
func WithFileSystemSecurityTokenRefresh(
	config FileSystemSecurityTokenRefreshConfig,
	receivers ...FileSystemSecurityTokenReceiver,
) Option {
	return withFileSystemSecurityTokenProvider(nil, config, receivers)
}

// WithFileSystemSecurityTokenProvider enables refresh through a custom provider.
func WithFileSystemSecurityTokenProvider(
	provider FileSystemSecurityTokenProvider,
	refresh FileSystemSecurityTokenRefreshConfig,
	receivers ...FileSystemSecurityTokenReceiver,
) Option {
	if provider == nil {
		return func(*config) error {
			return fmt.Errorf("%w: nil filesystem security token provider", ErrInvalidConfig)
		}
	}
	return withFileSystemSecurityTokenProvider(provider, refresh, receivers)
}

func withFileSystemSecurityTokenProvider(
	provider FileSystemSecurityTokenProvider,
	refresh FileSystemSecurityTokenRefreshConfig,
	receivers []FileSystemSecurityTokenReceiver,
) Option {
	return func(config *config) error {
		normalized, err := refresh.normalized()
		if err != nil {
			return err
		}
		for _, receiver := range receivers {
			if receiver == nil {
				return fmt.Errorf("%w: nil filesystem security token receiver", ErrInvalidConfig)
			}
		}
		config.tokens = securityTokenSettings{
			enabled: true, config: normalized, provider: provider,
			receivers: append([]FileSystemSecurityTokenReceiver(nil), receivers...),
		}
		return nil
	}
}

type securityTokenManager struct {
	provider  FileSystemSecurityTokenProvider
	receivers []FileSystemSecurityTokenReceiver
	config    FileSystemSecurityTokenRefreshConfig

	mu        sync.RWMutex
	current   FileSystemSecurityToken
	available bool
	disabled  bool
	failures  int

	lifecycleMu sync.Mutex
	started     bool
	stopped     bool
	cancel      context.CancelFunc
	done        chan struct{}
}

func newSecurityTokenManager(
	provider FileSystemSecurityTokenProvider,
	config FileSystemSecurityTokenRefreshConfig,
	receivers []FileSystemSecurityTokenReceiver,
) *securityTokenManager {
	return &securityTokenManager{
		provider: provider, config: config,
		receivers: append([]FileSystemSecurityTokenReceiver(nil), receivers...),
		done:      make(chan struct{}),
	}
}

func (m *securityTokenManager) Start() {
	m.lifecycleMu.Lock()
	if m.started || m.stopped {
		m.lifecycleMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.started = true
	m.cancel = cancel
	m.lifecycleMu.Unlock()
	go m.run(ctx)
}

func (m *securityTokenManager) Stop() {
	m.lifecycleMu.Lock()
	if m.stopped {
		done := m.done
		m.lifecycleMu.Unlock()
		<-done
		return
	}
	m.stopped = true
	if !m.started {
		close(m.done)
		m.lifecycleMu.Unlock()
		return
	}
	cancel := m.cancel
	done := m.done
	m.lifecycleMu.Unlock()
	cancel()
	<-done
}

func (m *securityTokenManager) Current() (FileSystemSecurityToken, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.available || tokenExpired(m.current, m.config.clock.Now(), m.config.ClockSkew) {
		return FileSystemSecurityToken{}, false
	}
	return m.current.Clone(), true
}

func (m *securityTokenManager) run(ctx context.Context) {
	defer close(m.done)
	delay := time.Duration(0)
	for {
		if !waitSecurityToken(ctx, m.config.clock, delay) {
			return
		}
		next, stop := m.refreshOnce(ctx)
		if stop {
			return
		}
		delay = next
	}
}

func (m *securityTokenManager) refreshOnce(ctx context.Context) (time.Duration, bool) {
	token, err := m.provider.AcquireFileSystemSecurityToken(ctx)
	if errors.Is(err, ErrSecurityTokenProviderDisabled) {
		m.disable()
		return 0, true
	}
	if err != nil || validateSecurityToken(token, m.config.clock.Now(), m.config.ClockSkew) != nil {
		return m.failedDelay(), false
	}
	if err := m.publish(ctx, token); err != nil {
		if ctx.Err() != nil {
			return 0, true
		}
		return m.failedDelay(), false
	}
	m.mu.Lock()
	m.failures = 0
	m.mu.Unlock()
	if token.Revoked {
		return m.jitter(m.config.RenewalRetryBackoff), false
	}
	if token.ExpiresAt.IsZero() {
		return 0, true
	}
	return m.renewalDelay(token.ExpiresAt), false
}

func (m *securityTokenManager) publish(
	ctx context.Context,
	token FileSystemSecurityToken,
) error {
	published := token.Clone()
	for _, receiver := range m.receivers {
		if err := callSecurityTokenReceiver(ctx, receiver, published.Clone()); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrFileSystemSecurityToken
		}
	}
	m.mu.Lock()
	if token.Revoked {
		m.current = FileSystemSecurityToken{}
		m.available = false
	} else {
		m.current = published
		m.available = true
	}
	m.mu.Unlock()
	return nil
}

func (m *securityTokenManager) disable() {
	m.mu.Lock()
	m.current = FileSystemSecurityToken{}
	m.available = false
	m.disabled = true
	m.mu.Unlock()
}

func (m *securityTokenManager) failedDelay() time.Duration {
	m.mu.Lock()
	m.failures++
	failures := m.failures
	m.mu.Unlock()
	exponent := failures - 1
	if exponent > 30 {
		exponent = 30
	}
	delay := time.Duration(float64(m.config.RenewalRetryBackoff) * math.Pow(2, float64(exponent)))
	if delay > m.config.MaxRenewalRetryBackoff || delay < 0 {
		delay = m.config.MaxRenewalRetryBackoff
	}
	return m.jitter(delay)
}

func (m *securityTokenManager) renewalDelay(expiresAt time.Time) time.Duration {
	now := m.config.clock.Now()
	lifetime := expiresAt.Add(-m.config.ClockSkew).Sub(now)
	if lifetime <= 0 {
		return m.jitter(m.config.RenewalRetryBackoff)
	}
	return m.jitter(time.Duration(float64(lifetime) * m.config.RenewalTimeRatio))
}

func (m *securityTokenManager) jitter(delay time.Duration) time.Duration {
	if m.config.Jitter == 0 {
		return delay
	}
	factor := 1 + (m.config.random()*2-1)*m.config.Jitter
	jittered := time.Duration(float64(delay) * factor)
	if jittered < 0 {
		return 0
	}
	return jittered
}

func validateSecurityToken(token FileSystemSecurityToken, now time.Time, skew time.Duration) error {
	if strings.TrimSpace(token.Schema) == "" {
		return ErrFileSystemSecurityToken
	}
	if !token.Revoked && tokenExpired(token, now, skew) {
		return ErrFileSystemSecurityToken
	}
	return nil
}

func tokenExpired(token FileSystemSecurityToken, now time.Time, skew time.Duration) bool {
	return !token.ExpiresAt.IsZero() && !now.Add(skew).Before(token.ExpiresAt)
}

func callSecurityTokenReceiver(
	ctx context.Context,
	receiver FileSystemSecurityTokenReceiver,
	token FileSystemSecurityToken,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if recover() != nil {
				err = ErrFileSystemSecurityToken
			}
			result <- err
		}()
		err = receiver.ReceiveFileSystemSecurityToken(token)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type securityTokenClock interface {
	Now() time.Time
	NewTimer(time.Duration) securityTokenTimer
}

type securityTokenTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type systemSecurityTokenClock struct{}

func (systemSecurityTokenClock) Now() time.Time { return time.Now() }

func (systemSecurityTokenClock) NewTimer(delay time.Duration) securityTokenTimer {
	return systemSecurityTokenTimer{Timer: time.NewTimer(delay)}
}

type systemSecurityTokenTimer struct{ *time.Timer }

func (t systemSecurityTokenTimer) C() <-chan time.Time { return t.Timer.C }

func waitSecurityToken(ctx context.Context, clock securityTokenClock, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-ctx.Done():
		return false
	}
}

type clientSecurityTokenProvider struct{ client *Client }

func (p clientSecurityTokenProvider) AcquireFileSystemSecurityToken(
	ctx context.Context,
) (FileSystemSecurityToken, error) {
	return p.client.fetchFileSystemSecurityToken(ctx)
}

func (c *Client) fetchFileSystemSecurityToken(ctx context.Context) (FileSystemSecurityToken, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetFilesystemSecurityToken, 0)
	if err != nil {
		return FileSystemSecurityToken{}, err
	}
	response, err := c.RequestCoordinator(ctx, request)
	if err != nil {
		return FileSystemSecurityToken{}, err
	}
	message, ok := response.Message().(*fmsg.GetFileSystemSecurityTokenResponse)
	if !ok {
		return FileSystemSecurityToken{}, ErrFileSystemSecurityToken
	}
	token := FileSystemSecurityToken{
		Schema: message.GetSchema(), Token: append([]byte(nil), message.GetToken()...),
		AdditionalInfo: make(map[string]string, len(message.GetAdditionInfo())),
	}
	if message.ExpirationTime != nil {
		token.ExpiresAt = time.UnixMilli(message.GetExpirationTime())
	}
	for _, item := range message.GetAdditionInfo() {
		token.AdditionalInfo[item.GetKey()] = item.GetValue()
	}
	return token, nil
}

// CurrentFileSystemSecurityToken returns a clone of the current valid,
// non-revoked token.
func (c *Client) CurrentFileSystemSecurityToken() (FileSystemSecurityToken, bool) {
	if c.tokenManager == nil {
		return FileSystemSecurityToken{}, false
	}
	return c.tokenManager.Current()
}
