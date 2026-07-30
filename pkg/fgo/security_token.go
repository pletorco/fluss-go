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

var (
	ErrFileSystemSecurityToken       = errors.New("fgo: filesystem security token unavailable")
	ErrSecurityTokenProviderDisabled = errors.New("fgo: filesystem security token provider disabled")
)

type FileSystemSecurityToken struct {
	Schema         string
	Token          []byte
	ExpiresAt      time.Time
	AdditionalInfo map[string]string
	Revoked        bool
}

func (t FileSystemSecurityToken) Clone() FileSystemSecurityToken {
	cloned := t
	cloned.Token = append([]byte(nil), t.Token...)
	cloned.AdditionalInfo = make(map[string]string, len(t.AdditionalInfo))
	for key, value := range t.AdditionalInfo {
		cloned.AdditionalInfo[key] = value
	}
	return cloned
}

func (t FileSystemSecurityToken) String() string {
	return fmt.Sprintf(
		"FileSystemSecurityToken{Schema:%q Token:[REDACTED] ExpiresAt:%s Revoked:%t}",
		t.Schema, t.ExpiresAt.UTC().Format(time.RFC3339), t.Revoked,
	)
}

func (t FileSystemSecurityToken) GoString() string { return t.String() }

type FileSystemSecurityTokenProvider interface {
	AcquireFileSystemSecurityToken(context.Context) (FileSystemSecurityToken, error)
}

type FileSystemSecurityTokenProviderFunc func(context.Context) (FileSystemSecurityToken, error)

func (f FileSystemSecurityTokenProviderFunc) AcquireFileSystemSecurityToken(
	ctx context.Context,
) (FileSystemSecurityToken, error) {
	return f(ctx)
}

type FileSystemSecurityTokenReceiver interface {
	ReceiveFileSystemSecurityToken(FileSystemSecurityToken) error
}

type FileSystemSecurityTokenReceiverFunc func(FileSystemSecurityToken) error

func (f FileSystemSecurityTokenReceiverFunc) ReceiveFileSystemSecurityToken(
	token FileSystemSecurityToken,
) error {
	return f(token)
}

type FileSystemSecurityTokenRefreshConfig struct {
	RenewalRatio    float64
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	ClockSkew       time.Duration
	Jitter          float64

	clock  securityTokenClock
	random func() float64
}

func (c FileSystemSecurityTokenRefreshConfig) normalized() (FileSystemSecurityTokenRefreshConfig, error) {
	if c.RenewalRatio == 0 {
		c.RenewalRatio = 0.75
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = time.Minute
	}
	if c.MaxRetryBackoff == 0 {
		c.MaxRetryBackoff = time.Hour
	}
	if c.ClockSkew == 0 {
		c.ClockSkew = 30 * time.Second
	}
	if c.RenewalRatio <= 0 || c.RenewalRatio >= 1 || c.RetryBackoff <= 0 ||
		c.MaxRetryBackoff < c.RetryBackoff || c.ClockSkew < 0 ||
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

func WithFileSystemSecurityTokenRefresh(
	config FileSystemSecurityTokenRefreshConfig,
	receivers ...FileSystemSecurityTokenReceiver,
) Option {
	return withFileSystemSecurityTokenProvider(nil, config, receivers)
}

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
		token, err := m.provider.AcquireFileSystemSecurityToken(ctx)
		if errors.Is(err, ErrSecurityTokenProviderDisabled) {
			m.disable()
			return
		}
		if err != nil || validateSecurityToken(token, m.config.clock.Now(), m.config.ClockSkew) != nil {
			delay = m.failedDelay()
			continue
		}
		if err := m.publish(token); err != nil {
			delay = m.failedDelay()
			continue
		}
		m.mu.Lock()
		m.failures = 0
		m.mu.Unlock()
		if token.Revoked {
			delay = m.jitter(m.config.RetryBackoff)
			continue
		}
		if token.ExpiresAt.IsZero() {
			return
		}
		delay = m.renewalDelay(token.ExpiresAt)
	}
}

func (m *securityTokenManager) publish(token FileSystemSecurityToken) error {
	published := token.Clone()
	for _, receiver := range m.receivers {
		if err := callSecurityTokenReceiver(receiver, published.Clone()); err != nil {
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
	delay := time.Duration(float64(m.config.RetryBackoff) * math.Pow(2, float64(exponent)))
	if delay > m.config.MaxRetryBackoff || delay < 0 {
		delay = m.config.MaxRetryBackoff
	}
	return m.jitter(delay)
}

func (m *securityTokenManager) renewalDelay(expiresAt time.Time) time.Duration {
	now := m.config.clock.Now()
	lifetime := expiresAt.Add(-m.config.ClockSkew).Sub(now)
	if lifetime <= 0 {
		return m.jitter(m.config.RetryBackoff)
	}
	return m.jitter(time.Duration(float64(lifetime) * m.config.RenewalRatio))
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
	receiver FileSystemSecurityTokenReceiver,
	token FileSystemSecurityToken,
) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrFileSystemSecurityToken
		}
	}()
	return receiver.ReceiveFileSystemSecurityToken(token)
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

func (c *Client) CurrentFileSystemSecurityToken() (FileSystemSecurityToken, bool) {
	if c.tokenManager == nil {
		return FileSystemSecurityToken{}, false
	}
	return c.tokenManager.Current()
}
