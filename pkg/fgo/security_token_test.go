package fgo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type fakeSecurityTokenClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeSecurityTokenTimer
}

func (c *fakeSecurityTokenClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeSecurityTokenClock) NewTimer(delay time.Duration) securityTokenTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeSecurityTokenTimer{
		deadline: c.now.Add(delay), ch: make(chan time.Time, 1), active: true,
	}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *fakeSecurityTokenClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	now := c.now
	timers := append([]*fakeSecurityTokenTimer(nil), c.timers...)
	c.mu.Unlock()
	for _, timer := range timers {
		timer.fire(now)
	}
}

func (c *fakeSecurityTokenClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

type fakeSecurityTokenTimer struct {
	mu       sync.Mutex
	deadline time.Time
	ch       chan time.Time
	active   bool
}

func (t *fakeSecurityTokenTimer) C() <-chan time.Time { return t.ch }

func (t *fakeSecurityTokenTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := t.active
	t.active = false
	return wasActive
}

func (t *fakeSecurityTokenTimer) fire(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active && !now.Before(t.deadline) {
		t.active = false
		t.ch <- now
	}
}

func tokenTestConfig(clock *fakeSecurityTokenClock) FileSystemSecurityTokenRefreshConfig {
	config, err := (FileSystemSecurityTokenRefreshConfig{
		RenewalRatio: 0.5, RetryBackoff: 10 * time.Second,
		MaxRetryBackoff: 40 * time.Second, ClockSkew: time.Second,
		clock: clock, random: func() float64 { return 0.5 },
	}).normalized()
	if err != nil {
		panic(err)
	}
	return config
}

func waitTokenTest(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for security token state")
}

func refreshingTokenProvider(
	clock *fakeSecurityTokenClock,
	calls *atomic.Int32,
) FileSystemSecurityTokenProvider {
	return FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
		call := calls.Add(1)
		token := FileSystemSecurityToken{
			Schema: "hadoop", Token: []byte(fmt.Sprintf("secret-%d", call)),
			AdditionalInfo: map[string]string{"service": "filesystem"},
		}
		if call == 1 {
			token.ExpiresAt = clock.Now().Add(101 * time.Second)
		}
		return token, nil
	})
}

func assertTokenRedacted(t *testing.T, token FileSystemSecurityToken) {
	t.Helper()
	for _, formatted := range []string{fmt.Sprintf("%v", token), fmt.Sprintf("%#v", token)} {
		if strings.Contains(formatted, "secret-1") || !strings.Contains(formatted, "[REDACTED]") {
			t.Fatalf("token formatting leaked material: %s", formatted)
		}
	}
}

func TestSecurityTokenManagerRefreshAndCopies(t *testing.T) {
	clock := &fakeSecurityTokenClock{now: time.Unix(1_000, 0)}
	var calls atomic.Int32
	provider := refreshingTokenProvider(clock, &calls)
	received := make(chan FileSystemSecurityToken, 2)
	receiver := FileSystemSecurityTokenReceiverFunc(func(token FileSystemSecurityToken) error {
		received <- token.Clone()
		token.Token[0] = 'X'
		token.AdditionalInfo["service"] = "mutated"
		return nil
	})
	manager := newSecurityTokenManager(provider, tokenTestConfig(clock), []FileSystemSecurityTokenReceiver{receiver})
	manager.Start()
	waitTokenTest(t, func() bool {
		_, ok := manager.Current()
		return ok && clock.timerCount() == 1
	})
	first, ok := manager.Current()
	if !ok || string(first.Token) != "secret-1" || first.AdditionalInfo["service"] != "filesystem" {
		t.Fatalf("first token = %#v, available=%v", first, ok)
	}
	first.Token[0] = 'Y'
	first.AdditionalInfo["service"] = "caller"
	again, _ := manager.Current()
	if string(again.Token) != "secret-1" || again.AdditionalInfo["service"] != "filesystem" {
		t.Fatalf("cached token was aliased: %#v", again)
	}
	assertTokenRedacted(t, again)
	clock.Advance(49 * time.Second)
	if calls.Load() != 1 {
		t.Fatalf("early refresh calls = %d", calls.Load())
	}
	clock.Advance(time.Second)
	waitTokenTest(t, func() bool {
		token, ok := manager.Current()
		return calls.Load() == 2 && ok && string(token.Token) == "secret-2"
	})
	second, ok := manager.Current()
	if !ok || string(second.Token) != "secret-2" {
		t.Fatalf("second token = %#v, available=%v", second, ok)
	}
	manager.Stop()
	clock.Advance(time.Hour)
	if calls.Load() != 2 {
		t.Fatalf("calls after stop = %d", calls.Load())
	}
	if len(received) != 2 {
		t.Fatalf("receiver calls = %d", len(received))
	}
}

func TestSecurityTokenManagerBackoffRevocationAndDisable(t *testing.T) {
	clock := &fakeSecurityTokenClock{now: time.Unix(2_000, 0)}
	var calls atomic.Int32
	provider := FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
		switch calls.Add(1) {
		case 1:
			return FileSystemSecurityToken{}, errors.New("provider failed with secret material")
		case 2:
			return FileSystemSecurityToken{
				Schema: "hadoop", Token: []byte("expired"),
				ExpiresAt: clock.Now().Add(time.Second),
			}, nil
		default:
			return FileSystemSecurityToken{Schema: "hadoop", Token: []byte("valid")}, nil
		}
	})
	manager := newSecurityTokenManager(provider, tokenTestConfig(clock), nil)
	manager.Start()
	waitTokenTest(t, func() bool { return calls.Load() == 1 && clock.timerCount() == 1 })
	clock.Advance(10 * time.Second)
	waitTokenTest(t, func() bool { return calls.Load() == 2 && clock.timerCount() == 2 })
	clock.Advance(20 * time.Second)
	waitTokenTest(t, func() bool {
		token, ok := manager.Current()
		return calls.Load() == 3 && ok && string(token.Token) == "valid"
	})
	manager.Stop()

	if got := manager.failedDelay(); got != 10*time.Second {
		t.Fatalf("reset backoff = %v", got)
	}
	if got := manager.failedDelay(); got != 20*time.Second {
		t.Fatalf("second backoff = %v", got)
	}
	if got := manager.failedDelay(); got != 40*time.Second {
		t.Fatalf("third backoff = %v", got)
	}
	if got := manager.failedDelay(); got != 40*time.Second {
		t.Fatalf("capped backoff = %v", got)
	}
	manager.config.Jitter = 0.2
	manager.config.random = func() float64 { return 0 }
	if got := manager.jitter(10 * time.Second); got != 8*time.Second {
		t.Fatalf("lower jitter = %v", got)
	}
	manager.config.random = func() float64 { return 1 }
	if got := manager.jitter(10 * time.Second); got != 12*time.Second {
		t.Fatalf("upper jitter = %v", got)
	}

	disabled := newSecurityTokenManager(
		FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
			return FileSystemSecurityToken{}, ErrSecurityTokenProviderDisabled
		}),
		tokenTestConfig(clock), nil,
	)
	disabled.Start()
	waitTokenTest(t, func() bool {
		select {
		case <-disabled.done:
			return true
		default:
			return false
		}
	})
	if _, ok := disabled.Current(); ok || !disabled.disabled {
		t.Fatalf("disabled manager retained a token")
	}
	disabled.Stop()
}

func TestSecurityTokenManagerRevocationAndReceiverFailure(t *testing.T) {
	clock := &fakeSecurityTokenClock{now: time.Unix(3_000, 0)}
	var calls atomic.Int32
	provider := FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
		switch calls.Add(1) {
		case 1:
			return FileSystemSecurityToken{
				Schema: "hadoop", Token: []byte("first"),
				ExpiresAt: clock.Now().Add(101 * time.Second),
			}, nil
		case 2:
			return FileSystemSecurityToken{Schema: "hadoop", Revoked: true}, nil
		default:
			return FileSystemSecurityToken{Schema: "hadoop", Token: []byte("restored")}, nil
		}
	})
	var receiverCalls atomic.Int32
	receiver := FileSystemSecurityTokenReceiverFunc(func(FileSystemSecurityToken) error {
		if receiverCalls.Add(1) == 1 {
			panic("receiver failure")
		}
		return nil
	})
	manager := newSecurityTokenManager(
		provider, tokenTestConfig(clock), []FileSystemSecurityTokenReceiver{receiver},
	)
	manager.Start()
	waitTokenTest(t, func() bool { return calls.Load() == 1 && clock.timerCount() == 1 })
	if _, ok := manager.Current(); ok {
		t.Fatal("token was published after receiver failure")
	}
	clock.Advance(10 * time.Second)
	waitTokenTest(t, func() bool { return calls.Load() == 2 && clock.timerCount() == 2 })
	if _, ok := manager.Current(); ok {
		t.Fatal("revoked token remained available")
	}
	clock.Advance(10 * time.Second)
	waitTokenTest(t, func() bool {
		token, ok := manager.Current()
		return calls.Load() == 3 && ok && string(token.Token) == "restored"
	})
	manager.Stop()
}

func TestSecurityTokenOptionsAndProtocol(t *testing.T) {
	for _, option := range []Option{
		WithFileSystemSecurityTokenProvider(nil, FileSystemSecurityTokenRefreshConfig{}),
		WithFileSystemSecurityTokenRefresh(
			FileSystemSecurityTokenRefreshConfig{RenewalRatio: 1},
		),
		WithFileSystemSecurityTokenRefresh(
			FileSystemSecurityTokenRefreshConfig{},
			FileSystemSecurityTokenReceiver(nil),
		),
	} {
		if err := option(&config{}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid token option error = %v", err)
		}
	}
	var cfg config
	if err := WithFileSystemSecurityTokenProvider(
		FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
			return FileSystemSecurityToken{}, nil
		}),
		FileSystemSecurityTokenRefreshConfig{Jitter: 0.2},
	)(&cfg); err != nil || !cfg.tokens.enabled {
		t.Fatalf("token option = %#v, %v", cfg.tokens, err)
	}

	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		message := response.Message().(*fmsg.GetFileSystemSecurityTokenResponse)
		message.Schema = proto.String("hadoop")
		message.Token = []byte("wire-secret")
		message.ExpirationTime = proto.Int64(4_000_000)
		message.AdditionInfo = []*fmsg.PbKeyValue{{
			Key: proto.String("service"), Value: proto.String("filesystem"),
		}}
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyGetFilesystemSecurityToken] = 0
	token, err := client.fetchFileSystemSecurityToken(context.Background())
	if err != nil || token.Schema != "hadoop" || string(token.Token) != "wire-secret" ||
		token.ExpiresAt.UnixMilli() != 4_000_000 ||
		token.AdditionalInfo["service"] != "filesystem" {
		t.Fatalf("wire token = %#v, %v", token, err)
	}
	provided, err := (clientSecurityTokenProvider{client: client}).
		AcquireFileSystemSecurityToken(context.Background())
	if err != nil || string(provided.Token) != "wire-secret" {
		t.Fatalf("client token provider = %#v, %v", provided, err)
	}
}

func TestSystemSecurityTokenClock(t *testing.T) {
	clock := systemSecurityTokenClock{}
	before := time.Now()
	if now := clock.Now(); now.Before(before) || now.After(time.Now()) {
		t.Fatalf("clock.Now() = %v", now)
	}
	timer := clock.NewTimer(time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("system token timer did not fire")
	}
	_ = timer.Stop()
}

func TestClientCloseStopsSecurityTokenManager(t *testing.T) {
	clock := &fakeSecurityTokenClock{now: time.Unix(5_000, 0)}
	manager := newSecurityTokenManager(
		FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
			return FileSystemSecurityToken{
				Schema: "hadoop", Token: []byte("secret"),
				ExpiresAt: clock.Now().Add(time.Hour),
			}, nil
		}),
		tokenTestConfig(clock), nil,
	)
	client := newClient(nil, nil)
	client.tokenManager = manager
	manager.Start()
	waitTokenTest(t, func() bool {
		_, ok := manager.Current()
		return ok
	})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.done:
	default:
		t.Fatal("client close did not stop token manager")
	}
	if _, ok := client.CurrentFileSystemSecurityToken(); !ok {
		t.Fatal("valid cached token should remain readable after manager shutdown")
	}

	unstarted := newSecurityTokenManager(
		FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
			t.Fatal("stopped manager called provider")
			return FileSystemSecurityToken{}, nil
		}),
		tokenTestConfig(clock), nil,
	)
	unstarted.Stop()
	unstarted.Start()
	unstarted.Stop()
}
