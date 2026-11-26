package fgo

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestRevokedSecurityTokenClearsCacheBeforeReceivers(t *testing.T) {
	clock := &fakeSecurityTokenClock{now: time.Unix(1, 0)}
	var manager *securityTokenManager
	var notifications []int
	receivers := []FileSystemSecurityTokenReceiver{
		FileSystemSecurityTokenReceiverFunc(func(token FileSystemSecurityToken) error {
			if !token.Revoked {
				return nil
			}
			notifications = append(notifications, 1)
			if _, ok := manager.Current(); ok {
				t.Error("revoked credentials are still available during notification")
			}
			return errors.New("receiver rejected the notification")
		}),
		FileSystemSecurityTokenReceiverFunc(func(token FileSystemSecurityToken) error {
			if token.Revoked {
				notifications = append(notifications, 2)
			}
			return nil
		}),
	}
	manager = newSecurityTokenManager(nil, tokenTestConfig(clock), receivers)
	if err := manager.publish(t.Context(), FileSystemSecurityToken{Schema: "test", Token: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if err := manager.publish(t.Context(), FileSystemSecurityToken{Schema: "test", Revoked: true}); !errors.Is(err, ErrFileSystemSecurityToken) {
		t.Fatalf("revocation notification error = %v", err)
	}
	if _, ok := manager.Current(); ok || !slices.Equal(notifications, []int{1, 2}) {
		t.Fatalf("current available=%v notification order=%v", ok, notifications)
	}
}

func TestRevokedSecurityTokenStaysClearedOnShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := &fakeSecurityTokenClock{now: time.Unix(1, 0)}
		provider := FileSystemSecurityTokenProviderFunc(func(context.Context) (FileSystemSecurityToken, error) {
			return FileSystemSecurityToken{Schema: "test", Revoked: true}, nil
		})
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		manager := newSecurityTokenManager(provider, tokenTestConfig(clock), []FileSystemSecurityTokenReceiver{
			FileSystemSecurityTokenReceiverFunc(func(token FileSystemSecurityToken) error {
				if token.Revoked {
					close(entered)
					<-release
				}
				return nil
			}),
		})
		if err := manager.publish(t.Context(), FileSystemSecurityToken{Schema: "test", Token: []byte{1}}); err != nil {
			t.Fatal(err)
		}
		manager.Start()
		<-entered
		manager.Stop()
		if _, ok := manager.Current(); ok {
			t.Error("revoked token remained after shutdown canceled a notification")
		}
	})
}

func TestSecurityTokenSchedulingClampsFinalJitter(t *testing.T) {
	clock := &fakeSecurityTokenClock{now: time.Unix(100, 0)}
	expiresAt := clock.Now().Add(101 * time.Second)
	cfg := tokenTestConfig(clock)
	cfg.RenewalTimeRatio, cfg.Jitter = 0.75, 1
	cfg.random = func() float64 { return 0.99 }
	manager := newSecurityTokenManager(nil, cfg, nil)
	if delay := manager.renewalDelay(expiresAt); delay != 100*time.Second-time.Nanosecond {
		t.Fatalf("upper renewal delay = %v", delay)
	}
	manager.failedDelay()
	manager.failedDelay()
	if delay := manager.failedDelay(); delay != 40*time.Second {
		t.Fatalf("upper retry delay = %v", delay)
	}
	manager.config.random = func() float64 { return 0 }
	if delay := manager.renewalDelay(expiresAt); delay != minSecurityTokenDelay {
		t.Fatalf("lower renewal delay = %v", delay)
	}
	if delay := manager.failedDelay(); delay != minSecurityTokenDelay {
		t.Fatalf("lower retry delay = %v", delay)
	}
	clock.Advance(100*time.Second - time.Nanosecond)
	manager.config.random = func() float64 { return 0.5 }
	if delay := manager.renewalDelay(expiresAt); delay != cfg.RenewalRetryBackoff {
		t.Fatalf("too-short renewal delay = %v", delay)
	}
	clock.Advance(time.Nanosecond)
	if delay := manager.renewalDelay(expiresAt); delay != cfg.RenewalRetryBackoff {
		t.Fatalf("expired renewal delay = %v", delay)
	}
}
