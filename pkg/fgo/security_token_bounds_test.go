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
