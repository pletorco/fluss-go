package fgo

import (
	"context"
	"errors"
	"testing"
)

func TestSASLPlainAuthenticator(t *testing.T) {
	factory := SASLPlainAuthenticator("alice", "secret")
	first, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	second, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("SASLPlainAuthenticator() reused an authenticator")
	}
	if first.Protocol() != "PLAIN" || !first.HasInitialResponse() {
		t.Fatalf("PLAIN capabilities = protocol %q initial %t", first.Protocol(), first.HasInitialResponse())
	}
	token, err := first.Authenticate(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(token), "\x00alice\x00secret"; got != want {
		t.Fatalf("PLAIN token = %q, want %q", got, want)
	}
	if !first.Complete() {
		t.Fatal("PLAIN authenticator is not complete after initial response")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Authenticate(context.Background(), nil); err == nil {
		t.Fatal("Authenticate after Close/completion error = nil")
	}
}

func TestSASLPlainAuthenticatorRejectsInvalidCredentials(t *testing.T) {
	for _, factory := range []AuthenticatorFactory{
		SASLPlainAuthenticator("", "secret"),
		SASLPlainAuthenticator("alice", ""),
	} {
		if _, err := factory(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("factory error = %v, want ErrInvalidConfig", err)
		}
	}
	auth, err := SASLPlainAuthenticator("alice", "secret")()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Authenticate(context.Background(), []byte("malformed")); err == nil {
		t.Fatal("PLAIN malformed challenge error = nil")
	}
}
