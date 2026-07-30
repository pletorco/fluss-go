package fgo

import (
	"context"
	"fmt"
)

// PlainAuthenticator returns a factory for the SASL PLAIN mechanism supported by Fluss 0.9.1.
// Each connection receives its own credentials buffer, which is cleared when the connection closes.
func PlainAuthenticator(username, password string) AuthenticatorFactory {
	return func() (Authenticator, error) {
		if username == "" || password == "" {
			return nil, fmt.Errorf("%w: SASL PLAIN username and password are required", ErrInvalidConfig)
		}
		return &plainAuthenticator{
			username: []byte(username),
			password: []byte(password),
		}, nil
	}
}

type plainAuthenticator struct {
	username  []byte
	password  []byte
	completed bool
}

func (*plainAuthenticator) Protocol() string         { return "PLAIN" }
func (*plainAuthenticator) HasInitialResponse() bool { return true }
func (a *plainAuthenticator) Complete() bool         { return a.completed }

func (a *plainAuthenticator) Authenticate(_ context.Context, challenge []byte) ([]byte, error) {
	if a.completed {
		return nil, fmt.Errorf("SASL PLAIN does not accept a server challenge")
	}
	if len(challenge) != 0 {
		return nil, fmt.Errorf("SASL PLAIN received an unexpected server challenge")
	}
	token := make([]byte, 0, len(a.username)+len(a.password)+2)
	token = append(token, 0)
	token = append(token, a.username...)
	token = append(token, 0)
	token = append(token, a.password...)
	a.completed = true
	return token, nil
}

func (a *plainAuthenticator) Close() error {
	clear(a.username)
	clear(a.password)
	a.username = nil
	a.password = nil
	return nil
}
