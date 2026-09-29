package fgo

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// DialContextFunc opens one network connection for the client.
type DialContextFunc func(context.Context, string, string) (net.Conn, error)

// Authenticator performs one Fluss Authenticate challenge exchange. An instance belongs to one
// server connection and must not be shared by concurrent connections.
type Authenticator interface {
	// Protocol returns the SASL mechanism name sent to Fluss.
	Protocol() string
	// HasInitialResponse reports whether Authenticate should run before the
	// first server challenge.
	HasInitialResponse() bool
	// Authenticate consumes one challenge and returns the next response.
	// Implementations must not retain or expose challenge or response bytes.
	Authenticate(context.Context, []byte) ([]byte, error)
	// Complete reports whether the exchange has reached a terminal success
	// state.
	Complete() bool
	// Close releases mechanism-specific state and secret material.
	Close() error
}

// AuthenticatorFactory creates a fresh authenticator for each server connection.
type AuthenticatorFactory func() (Authenticator, error)

// AuthenticationError reports whether a failed authentication exchange may be retried on a new
// connection. Its message intentionally never includes authentication tokens or credentials.
type AuthenticationError struct {
	// Err is the underlying mechanism or transport failure.
	Err error
	// Retriable reports whether a fresh connection may repeat authentication.
	Retriable bool
}

// Error returns a credential-safe authentication summary.
func (e *AuthenticationError) Error() string {
	if e != nil && e.Retriable {
		return ErrAuthentication.Error() + " (retriable)"
	}
	return ErrAuthentication.Error()
}

// Unwrap returns the underlying authentication failure.
func (e *AuthenticationError) Unwrap() error { return e.Err }

// Is reports whether target is [ErrAuthentication].
func (e *AuthenticationError) Is(target error) bool { return target == ErrAuthentication }

func (c *Client) authenticate(ctx context.Context, auth Authenticator) error {
	if auth == nil || auth.Protocol() == "" {
		return authenticationError(fmt.Errorf("invalid authenticator"), false)
	}
	token, err := initialAuthenticationToken(ctx, auth)
	if err != nil {
		return err
	}
	for step := 0; step < 16; step++ {
		challenge, complete, err := c.authenticationChallenge(ctx, auth, token)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		token, complete, err = authenticationResponseToken(ctx, auth, challenge)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
	}
	return authenticationError(fmt.Errorf("authentication exchange exceeded 16 steps"), false)
}

func initialAuthenticationToken(ctx context.Context, auth Authenticator) ([]byte, error) {
	if !auth.HasInitialResponse() {
		return nil, nil
	}
	token, err := auth.Authenticate(ctx, nil)
	if err != nil {
		return nil, authenticationError(err, false)
	}
	if token == nil {
		return nil, authenticationError(fmt.Errorf("initial response is missing"), false)
	}
	return token, nil
}

func (c *Client) authenticationChallenge(ctx context.Context, auth Authenticator, token []byte) ([]byte, bool, error) {
	response, err := c.authenticateRequest(ctx, auth.Protocol(), token)
	if err != nil {
		classified := serverError(err, fmsg.APIKeyAuthenticate, c.address)
		return nil, false, authenticationError(classified, isRetriableAuthenticationError(classified))
	}
	// Fluss SASL/PLAIN returns a present, empty final challenge. The Java
	// client treats any server response received after local completion as success.
	if auth.Complete() {
		return nil, true, nil
	}
	if response.Challenge != nil {
		return append([]byte(nil), response.Challenge...), false, nil
	}
	return nil, false, authenticationError(fmt.Errorf("server completed exchange before authenticator completed"), false)
}

func authenticationResponseToken(ctx context.Context, auth Authenticator, challenge []byte) ([]byte, bool, error) {
	token, err := auth.Authenticate(ctx, challenge)
	if err != nil {
		return nil, false, authenticationError(err, false)
	}
	if token != nil {
		return token, false, nil
	}
	if auth.Complete() {
		return nil, true, nil
	}
	return nil, false, authenticationError(fmt.Errorf("authenticator returned no response before completion"), false)
}

func (c *Client) authenticateRequest(ctx context.Context, protocol string, token []byte) (*fmsg.AuthenticateResponse, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClosed
	}
	version, ok := c.versions[fmsg.APIKeyAuthenticate]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: AUTHENTICATE", ErrUnsupportedAPI)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyAuthenticate, 0)
	if err != nil {
		return nil, err
	}
	if err := request.SetVersion(version); err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.AuthenticateRequest)
	message.Protocol = proto.String(protocol)
	message.Token = append([]byte(nil), token...)
	response, err := c.requester.Request(ctx, request)
	if err != nil {
		return nil, err
	}
	authenticateResponse, ok := response.Message().(*fmsg.AuthenticateResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected authentication response %T", response.Message())
	}
	return authenticateResponse, nil
}

func authenticationError(err error, retriable bool) error {
	if err == nil {
		return nil
	}
	return &AuthenticationError{Err: err, Retriable: retriable}
}

func isRetriableAuthenticationError(err error) bool {
	var authenticationError *AuthenticationError
	if errors.As(err, &authenticationError) {
		return authenticationError.Retriable
	}
	var serverError *ServerError
	return errors.As(err, &serverError) && serverError.Retriable &&
		serverError.Code == fmsg.ErrorCodeRetriableAuthenticateException
}
