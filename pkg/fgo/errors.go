package fgo

import (
	"errors"
	"fmt"

	"github.com/pletorco/fluss-go/internal/transport"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

var (
	ErrServerFailure = errors.New("fgo: server failure")
	ErrAuthorization = errors.New("fgo: authorization failure")
	ErrMetadata      = errors.New("fgo: stale or invalid metadata")
	ErrTimeout       = errors.New("fgo: server timeout")
	ErrStorage       = errors.New("fgo: storage failure")
	ErrSequence      = errors.New("fgo: sequence failure")
	ErrRecord        = errors.New("fgo: record failure")
	ErrValidation    = errors.New("fgo: validation failure")
)

// ServerError is a Fluss 0.9.1 server failure with safe request context.
// Unknown future error codes remain inspectable and are never retriable by default.
type ServerError struct {
	Code      fmsg.ErrorCode
	Name      string
	Message   string
	API       fmsg.APIKey
	Endpoint  string
	Retriable bool
	category  error
}

func (e *ServerError) Error() string {
	if e == nil {
		return ErrServerFailure.Error()
	}
	location := ""
	if e.Endpoint != "" {
		location = " from " + e.Endpoint
	}
	if e.Message == "" {
		return fmt.Sprintf("%s: %s%s", ErrServerFailure, e.Name, location)
	}
	return fmt.Sprintf("%s: %s%s: %s", ErrServerFailure, e.Name, location, e.Message)
}

func (e *ServerError) Is(target error) bool {
	return target == ErrServerFailure || (e != nil && target == e.category)
}

func serverError(err error, api fmsg.APIKey, endpoint string) error {
	var remote *transport.RemoteError
	if !errors.As(err, &remote) {
		return err
	}
	metadata, known := fmsg.LookupErrorCode(remote.Code)
	name := "UNKNOWN_FUTURE_ERROR"
	code := fmsg.ErrorCode(remote.Code)
	if known {
		name = metadata.Name
		code = metadata.Code
	}
	return &ServerError{
		Code: code, Name: name, Message: remote.Message, API: api, Endpoint: endpoint,
		Retriable: known && retriableErrorCode(code), category: errorCategory(code),
	}
}

func responseServerError(code int32, message string, api fmsg.APIKey) error {
	if code == int32(fmsg.ErrorCodeNone) {
		return nil
	}
	metadata, known := fmsg.LookupErrorCode(code)
	name := "UNKNOWN_FUTURE_ERROR"
	typedCode := fmsg.ErrorCode(code)
	if known {
		name, typedCode = metadata.Name, metadata.Code
	}
	return &ServerError{
		Code: typedCode, Name: name, Message: message, API: api,
		Retriable: known && retriableErrorCode(typedCode), category: errorCategory(typedCode),
	}
}

func errorCategory(code fmsg.ErrorCode) error {
	switch code {
	case fmsg.ErrorCodeAuthenticateException, fmsg.ErrorCodeRetriableAuthenticateException:
		return ErrAuthentication
	case fmsg.ErrorCodeAuthorizationException, fmsg.ErrorCodeSecurityDisabledException, fmsg.ErrorCodeSecurityTokenException:
		return ErrAuthorization
	case fmsg.ErrorCodeNotLeaderOrFollower, fmsg.ErrorCodeUnknownTableOrBucketException,
		fmsg.ErrorCodeInvalidCoordinatorException, fmsg.ErrorCodeFencedLeaderEpochException,
		fmsg.ErrorCodeLeaderNotAvailableException, fmsg.ErrorCodeServerNotExistException:
		return ErrMetadata
	case fmsg.ErrorCodeRequestTimeOut, fmsg.ErrorCodeNetworkException:
		return ErrTimeout
	case fmsg.ErrorCodeLogStorageException, fmsg.ErrorCodeKvStorageException, fmsg.ErrorCodeStorageException:
		return ErrStorage
	case fmsg.ErrorCodeOutOfOrderSequenceException, fmsg.ErrorCodeDuplicateSequenceException, fmsg.ErrorCodeUnknownWriterIdException:
		return ErrSequence
	case fmsg.ErrorCodeRecordTooLargeException, fmsg.ErrorCodeCorruptRecordException:
		return ErrRecord
	default:
		return ErrValidation
	}
}

func retriableErrorCode(code fmsg.ErrorCode) bool {
	switch code {
	case fmsg.ErrorCodeNetworkException, fmsg.ErrorCodeNotLeaderOrFollower,
		fmsg.ErrorCodeInvalidCoordinatorException, fmsg.ErrorCodeRequestTimeOut,
		fmsg.ErrorCodeOperationNotAttemptedException, fmsg.ErrorCodeNotEnoughReplicasAfterAppendException,
		fmsg.ErrorCodeNotEnoughReplicasException, fmsg.ErrorCodeLeaderNotAvailableException,
		fmsg.ErrorCodeRetriableAuthenticateException, fmsg.ErrorCodeIneligibleReplicaException:
		return true
	default:
		return false
	}
}
