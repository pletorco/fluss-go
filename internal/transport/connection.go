// Package transport implements the bounded Fluss RPC connection.
package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

const (
	requestHeaderSize  = 8
	responseHeaderSize = 5
	defaultMaxFrame    = 64 << 20
	defaultInFlight    = 1024
)

var (
	ErrClosed        = errors.New("transport: closed")
	ErrFrameTooLarge = errors.New("transport: frame too large")
	ErrMalformed     = errors.New("transport: malformed frame")
)

type RemoteError struct {
	Code    int32
	Message string
}

func (e *RemoteError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("transport: remote error %d", e.Code)
	}
	return fmt.Sprintf("transport: remote error %d: %s", e.Code, e.Message)
}

type Config struct {
	MaxFrameSize int
	MaxInFlight  int
}

type Connection struct {
	conn     net.Conn
	maxFrame int
	sem      chan struct{}

	writeMu  sync.Mutex
	mu       sync.Mutex
	nextID   int32
	pending  map[int32]chan result
	closed   bool
	closeErr error
	done     chan struct{}
}

type result struct {
	body []byte
	err  error
}

func New(conn net.Conn, cfg Config) (*Connection, error) {
	if conn == nil {
		return nil, fmt.Errorf("%w: nil connection", fmsg.ErrInvalidArgument)
	}
	if cfg.MaxFrameSize == 0 {
		cfg.MaxFrameSize = defaultMaxFrame
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = defaultInFlight
	}
	if cfg.MaxFrameSize < responseHeaderSize || cfg.MaxInFlight < 1 {
		return nil, fmt.Errorf("%w: invalid transport limits", fmsg.ErrInvalidArgument)
	}
	c := &Connection{conn: conn, maxFrame: cfg.MaxFrameSize, sem: make(chan struct{}, cfg.MaxInFlight), pending: make(map[int32]chan result), done: make(chan struct{})}
	go c.readLoop()
	return c, nil
}

func (c *Connection) Request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: nil request", fmsg.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	if len(body)+requestHeaderSize > c.maxFrame {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(body)+requestHeaderSize)
	}
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.err()
	}
	defer func() { <-c.sem }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	id, resultCh, err := c.register()
	if err != nil {
		return nil, err
	}
	defer c.unregister(id)
	discard, err := c.writeRequest(ctx, id, request, body)
	if err != nil {
		if discard {
			terminal := fmt.Errorf("%w: write failed: %v", ErrClosed, err)
			c.fail(terminal)
			return nil, fmt.Errorf("transport: connection discarded: %w: %w", err, terminal)
		}
		return nil, err
	}
	select {
	case outcome := <-resultCh:
		if outcome.err != nil {
			return nil, outcome.err
		}
		response := request.NewResponse()
		if response == nil {
			return nil, fmt.Errorf("%w: no response constructor", ErrMalformed)
		}
		if err := response.Unmarshal(outcome.body); err != nil {
			return nil, err
		}
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.err()
	}
}

func (c *Connection) Close() error {
	c.fail(ErrClosed)
	return nil
}

func (c *Connection) register() (int32, chan result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		if c.closeErr == nil {
			return 0, nil, ErrClosed
		}
		return 0, nil, c.closeErr
	}
	for i := 0; i < int(^uint32(0)); i++ {
		c.nextID++
		if c.nextID <= 0 {
			c.nextID = 1
		}
		if _, exists := c.pending[c.nextID]; !exists {
			ch := make(chan result, 1)
			c.pending[c.nextID] = ch
			return c.nextID, ch, nil
		}
	}
	return 0, nil, fmt.Errorf("%w: request IDs exhausted", ErrClosed)
}

func (c *Connection) unregister(id int32) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Connection) takePending(id int32) chan result {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	return ch
}

func (c *Connection) writeRequest(
	ctx context.Context,
	id int32,
	request fmsg.Request,
	body []byte,
) (bool, error) {
	frame := make([]byte, 4+requestHeaderSize+len(body))
	binary.BigEndian.PutUint32(frame, uint32(requestHeaderSize+len(body)))
	binary.BigEndian.PutUint16(frame[4:], uint16(request.APIKey()))
	binary.BigEndian.PutUint16(frame[6:], uint16(request.Version()))
	binary.BigEndian.PutUint32(frame[8:], uint32(id))
	copy(frame[12:], body)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	finish, err := c.prepareWrite(ctx)
	if err != nil {
		return true, err
	}
	written, writeErr := c.writeFrame(ctx, frame)
	finishErr := finish()
	if finishErr != nil {
		return true, errors.Join(writeErr, finishErr)
	}
	if writeErr == nil {
		return false, nil
	}
	discard := written != 0 ||
		(!errors.Is(writeErr, context.Canceled) &&
			!errors.Is(writeErr, context.DeadlineExceeded))
	return discard, writeErr
}

func (c *Connection) prepareWrite(ctx context.Context) (func() error, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.conn.SetWriteDeadline(deadline); err != nil {
			return nil, fmt.Errorf("transport: set write deadline: %w", err)
		}
	}
	interruptDone := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() {
		_ = c.conn.SetWriteDeadline(time.Now())
		close(interruptDone)
	})
	return func() error {
		if !stopInterrupt() {
			<-interruptDone
		}
		if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
			return fmt.Errorf("transport: clear write deadline: %w", err)
		}
		return nil
	}, nil
}

func (c *Connection) writeFrame(ctx context.Context, frame []byte) (int, error) {
	total := 0
	for len(frame) > 0 {
		written, err := c.conn.Write(frame)
		if written < 0 || written > len(frame) {
			return total, io.ErrShortWrite
		}
		total += written
		frame = frame[written:]
		if err != nil {
			return total, interruptedWriteError(ctx, err)
		}
		if written == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func interruptedWriteError(ctx context.Context, writeErr error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("transport: write interrupted: %w", ctxErr)
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return fmt.Errorf("transport: write interrupted: %w", context.DeadlineExceeded)
	}
	return writeErr
}

func (c *Connection) readLoop() {
	for {
		frame, err := c.readFrame()
		if err != nil {
			c.fail(err)
			return
		}
		if err := c.handleFrame(frame); err != nil {
			c.fail(err)
			return
		}
	}
}

func (c *Connection) readFrame() ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(header[:]))
	if size < 1 {
		return nil, fmt.Errorf("%w: zero-length frame", ErrMalformed)
	}
	if size > c.maxFrame {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, size)
	}
	frame := make([]byte, size)
	if _, err := io.ReadFull(c.conn, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func (c *Connection) handleFrame(frame []byte) error {
	if len(frame) == 0 {
		return fmt.Errorf("%w: empty response frame", ErrMalformed)
	}
	switch frame[0] {
	case 0:
		if len(frame) < responseHeaderSize {
			return fmt.Errorf("%w: short response header", ErrMalformed)
		}
		id := int32(binary.BigEndian.Uint32(frame[1:5]))
		c.deliver(id, result{body: append([]byte(nil), frame[5:]...), err: nil})
		return nil
	case 1:
		if len(frame) < responseHeaderSize {
			return fmt.Errorf("%w: short response header", ErrMalformed)
		}
		var remote fmsg.ErrorResponse
		if err := proto.Unmarshal(frame[5:], &remote); err != nil {
			return fmt.Errorf("%w: invalid error response: %v", ErrMalformed, err)
		}
		id := int32(binary.BigEndian.Uint32(frame[1:5]))
		c.deliver(id, result{err: &RemoteError{Code: remote.GetErrorCode(), Message: remote.GetErrorMessage()}})
		return nil
	case 2:
		var remote fmsg.ErrorResponse
		if err := proto.Unmarshal(frame[1:], &remote); err != nil {
			return fmt.Errorf("%w: invalid server failure: %v", ErrMalformed, err)
		}
		return &RemoteError{Code: remote.GetErrorCode(), Message: remote.GetErrorMessage()}
	default:
		return fmt.Errorf("%w: unknown response type %d", ErrMalformed, frame[0])
	}
}

func (c *Connection) deliver(id int32, outcome result) {
	ch := c.takePending(id)
	if ch != nil {
		complete(ch, outcome)
	}
}

func (c *Connection) fail(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closeErr = err
	pending := c.pending
	c.pending = make(map[int32]chan result)
	close(c.done)
	c.mu.Unlock()
	_ = c.conn.Close()
	for _, ch := range pending {
		complete(ch, result{err: err})
	}
}

func complete(ch chan result, outcome result) {
	select {
	case ch <- outcome:
	default:
	}
}

func (c *Connection) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr == nil {
		return ErrClosed
	}
	return c.closeErr
}
