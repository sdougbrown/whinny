package apronws

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Sentinel errors returned by the client, independent of the server.
var (
	// ErrNotAuthenticated is returned by Send and Ping when the connection
	// is not authenticated: before the initial auth completes, or while a
	// reconnect is in flight. Nothing is sent to the server.
	ErrNotAuthenticated = errors.New("apronws: not authenticated")

	// ErrConnectionLost is returned for in-flight requests when the
	// connection drops before the server replies.
	ErrConnectionLost = errors.New("apronws: connection lost")

	// ErrClientClosed is returned for in-flight requests when the client
	// is closed.
	ErrClientClosed = errors.New("apronws: client closed")
)

// Error is a server-side request error.
type Error struct {
	Code    int
	Message string
	// Data is the raw error data object, when present.
	Data json.RawMessage
	// RetryAfter is the server-suggested wait for rate-limited requests
	// (error data field "retry_after", in seconds). Zero when absent.
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("apronws: server error %d: %s (retry after %s)", e.Code, e.Message, e.RetryAfter)
	}
	return fmt.Sprintf("apronws: server error %d: %s", e.Code, e.Message)
}

// wireError is the error object of a reply frame.
type wireError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (e *wireError) clientError() *Error {
	ce := &Error{Code: e.Code, Message: e.Message, Data: e.Data}
	if e.Data != nil {
		var d struct {
			RetryAfter float64 `json:"retry_after"`
		}
		if json.Unmarshal(e.Data, &d) == nil && d.RetryAfter > 0 {
			ce.RetryAfter = time.Duration(d.RetryAfter * float64(time.Second))
		}
	}
	return ce
}
