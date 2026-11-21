package cli

import (
	"fmt"
)

// Class discriminates a CLI error's exit-code class.
type Class int

const (
	// ClassUsage is a caller error: bad input, missing config, or a room
	// the caller named does not resolve. The adapter exits 2.
	ClassUsage Class = iota
	// ClassOperational is a failure outside the caller's control: connect
	// failures, server errors. The adapter exits 1.
	ClassOperational
)

// Error is a typed CLI error; Class decides the exit code.
type Error struct {
	Class   Class
	Message string
}

func (e *Error) Error() string { return e.Message }

// UsageErrorf builds a usage-class error.
func UsageErrorf(format string, args ...any) error {
	return &Error{Class: ClassUsage, Message: fmt.Sprintf(format, args...)}
}

// OperationalErrorf builds an operational-class error.
func OperationalErrorf(format string, args ...any) error {
	return &Error{Class: ClassOperational, Message: fmt.Sprintf(format, args...)}
}
