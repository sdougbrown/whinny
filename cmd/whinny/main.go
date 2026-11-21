// Command whinny talks to apron rooms from the command line.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/cli"
	"github.com/sdougbrown/whinny/internal/store"
)

func main() {
	os.Exit(clicontract.Run(cli.App(deps()), adapter{}, os.Args[1:], os.Stdout, os.Stderr))
}

func deps() cli.Deps {
	env := func(k string) string { v, _ := os.LookupEnv(k); return v }
	return cli.Deps{
		Connect: func(ctx context.Context, cfg cli.Config) (cli.RoomClient, error) {
			return apronws.Connect(ctx, apronws.Options{URL: cfg.URL, Token: cfg.Token})
		},
		OpenStore: func() (*store.Store, error) {
			return cli.OpenStore(env)
		},
		Env:   env,
		Stdin: os.Stdin,
	}
}

// adapter writes string results as plain lines and structured results
// as one JSON value, and maps usage-class failures to the usage exit
// code (2) and everything else to the operational code (1).
type adapter struct{}

func (adapter) ErrorResult(command string, err error) any {
	code := "operational"
	if isUsage(err) {
		code = "usage"
	}
	return map[string]any{
		"command": command,
		"status":  "error",
		"error":   map[string]any{"code": code, "message": err.Error()},
	}
}

func (adapter) Write(w io.Writer, result any) error {
	if s, ok := result.(string); ok {
		if s != "" {
			fmt.Fprintln(w, s)
		}
		return nil
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func (adapter) ExitCode(_ clicontract.Invocation, _ any, handlerErr error) int {
	if handlerErr != nil {
		if isUsage(handlerErr) {
			return 2
		}
		return clicontract.ExitOperational
	}
	return clicontract.ExitOK
}

// commandDeadline bounds every interactive (non-daemon) command. A server
// that accepts a request but never replies must not hang the command
// forever (apronws.Send blocks on the context).
const commandDeadline = 60 * time.Second

// Deadline decorates the run context before a handler runs. Interactive
// commands (post, history, rooms, watch, quiet, resume) get a finite
// deadline so a server that accepts a request but never replies cannot
// hang the command forever. The daemon is a Raw handler: the library
// never decorates a raw command's context, and Deadline explicitly leaves
// it unbounded so the long-running wake daemon is never cut off.
func (adapter) Deadline(ctx context.Context, inv clicontract.Invocation) (context.Context, context.CancelFunc) {
	if inv.Command == "daemon" {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, commandDeadline)
}

func (adapter) BeforeHandler(_ clicontract.Invocation, _ io.Writer) {}

func (adapter) Validate(_ clicontract.Invocation) error { return nil }

// isUsage reports whether the error is a caller error: a parse failure,
// a missing-config or bad-input cli.Error, or a room-resolution
// failure.
func isUsage(err error) bool {
	var usage *clicontract.UsageError
	var cliErr *cli.Error
	var roomErr *cli.RoomError
	switch {
	case errors.As(err, &usage):
		return true
	case errors.As(err, &cliErr):
		return cliErr.Class == cli.ClassUsage
	case errors.As(err, &roomErr):
		return true
	}
	return false
}
