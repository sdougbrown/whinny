// Command whinny talks to apron rooms from the command line.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

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

func (adapter) Deadline(ctx context.Context, _ clicontract.Invocation) (context.Context, context.CancelFunc) {
	return ctx, func() {}
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
