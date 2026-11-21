package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/cli"
)

func TestAdapterExitCode(t *testing.T) {
	a := adapter{}
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, 0},
		{"parse usage error", &clicontract.UsageError{Message: "bad flag"}, 2},
		{"cli usage error", &cli.Error{Class: cli.ClassUsage, Message: "no token"}, 2},
		{"room error", &cli.RoomError{Kind: cli.RoomNoMatch, Query: "x"}, 2},
		{"operational error", errors.New("connect: refused"), 1},
		{"cli operational error", &cli.Error{Class: cli.ClassOperational, Message: "server said no"}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.ExitCode(clicontract.Invocation{}, nil, tc.err); got != tc.want {
				t.Fatalf("ExitCode = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestAdapterWrite(t *testing.T) {
	a := adapter{}
	var b bytes.Buffer
	if err := a.Write(&b, "msg-1"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := b.String(); got != "msg-1\n" {
		t.Fatalf("string write = %q, want a plain line", got)
	}
	b.Reset()
	if err := a.Write(&b, map[string]any{"message_id": "msg-1"}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b.Bytes(), &m); err != nil {
		t.Fatalf("structured write %q is not JSON: %v", b.String(), err)
	}
	if m["message_id"] != "msg-1" {
		t.Fatalf("message_id = %v, want msg-1", m["message_id"])
	}
}

func TestAdapterErrorResult(t *testing.T) {
	a := adapter{}
	res := a.ErrorResult("post", &cli.Error{Class: cli.ClassUsage, Message: "no token"})
	m, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("ErrorResult = %T, want a map", res)
	}
	if m["command"] != "post" || m["status"] != "error" {
		t.Fatalf("envelope = %v, want command/status", m)
	}
	errObj, ok := m["error"].(map[string]any)
	if !ok {
		t.Fatalf("error member = %T, want a map", m["error"])
	}
	if errObj["code"] != "usage" || errObj["message"] != "no token" {
		t.Fatalf("error = %v, want usage/no token", errObj)
	}
	res = a.ErrorResult("post", errors.New("boom"))
	m = res.(map[string]any)
	if m["error"].(map[string]any)["code"] != "operational" {
		t.Fatalf("code = %v, want operational", m["error"])
	}
}

func TestAdapterErrorResultRoomError(t *testing.T) {
	a := adapter{}
	res := a.ErrorResult("post", &cli.RoomError{Kind: cli.RoomNoMatch, Query: "x"})
	m := res.(map[string]any)
	if m["error"].(map[string]any)["code"] != "usage" {
		t.Fatalf("code = %v, want usage for a room error", m["error"])
	}
	if !strings.Contains(m["error"].(map[string]any)["message"].(string), "no room matches") {
		t.Fatalf("message = %v, want the no-room-match text", m["error"])
	}
}
