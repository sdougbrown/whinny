// Command aprontool is the whinny e2e helper peer: it speaks raw Apron
// WebSocket frames against a real aprond. It exists so e2e/run.sh can
// enroll identities and post as a human without going through whinny.
//
// Usage:
//
//	aprontool enroll --url ws://host/ws --email E --user-id U --name N --log PATH
//	aprontool say    --url ws://host/ws --token T --room R --text S [--mention M]...
//
// enroll runs the email sign-in ceremony (propose, read the six-digit
// code from the server's log — aprond --email.sender log writes codes
// there — then approve on the same connection) and prints
// {"user_id": ..., "token": ...}. The account joins the default room at
// sign-in, so the token is usable immediately.
//
// say resumes the session with the token scheme and posts one message
// frame; it prints the server's result as JSON.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// frame is one Apron wire frame.
type frame struct {
	ID     string          `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("apron %d: %s", e.Code, e.Message) }

// peer is one raw WebSocket connection speaking Apron frames.
type peer struct {
	conn *websocket.Conn
	ctx  context.Context
	seq  int
}

func dial(ctx context.Context, url string) (*peer, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}
	return &peer{conn: conn, ctx: ctx}, nil
}

// call sends one request frame and reads until its reply arrives,
// skipping pushes.
func (p *peer) call(method string, params any) (json.RawMessage, error) {
	p.seq++
	id := fmt.Sprintf("tool-%d-%d", os.Getpid(), p.seq)
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if err := p.write(frame{ID: id, Method: method, Params: raw}); err != nil {
		return nil, err
	}
	for {
		var f frame
		if err := p.read(&f); err != nil {
			return nil, err
		}
		if f.ID != id {
			continue // a push; replies only carry the request's id
		}
		if f.Error != nil {
			return nil, f.Error
		}
		return f.Result, nil
	}
}

func (p *peer) write(f frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return p.conn.Write(p.ctx, websocket.MessageText, b)
}

func (p *peer) read(f *frame) error {
	_, data, err := p.conn.Read(p.ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, f)
}

func (p *peer) close() {
	_ = p.conn.Close(websocket.StatusNormalClosure, "done")
}

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: aprontool enroll|say [flags]")
		return 2
	}
	var err error
	switch args[0] {
	case "enroll":
		err = enroll(args[1:])
	case "say":
		err = say(args[1:])
	default:
		usagef("unknown subcommand %q", args[0])
		return 2
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "aprontool: %v\n", err)
		return 1
	}
	return 0
}

func usagef(format string, a ...any) error {
	fmt.Fprintf(os.Stderr, "aprontool: "+format+"\n", a...)
	return errors.New("usage")
}

// enroll runs the email sign-in ceremony for one identity.
func enroll(args []string) error {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	url := fs.String("url", "", "apron WebSocket endpoint")
	email := fs.String("email", "", "email address to enroll")
	userID := fs.String("user-id", "", "requested user_id (Appendix A.3 convention)")
	name := fs.String("name", "", "requested profile name")
	logPath := fs.String("log", "", "path of the aprond log file (email.sender log)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" || *email == "" || *logPath == "" {
		return errors.New("enroll: --url, --email, and --log are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p, err := dial(ctx, *url)
	if err != nil {
		return err
	}
	defer p.close()

	// The log offset marks where this identity's code can appear: the
	// propose below is the only thing that writes a code after it.
	offset, err := logSize(*logPath)
	if err != nil {
		return fmt.Errorf("enroll: read log: %w", err)
	}
	if _, err := p.call("auth", map[string]any{
		"scheme":  "email",
		"email":   *email,
		"user_id": *userID,
		"name":    *name,
	}); err != nil {
		return fmt.Errorf("enroll: propose: %w", err)
	}
	code, err := waitForCode(*logPath, offset, 20*time.Second)
	if err != nil {
		return err
	}
	result, err := p.call("auth", map[string]any{"scheme": "email", "token": code})
	if err != nil {
		return fmt.Errorf("enroll: approve: %w", err)
	}
	var signed struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(result, &signed); err != nil || signed.Token == "" {
		return fmt.Errorf("enroll: approve result carries no token: %s", string(result))
	}
	out, _ := json.Marshal(map[string]string{"user_id": *userID, "token": signed.Token})
	fmt.Println(string(out))
	return nil
}

// logSize returns the file's current size, 0 for a missing file.
func logSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// waitForCode polls the log from offset for a sign-in code
// (`code=<digits>`, as the development sender logs it).
func waitForCode(path string, offset int64, within time.Duration) (string, error) {
	re := regexp.MustCompile(`code="?(\d{4,12})`)
	deadline := time.Now().Add(within)
	for {
		data, err := os.ReadFile(path)
		if err == nil && int64(len(data)) > offset {
			if m := re.FindSubmatch(data[offset:]); m != nil {
				return string(m[1]), nil
			}
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("enroll: no sign-in code appeared in %s within %s", path, within)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// say resumes a session and posts one message frame.
func say(args []string) error {
	fs := flag.NewFlagSet("say", flag.ContinueOnError)
	url := fs.String("url", "", "apron WebSocket endpoint")
	token := fs.String("token", "", "session token to resume")
	room := fs.String("room", "", "target room_id")
	text := fs.String("text", "", "message text")
	var mentions []string
	fs.Func("mention", "user_id to add to body.mentions (repeatable)", func(v string) error {
		mentions = append(mentions, v)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" || *token == "" || *room == "" || *text == "" {
		return errors.New("say: --url, --token, --room, and --text are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p, err := dial(ctx, *url)
	if err != nil {
		return err
	}
	defer p.close()

	if _, err := p.call("auth", map[string]any{"scheme": "token", "token": *token}); err != nil {
		return fmt.Errorf("say: auth: %w", err)
	}
	body := map[string]any{"text": *text, "format": "plain"}
	if len(mentions) > 0 {
		body["mentions"] = mentions
	}
	result, err := p.call("message", map[string]any{"room_id": *room, "body": body})
	if err != nil {
		return fmt.Errorf("say: message: %w", err)
	}
	fmt.Println(strings.TrimRight(string(bytes.TrimSpace(result)), "\n"))
	return nil
}
