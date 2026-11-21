# whinny

**whinny is the apron link daemon and CLI — the room is a tool, not the I/O
surface.** The agent loop stays orthogonal to the chat room: silence is the
default, a post is an explicit act through one guarded send path, and the room
wakes the agent only when the wake policy says so.

One paragraph of architecture: the **wake layer** evaluates every inbound room
message against deterministic rules — mention, reply-to-my-message, registered
watches, and one ambient gate — and never decides whether the agent replies,
only whether it wakes. The **policy** is where the ack-storm firebreak lives:
a bot's own traffic only evaluates watches, never wakes anyone by its posts,
and a `peers` map distinguishes bots from humans. The **send path** is the
single outbound surface (`whinny post`): every post runs the guard chain —
persisted rate bucket, breaker, quiet gate — and refusals come back as typed
denials (`refused-rate-limit`, `refused-breaker`, `refused-quiet`), never as prompt
machinery. The breaker trips on five consecutive bad send outcomes (rate-limit
denials or failed posts) and latches until `whinny breaker-clear` releases it.

whinny speaks [Apron](https://github.com/shazow/apron) protocol v9 over
WebSockets (`internal/apronws`) and keeps its state in one SQLite database
(`internal/store`).

## Quickstart

```sh
# build
go build -o whinny ./cmd/whinny

# point at a real aprond. For a local one:
#   go build -o aprond ./cmd/aprond     (in apron-server-go)
#   ./aprond --addr 127.0.0.1:8070 --email.enable --email.sender log
# Sign-in codes then land in the server log. Enroll is the email ceremony:
# propose -> read the code -> approve on the same connection (the e2e helper
# does this non-interactively; see "End-to-end test" below).

export WHINNY_URL=ws://127.0.0.1:8070/ws
export WHINNY_TOKEN=<session token>
export WHINNY_DB=~/.local/share/whinny/state.db

# run the daemon (holds the connection, evaluates the wake policy)
whinny daemon --mode resident --deliver exec --exec-cmd 'notify-bot'

# in another terminal: arm a watch, wait for the room to match it
whinny watch add --room general --pattern 'deploy done' --note 'waiting for deploy' --ttl 1h

# post (the only outbound path; guard chain runs here)
whinny post --room general 'deploy is green, carrying on'
```

Prove the whole loop against a real server with the e2e suite — see
[End-to-end test](#end-to-end-test).

## Command reference

Common flags on every command: `--url <endpoint>` (`$WHINNY_URL`, then
`$APRON_URL`, default `ws://127.0.0.1:8070/ws`) and `--token <token>`
(`$WHINNY_TOKEN`, then `$APRON_TOKEN`; a missing token is a usage error).

- `whinny post <text>` — post a message; the guard chain runs before the
  connection is opened. Flags: `--room <id|title>` (required),
  `--reply-to <message_id>`, `--thread-of <message_id>` (create a thread and
  post the first reply; mutually exclusive with `--reply-to`), `--json`. `-` reads the text
  from stdin.
- `whinny history --room <id|title>` — print recent messages. Flags:
  `--limit <n>` (default 50), `--before <message_id>`, `--since <message_id>`
  (client-side cursor filters; a cursor outside the fetched window is a usage
  error), `--json`.
- `whinny rooms` — list joined and not-joined rooms. Flags: `--json`.
- `whinny watch add` — add a wake watch. Flags: `--room` (required), and at
  least one of `--from <user_id>`, `--pattern <regex>` (message text and embed
  titles/captions), `--thread <message_id>`, `--reply-to <message_id>`;
  `--note <text>` (echoed in the wake), `--once` (default) or
  `--persistent`, `--ttl <duration>` (default 24h, cap 168h). Shared mode
  only: `--owner <identity>` + `--on-fire <command>` register a transient
  watch whose hits fire through the owner's delivery command.
- `whinny watch cancel <id>` — cancel a watch by id.
- `whinny watch list` — list watches. Flags: `--json`.
- `whinny quiet --for <duration>` — gate the send path: posts are refused
  until the period ends (default 8h; `--for 0` is indefinite). Records keep
  flowing; quiet only gates sends.
- `whinny resume` — clear the quiet gate.
- `whinny breaker-clear` — clear the send-path breaker latch and reset the
  consecutive post-failure counter, so the next post is allowed.
- `whinny daemon` — run the wake daemon (long-running). Flags:
  `--mode resident|shared` (`$WHINNY_DAEMON_MODE`), `--deliver
  exec|resume|socket|feed` (`$WHINNY_DAEMON_DELIVER`), `--exec-cmd <cmd>`
  (shell-split), `--socket-url <url>`, `--config <path>`
  (`$WHINNY_CONFIG`). SIGINT/SIGTERM shut it down cleanly with exit 0.
- `whinny schema`, `whinny manifest`, `whinny version` — tool discovery
  documents (see [Registry manifests](#registry-manifests)).

## Daemon

**Resident mode** is the full wake policy for one resident identity: it holds
the apron connection, evaluates every inbound message, and delivers wakes
through the configured backend. Fired wakes are enqueued in the wake queue
and delivered immediately. Delivery is at-most-once: the queue only
protects a crash between enqueue and dequeue, and a failed delivery is
logged and dropped. Posting a message auto-subscribes its thread for a
day; disable with meta `auto_subscribe=off`.

**Shared mode** evaluates watches only — no wake queue, no redelivery. A watch
registered with `--owner <identity> --on-fire <command>` fires straight to
that owner's delivery command (the meta key `delivery:<owner>`), which is how
one daemon serves transient identities.

**Delivery backends** (`--deliver`):

- `exec` (default) — spawn `--exec-cmd` per wake, envelope on stdin, wait
  bounded at 30s. A failed delivery is logged, not fatal.
- `resume` — the same spawn shape; the caller's command owns the session
  semantics.
- `socket` — POST `{"text": <envelope>, "agent": <agent>}` to `--socket-url`.
- `feed` — write `{"text", "from", "image"?}` JSON lines to stdout (the poo
  feed runner's contract); image embeds are fetched into a private temp dir
  and referenced by path.

**Ambient gate** (daemon config file only): `ambient.endpoint` is a
classifier URL — POST `{"from": <user_id>, "text": <text>}`, read back
`{"directed": bool, "p": <0..1>}` (`p` wins when present, against
`ambient.threshold`). Transport errors fail open as one cheap no-op wake.
Valves: `min_spacing` (default 30m) and `max_per_hour` (default 4). An empty
endpoint disables the gate. Non-ambient paths — mention, reply, watch — are
deterministic and never consult the classifier.

**Config file** (`--config`, `$WHINNY_CONFIG`, default
`~/.config/whinny/daemon.json`, optional): URL and token never come from the
file — env and flags carry those.

```json
{
  "agent": "whinny-e2e",
  "mode": "resident",
  "deliver": "exec",
  "exec_cmd": ["notify-bot"],
  "peers": { "doug-e2e": { "name": "Doug", "is_bot": false } },
  "self": { "user_id": "whinny-e2e", "name": "whinny-e2e", "aliases": [] },
  "ambient": { "endpoint": "http://127.0.0.1:9100/directed", "threshold": 0.5 }
}
```

**Envelope format** — one delivered wake is a header line plus the message
text:

```
[wake kind=mention room=general from=doug-e2e message_id=1795241456365]
whinny-e2e can you look at the roster

[wake kind=watch id=w7 note="waiting for deploy" room=general from=doug-e2e message_id=...]
deploy done, all green

[wake kind=watch-expired id=w8 note="waiting for nothing"]
no match within ttl
```

Header values with spaces or quotes are double-quoted with escapes.

## State

One SQLite database (`$WHINNY_DB`, default `~/.local/share/whinny/state.db`):

- `watches` — registered watches: room, owner, kind
  (pattern/edge/thread/subscription), condition, note, once flag, ttl, expiry.
- `wake_queue` — durable wake envelopes awaiting delivery, per agent.
- `breaker` — single-row circuit breaker (latched, when, why).
- `cursors` — per-room last-seen `log_id`, maintained by the daemon.
- `meta` — guard state: `quiet_until` (the send-path quiet gate), the
  persisted rate bucket, `post_failures` (the breaker trip counter),
  `delivery:<owner>` shared-mode callbacks,
  `auto_subscribe`.

**Environment variables**: `WHINNY_URL` / `APRON_URL` (endpoint),
`WHINNY_TOKEN` / `APRON_TOKEN` (session token), `WHINNY_DB` (state path),
`WHINNY_CONFIG` (daemon config file), `WHINNY_DAEMON_MODE`,
`WHINNY_DAEMON_DELIVER`, `WHINNY_RATE_PER_MINUTE` (send-path rate limit,
default 20).

## Known gaps (v1)

- Feed image fetch is same-origin restricted and capped.
- Delivery runs synchronously in the record loop: a slow deliverer can
  backpressure the connection.

## Registry manifests

`whinny manifest` prints this tool's
[cli-registry](https://github.com/sdougbrown/cli-contract) manifest (commands,
flags, output modes), and `whinny schema` prints the full schema document. A
host that embeds the manifest — poo's registry does — can drive whinny's
commands without hardcoding them, so the room shows up to agents as plain
tools.

## End-to-end test

`e2e/run.sh` proves the whole loop against a real `aprond`:
builds both binaries, starts a fresh server on an ephemeral port with
sign-in codes going to the server log, enrolls a bot (`whinny-e2e`) and a
human (`doug-e2e`) through the email ceremony (`e2e/aprontool` speaks raw
Apron frames), runs the daemon in resident mode with exec delivery into a
sink file, and asserts the mention wake, a watch hit, a watch expiry, and the
quiet gate's refusal with no message posted. Everything runs under a `mktemp`
directory; waits poll conditions with deadlines.

```sh
e2e/run.sh
# APRON_SERVER_DIR=~/elsewhere/apron-server-go e2e/run.sh   # other server checkout
```

Requires Go (a toolchain matching whinny's go.mod; the script builds with
`GOTOOLCHAIN=auto`) and `curl`/`python3` for waits and JSON parsing.
