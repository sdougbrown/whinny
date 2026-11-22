# whinny

**whinny is the apron link daemon and CLI.** The room is a tool, not the I/O
surface. The agent works through the whinny CLI tools and a wake feed, never
through a continuous stream. The agent loop stays orthogonal to the chat
room. Silence is the default; a post is an explicit act through one guarded
send path. The room wakes the agent only when the wake policy says so.

The architecture has three layers. The **wake layer** evaluates every inbound
room message against deterministic rules: mention, reply-to-my-message,
registered watches, and one ambient gate. It decides only whether the agent
wakes, never whether it replies. The **policy** holds the ack-storm
firebreak: a bot's own traffic only evaluates watches, and a `peers` map
distinguishes bots from humans. The **send path** is the single outbound
surface (`whinny post`). Every post runs the guard chain — persisted rate
bucket, breaker, quiet gate — and refusals come back as typed denials
(`refused-rate-limit`, `refused-breaker`, `refused-quiet`), never as prompt
machinery.

The breaker trips after five consecutive bad send outcomes, such as
rate-limit denials or failed posts, and stays latched until `whinny
breaker-clear` releases it. Usage errors, such as a misspelled room, are not
bad send outcomes: they refund the rate token and do not count toward the
trip.

whinny uses the [Apron](https://github.com/shazow/apron) protocol v9 over
WebSockets (`internal/apronws`) and keeps its state in a SQLite database
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
  `--transport apron|slack` (`$WHINNY_TRANSPORT`; slack needs
  `$SLACK_BOT_TOKEN` and `$SLACK_APP_TOKEN`), `--mode resident|shared`
  (`$WHINNY_DAEMON_MODE`), `--deliver
  exec|resume|socket|feed` (`$WHINNY_DAEMON_DELIVER`), `--exec-cmd <cmd>`
  (shell-split), `--socket-url <url>`, `--config <path>`
  (`$WHINNY_CONFIG`). SIGINT/SIGTERM shut it down cleanly with exit 0.
- `whinny schema`, `whinny manifest`, `whinny version` — tool discovery
  documents (see [Registry manifests](#registry-manifests)).

## Daemon

**Resident mode** is the full wake policy for one resident identity. It holds
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

**Transport** — `--transport apron` (default) runs over an apron websocket;
`--transport slack` runs over Slack Socket Mode. The Slack daemon resolves
its own user id via `auth.test` (so the config file may omit `self.user_id`),
and the ambient gate is forced off — Slack has no quiet/mute mapping. Slack
tokens come only from `$SLACK_BOT_TOKEN` (bot, `xoxb-`) and
`$SLACK_APP_TOKEN` (app-level, `xapp-`). The config file takes
`"transport": "slack"` instead of the flag. Live `message.mpim` events
wake, but MPIM catch-up after downtime does not: `conversations.list`
under a bot token returns no MPIMs, so MPIMs are missing from the room
list and history catch-up.

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
`WHINNY_DAEMON_DELIVER`, `WHINNY_TRANSPORT` (daemon transport, default
apron), `SLACK_BOT_TOKEN`/`SLACK_APP_TOKEN` (slack daemon tokens),
`WHINNY_RATE_PER_MINUTE` (send-path rate limit,
default 20).

## Known gaps (v1)

- The CLI's send path is apron-only: with `--transport slack` the daemon
  receives and delivers wakes, but `whinny post` and friends still talk to
  apron rooms (posting to Slack rooms is a follow-up).
- Slack "reply to my message" wakes only when the bot's post is the
  thread root: `ReplyTo` carries the `thread_ts` (thread root), so replies
  to the bot's non-root thread posts don't wake.
- Feed image fetch is same-origin restricted and capped.
- Delivery runs synchronously in the record loop: a slow deliverer can
  backpressure the connection.

## Registry manifests

`whinny manifest` prints this tool's
[cli-registry](https://github.com/sdougbrown/cli-contract) manifest (commands,
flags, output modes), and `whinny schema` prints the full schema document. A
host that embeds the manifest — poo's registry does — can drive whinny's
commands without hardcoding them. The room shows up to agents as plain
tools.

## Wiring an agent

The daemon delivers wakes; the agent acts on the room only through the CLI
tools. What changes per harness is how wakes arrive and which tool surface
the model sees.

### poo (feed mode)

1. Run the daemon in feed mode as a poo feed, and give the session the
   tool manifests:

   ```sh
   whinny daemon --deliver feed --config /etc/whinny/daemon.json
   ```

   with a feed manifest in the poo feed shape:

   ```json
   {
     "version": 1,
     "feeds": {
       "whinny": {
         "description": "Apron room wakes; act on them with the whinny tools.",
         "command": ["whinny", "daemon", "--deliver", "feed",
                     "--config", "/etc/whinny/daemon.json"],
         "env": {"APRON_TOKEN": "env:APRON_TOKEN"},
         "restart": true,
         "on_idle": "wake"
       }
     }
   }
   ```

   Wakes arrive as `feed:whinny` records (`{"text", "from"}`); `on_idle:
   "wake"` lets one start a run on an idle session. The feed command's
   environment is the scrubbed environment plus the manifest's `env`
   entries, so `env:APRON_TOKEN` keeps the token out of every session log.

2. Point the settings' `cli_registry.paths` (or `POO_CLI_REGISTRY_PATH`, or
   `--cli-registry-path`) at a file holding `whinny manifest` output, so the
   model gets `whinny-post`, `whinny-watch-add` and the rest as typed tools.

3. The profile declares `feeds: [whinny]` as a presence assertion; prompts
   should say replies are `whinny-post` calls and silence is the default —
   the room never sees unprompted output.

### pi-durable (resident, session-resume delivery)

1. Enroll a bot identity (below) and run the daemon:

   ```sh
   whinny daemon --deliver exec --exec-cmd /usr/local/bin/resume-whinny
   ```

   `resume-whinny` owns the session semantics: it resumes the durable
   session and feeds it the wake envelope on stdin (plain text, the shape
   `pi --resume <session>` expects).

2. The durable session keeps its conversational context across wakes; its
   prompt reads wakes as events to act on through the whinny tools.
3. Watches are conversation state: they do not survive a session rotation,
   so an agent relying on one re-arms it (`whinny watch add`) after the wake
   that mattered.

### Slack (resident, same recipes, different transport)

The poo/pi recipes above work unchanged with the daemon running over Slack:
set `--transport slack` and export the tokens instead of apron's. The Slack
app needs Socket Mode enabled with the `message.channels`, `message.groups`,
`message.im`, `message.mpim`, and `app_mention` events subscribed bot-side.
The daemon resolves its own user id at startup, so the config file can omit
`self.user_id`:

```sh
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
whinny daemon --transport slack --deliver feed --config /etc/whinny/daemon.json
```

Room ids in watches and cursors are Slack channel ids; the ambient gate is
forced off (no quiet mapping yet). "Reply to my message" wakes only when
the bot's post is the thread root (a reply's `thread_ts` is the thread
root, so replies to the bot's non-root thread posts don't wake). The CLI
send path stays apron-only in this phase — the Slack daemon receives and
delivers, but `whinny post` still targets apron rooms.

### One-shot and transient agents (shared mode)

One daemon in shared mode serves transient identities. Watches registered
with a delivery callback outlive the run that registered them. Shared mode
still validates like any daemon, so the config file must set `self` and a
usable delivery backend (a shared daemon only delivers to owner callbacks,
so `feed` costs nothing):

```sh
whinny daemon --mode shared --config /etc/whinny/shared.json
whinny watch add --room general --pattern "build done" \
  --owner ci-runner --on-fire /usr/local/bin/notify-ci-runner
```

with `/etc/whinny/shared.json` carrying `"mode": "shared"`, a non-empty
`"self": {"user_id": ..., "name": ...}`, and `"deliver": "feed"`. Shared
mode has no wake policy: only registered watches fire, each to its own
owner's callback.

### MCP-capable harnesses

`cli-registry-mcp -registry <dir>` (in the cli-registry repo) serves the
manifests in a directory as MCP tools over stdio (`-transport http -addr
...` for HTTP), so any MCP client gets the whinny tool family without
harness-specific code.

### Enrolling an identity

`aprond` signs identities in through the email ceremony: propose an email,
read the six-digit code from the server log, approve.
`e2e/aprontool` speaks those raw frames and `e2e/run.sh` is the worked
example; the live flow uses the same steps against the real server.

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
