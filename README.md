# gray-muse-sub

Gray provider sidecar for **Muse Code** — relays chat turns to the local
`muse serve` MSP host (newline-delimited JSON-RPC 2.0 over stdio), the same
surface [`pi-muse-msp`](https://github.com/jwise7/pi-muse-msp) uses to drive
Muse from Pi. Muse models are Command Code's `muse-spark` family
(commandcode.ai); there is no public HTTP API, so the sidecar drives the
CLI's `muse serve` child exactly like the Pi extension does.

## What it does

- `provider/models` — live `model/list` from the running host, with the
  documented static catalog (`muse-spark` + `muse-spark-1.2/1.3`
  ± `-contributor`) as fallback when the host can't be reached.
- `provider/chat` — per-turn loopback relay (shared `acp_bridge::relay`).
  The host POSTs an OpenAI Responses request; the sidecar drives one MSP
  `turn/start` → `item/delta`/`item/completed` → `turn/completed` cycle and
  streams Responses SSE back.
- `provider/auth/*` — `muse-cli` (oauth-kind) auth method: `muse` is a
  login-owning CLI, so "credentials" = a working `muse serve` handshake
  (`initialize` + `model/list` probe). `auth/start` runs `muse login`
  best-effort and polls the probe until the host serves sessions.
- `/muse [status]` — binary + handshake report.

## Install

Build from source (workspace: `~/grayplugins`):

```sh
cd ~/grayplugins/gray-muse-sub
cargo build --release
gray plugin check ./target/release/muse-sub
```

Then install the binary as a gray plugin (same flow as `cursor-sub` /
`opencode-sub`).

## Setup

1. Install Muse Code from commandcode.ai so `muse` is on `PATH`
   (override with `GRAY_MUSE_SUB_COMMAND`).
2. Authenticate the CLI (`muse login`, or `cmd login` — same product
   family) or let `/muse` auth start the flow.
3. Pick a model (`muse-spark` = server default).

### Environment

| Var | Default | Meaning |
| --- | --- | --- |
| `GRAY_MUSE_SUB_COMMAND` | `muse` on PATH | Muse binary override (pi's `PI_MUSE_BINARY`) |
| `GRAY_MUSE_SUB_SANDBOXED` | unset (unsandboxed) | `1` = serve with workspace trust only (`approvalMode: onRequest`, auto-decided); default = `--disable-sandbox` + `allowAll`, same as pi-muse-msp |

## Behavior notes (pi-muse-msp parity)

- One `muse serve --trust-workspace [--disable-sandbox]` host per sidecar;
  spawned lazily, respawned if it dies, killed on `plugin/shutdown`.
- One MSP `session/start` per conversation; continuation is matched by
  verbatim absorbed-prefix + reply echo (same model as `acp_bridge`).
- Spark-family models (`muse-spark*`, `spark`) send `providerId: "meta"`;
  the bare `muse-spark` id sends no `modelId` (server default).
- `reasoningEffort` maps `off`→`none`, `max`→`ultra`, rest verbatim.
- Approvals: first `approved`/`approvedForSession` choice is auto-decided;
  no suitable choice (or a `userInput/requested` prompt) fails the turn —
  the headless path, same as `pi -p`.
- Usage: `session/tokenUsage` and terminal `turn/completed.usage`
  (`inputTokens`/`outputTokens`/cache fields folded into Responses usage).
- `model[effort=X]` bracket overrides are honored via `reasoningEffort`.

## Limits

- Muse's own tools run inside its session (bash/edit/etc.); gray host
  tools are not bridged into MSP — `function_call_output` tail items are
  forwarded as `## Tool result` transcript text, matching pi's deltaParts.
- Durable-log salvage (`~/.local/share/muse/sessions/*.jsonl`) and session
  resume/fork/steer are not implemented — a dead projector fails the turn.
- `muse login` command name is best-effort (unverified — the Pi extension
  never invokes login); if it doesn't exist, run the CLI's documented
  login manually — `auth/poll` settles on any working host.
- Live end-to-end needs a real `muse` install; this repo was verified
  against a mock MSP host (`cargo test`), which exercises the full relay:
  handshake → session/start → turn/start → deltas → terminal, approval
  auto-decide, userInput refusal, and cross-turn session reuse.
