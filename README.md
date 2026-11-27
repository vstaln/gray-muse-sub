# gray-muse-sub

Gray connects to the local `muse serve` host over MSP using the official
Rust `muse-code-sdk`. Muse Code owns sign-in, model discovery, native
sessions, and workspace tools. Gray handles turn cancellation and
converts MSP text, reasoning, and usage into the provider response.

## What it does

- `provider/models` — live `model/list` from the running host, and no hardcoded fallback: if the host can't be reached, the list is reported unavailable.
- `provider/chat` — per-turn loopback relay (shared `gray_acp::relay`).
  The host POSTs an OpenAI Responses request; the sidecar drives one MSP
  `turn/start` → `item/delta`/`item/completed` → `turn/completed` cycle and
  returns Responses-format SSE. Settled turns persist a pool record
  (`~/.cache/muse-sub/resume/<sid>`); on the first turn after a restart,
  records inside a 6h warm window are reattached via `session/resume` so
  that turn continues the MSP session instead of replaying the transcript.
- `provider/auth/*` — `muse-cli` (oauth-kind) auth method: `muse` is a
  login-owning CLI, so "credentials" = a working `muse serve` handshake
  (`initialize` + `model/list` probe). `auth/start` runs `muse login`
  best-effort and polls the probe until the host serves sessions.
- `/muse [status]` — binary + handshake report.

## Install

Build from source with Rust 1.97 or newer (workspace: `~/grayplugins`):

```sh
cd ~/grayplugins/gray-muse-sub
cargo build --locked --release
gray plugin install "$PWD/target/release/muse-sub"
```

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
| `GRAY_MUSE_SUB_UNSANDBOXED` | unset (sandboxed) | `1` = `--disable-sandbox` + `allowAll` (pi-muse-msp's old default); `GRAY_MUSE_SUB_SANDBOXED=1` remains a no-op |
| `GRAY_MUSE_SUB_MAX_STEPS` | `30` | per-turn cap on billed model steps (`0` = unlimited); on the cap the turn is interrupted and ends with a notice |

## Behavior notes

- One `muse serve --trust-workspace` host per sidecar (`--disable-sandbox`
  only when `GRAY_MUSE_SUB_UNSANDBOXED=1`); spawned lazily, respawned if it
  dies, killed on `plugin/shutdown`.
- One MSP `session/start` per conversation; continuation is matched by
  verbatim absorbed-prefix + reply echo (same model as `gray_acp`).
- Spark-family models (`muse-spark*`, `spark`) send `providerId: "meta"`;
  the bare `muse-spark` id sends no `modelId` (server default).
- `reasoningEffort` maps `off`→`none`, `max`→`ultra`, rest verbatim.
- Approvals: a `denied`/`abort` choice is picked and the turn is
  interrupted (nothing is auto-approved); an approval with no such choice
  (or a `userInput/requested` prompt) also fails the turn.
- Usage: `session/tokenUsage` and terminal `turn/completed.usage`
  (`inputTokens`/`outputTokens`/cache fields folded into Responses usage).
- `model[effort=X]` bracket overrides are honored via `reasoningEffort`.

## Limits

- Muse's own tools run inside its session (bash/edit/etc.); gray host
  tools are not bridged into MSP — `function_call_output` tail items are
  forwarded as `## Tool result` transcript text, matching pi's deltaParts.
- Durable-log salvage (`~/.local/share/muse/sessions/*.jsonl`) and session
  fork/steer are not implemented — a dead projector fails the turn.
  Resume is only used to re-pool this sidecar's own sessions after a
  restart; a re-pooled session that dies on its first turn falls back to
  a fresh `session/start` (`fresh:rewarm_dead`).
- `muse login` command name is best-effort (unverified — the Pi extension
  never invokes login); if it doesn't exist, run the CLI's documented
  login manually — `auth/poll` settles on any working host.
- The release build was exercised through Gray against an isolated MSP
  host fixture: reasoning and text delivery, native session resume across
  a Gray restart, and interruption both before and after the turn-start
  acknowledgement. Live model access requires an authenticated Muse CLI.
