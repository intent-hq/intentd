# intentd

`intentd` is a local-first, headless Rust daemon that owns the Intent domain model —
workspaces, notes, tasks, comments, agents, git, pull requests, scripts, terminals, files,
and events — and exposes it over a **JSON-RPC 2.0** API. Clients (a desktop UI, a CLI, or an
agent acting as an MCP client) are thin: all business logic lives in the daemon.

This repo is consumed as a git submodule by [intent-hq/monorepo](https://github.com/intent-hq/monorepo).

## What it is

- **Local-first.** The default transport is a **Unix-domain socket** (mode `0600`,
  newline-delimited JSON-RPC frames). A secure **WSS/TLS** LAN transport (bearer auth,
  origin allow-list, TLS fingerprint pinning) runs alongside it; there
  is no plaintext TCP product transport.
- **Single source of truth.** The daemon owns all durable state in **SQLite** (via `sqlx`
  with embedded migrations); clients hold only ephemeral UI state.
- **One service layer, many transports.** Transports are thin; the shared `WorkspaceApi`
  service surface is the single code path every listener — and the agent-facing MCP
  server — calls.

## Architecture

A single cargo **workspace** with 12 crates. Dependency direction is enforced per the
"Dependency-direction rules" in the monorepo's `docs/ARCHITECTURE.md`: `intent-core` is
the leaf, `intent-transport` depends only on `intent-services` (never on `intent-store`),
and the `intentd` binary is the only composition root that wires concrete
implementations together.

```text
                       ┌──────────────────────────────┐
                       │     intentd (bin)            │  CLI: serve / call / status / stop / doctor / …
                       │   composition root           │  wires store → services → transport
                       └───────────────┬──────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                               ▼
┌───────────────┐            ┌──────────────────┐            ┌──────────────────┐
│ intent-store  │  ◄───────  │ intent-services  │  ◄───────  │ intent-transport │
│ SQLite + sqlx │            │  WorkspaceApi    │            │ UDS listener +   │
│  migrations   │            │  implementation  │            │ JSON-RPC router  │
└───────┬───────┘            └────────┬─────────┘            └────────┬─────────┘
        │                             │                               │
        └─────────────────────────────┴───────────────┬───────────────┘
                                                       ▼
                                              ┌──────────────────┐
                                              │   intent-core    │  leaf: ids, errors→codes,
                                              │  domain + traits │  Config/paths, Workspace/Note,
                                              └──────────────────┘  WorkspaceApi trait
```

The diagram shows the core service path. The composition root also wires the ACP agent
runtime, source-control, git, PTY, and search engines into the service layer.

| Crate | Role |
| --- | --- |
| `intent-core` | Leaf domain vocabulary: ids, `Error`→JSON-RPC code mapping, `Config`/path resolution, domain model, the `WorkspaceApi` trait. |
| `intent-store` | SQLite persistence via `sqlx` + embedded migrations. |
| `intent-services` | `WorkspaceApi` implementation (the shared service surface) + the `AgentManager`, MCP callback server, and per-domain ops. |
| `intent-transport` | JSON-RPC router + UDS listener, the WSS/TLS listener, bearer auth + origin allow-list, and heartbeat. |
| `intentd` | Binary composition root + CLI (`serve`/`call`/`status`/`stop`/`doctor`/`settings`/`import`/`mcp-bridge`). |
| `intent-acp` | ACP client core + `AgentManager` orchestration, agent→BE MCP callback server, and the loopback MCP bridge. |
| `intent-providers` | Provider registry + model resolution for spawning agent runtimes. |
| `intent-sourcecontrol` | GitHub/PR via `octocrab` (REST + GraphQL), token resolution, GHE support. |
| `intent-git` | Local git over `libgit2` (status/stage/commit/branches/merge-conflict + worktree/diff helpers). |
| `intent-context` | Ripgrep/symbol-backed context engine (auggie codebase-retrieval is **won't-port**). |
| `intent-pty` | Unified `portable-pty` host for interactive terminals **and** scripts. |
| `intent-search` | Gitignore-aware content + filename search over ripgrep/`ignore`/`globset`. |

## Install

Installing intentd installs the **sitter** — a small self-updating supervisor shim,
packaged and named `intentd` (built from `crates/intentd-sitter`). On the first `serve`
it downloads the real daemon from the per-channel release manifests (**stable** by
default) — served from the public
[intent-hq/intentd-releases](https://github.com/intent-hq/intentd-releases) mirror
first, with a coded fallback to this repo (see
[Channel manifests](#channel-manifests)) — checks for updates at startup and then every
12–24 hours, forwards **all** CLI args to the daemon verbatim, and respawns the daemon
if it crashes. Update checks happen only for `intentd serve`; one-shot subcommands
(`doctor`, `status`, `stop`, `call`, …) run the already-installed daemon immediately
without checking for or installing updates. You never install the daemon binary
directly.

> **Note:** although this repository is private, the install commands below work
> unauthenticated: the daemon's channel manifests and platform archives **and** the
> sitter installer assets (archives, `.deb` packages, the `install.sh` / `install.ps1`
> scripts, and the archives the Homebrew formula downloads) are all mirrored to the
> public [intent-hq/intentd-releases](https://github.com/intent-hq/intentd-releases)
> repo — the permanent public distribution channel for release assets (manifests,
> download URLs, and the Homebrew formula keep pointing at it even after this repo
> goes public). The formula itself lives in the public `intent-hq/homebrew-tap`.

> **Installing for the Intent desktop app?** You don't need to: desktop releases in
> [intent-hq/cloudlands-releases](https://github.com/intent-hq/cloudlands-releases)
> bundle intentd as a sidecar and are self-contained. Install standalone intentd only
> for a **headless** machine (a remote Linux box, a spare Mac) that the desktop/mobile
> apps connect to remotely — see
> [Pairing a remote client](#pairing-a-remote-client-wss). The user-facing install
> guide lives on the
> [intent-hq/intentd-releases README](https://github.com/intent-hq/intentd-releases#readme);
> keep the two in sync when editing this section.

### Requirements

The daemon expects a few tools on the host it runs on:

- **git** — required. Workspace provisioning and daemon-side fetch (including the
  fetch step of pull) shell out to the `git` CLI (local status/stage/commit and
  push use bundled libgit2).
- **Node.js** (with `npm`/`npx`) — required to run the coding-agent provider CLIs:
  several providers are npm-installed or launched via pinned `npx` packages
  (auggie, claude-code, codex, …).
- **gh** (GitHub CLI) — optional. Enables the GitHub integration without a manual
  token: the daemon resolves its GitHub token as secrets store (the in-app GitHub
  connection) → `GITHUB_TOKEN`/`GH_TOKEN` env → `gh auth token`.

### One-line script (macOS / Linux)

```sh
curl -fsSL https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/install.sh | sh
```

Detects OS and architecture, downloads the matching `sitter-latest` archive, verifies
its `.sha256` sidecar, and installs `intentd` to `/usr/local/bin` when writable, else
`~/.local/bin` (override with `INTENTD_INSTALL_DIR`). Re-running updates in place.

After installing, the script offers to register intentd as a per-user service that
starts at login (systemd user unit on Linux, launchd LaunchAgent on macOS — the same
definitions the `.deb` and Homebrew installs use) and starts it immediately. The
prompt reads from the terminal, so it works with `curl … | sh`; non-interactive runs
skip service setup with a hint. Set `INTENTD_INSTALL_SERVICE=1` (or pass `--service`
on a direct run) to set it up without prompting, `INTENTD_INSTALL_SERVICE=0` /
`--no-service` to skip. On headless Linux boxes, user services only start at boot
once lingering is enabled (`sudo loginctl enable-linger $USER` — the script prints
this hint).

When a service is set up, the script also asks whether it should auto-resume
interrupted agents at startup (see [Auto-resume on start](#auto-resume-on-start);
answering `auto` — the default — writes nothing). `INTENTD_AUTO_RESUME=auto|on|off`
(or `--auto-resume=<value>` on a direct run) answers non-interactively.

```sh
intentd pair   # pairing info for remote clients (URL/token/fingerprint) — skip if only the local desktop app uses this daemon
```

To connect the desktop or iOS app from another machine, run `intentd pair` once the
daemon is running (the service starts it; if you declined service setup, start it
with `intentd serve` first) — see
[Pairing a remote client](#pairing-a-remote-client-wss).

### One-line script (Windows)

```powershell
powershell -c "irm https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/install.ps1 | iex"
```

Installs `intentd.exe` to `%LOCALAPPDATA%\intentd\bin` (override with
`INTENTD_INSTALL_DIR`) and adds that directory to the user `PATH`. After installing,
it offers to register a per-user Scheduled Task that runs `intentd serve`
at logon and starts it now; non-interactive runs skip with a hint. Set
`$env:INTENTD_INSTALL_SERVICE = '1'` (or `-Service` on a direct run) to set it up
without prompting, `'0'` / `-NoService` to skip. When the task is set up, the
installer also asks whether the service should auto-resume interrupted agents at
startup (see [Auto-resume on start](#auto-resume-on-start));
`$env:INTENTD_AUTO_RESUME = 'auto'|'on'|'off'` (or `-AutoResume auto|on|off` on a
direct run) answers non-interactively.

```powershell
intentd pair   # pairing info for remote clients (URL/token/fingerprint) — skip if only the local desktop app uses this daemon
```

To connect the desktop or iOS app from another machine, run `intentd pair` once the
daemon is running (the scheduled task starts it; if you declined task setup, start it
with `intentd serve` first) — see
[Pairing a remote client](#pairing-a-remote-client-wss).

### Homebrew (macOS / Linux)

```sh
brew install intent-hq/tap/intentd
# Run as a login service (launchd/systemd) — executes `intentd serve`:
brew services start intentd
# Pairing info for remote clients (URL/token/fingerprint) — skip if only the
# local desktop app uses this daemon; see "Pairing a remote client" below:
intentd pair
```

The formula lives in [intent-hq/homebrew-tap](https://github.com/intent-hq/homebrew-tap),
ships the sitter for both macOS targets (Apple Silicon `aarch64-apple-darwin` and Intel
`x86_64-apple-darwin`) as well as Linux, and is updated automatically by every
`sitter-vX.Y.Z` release. Which **daemon** version you run is decided by the sitter's
release channel (stable by default), not by the formula version.

### Debian / Ubuntu (.deb)

Sitter releases ship Debian packages (`intentd_<version>_amd64.deb` /
`intentd_<version>_arm64.deb`, with constant-named `intentd_amd64.deb` /
`intentd_arm64.deb` copies on the fixed `sitter-latest` release) installing the sitter at
`/usr/bin/intentd` and a systemd **user** unit at
`/usr/lib/systemd/user/intentd.service` (runs `intentd serve`):

```sh
curl -fLO https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/intentd_amd64.deb
sudo apt install ./intentd_amd64.deb
# The package does not auto-enable the unit (it is per-user); start it at login with:
systemctl --user enable --now intentd
# Pairing info for remote clients (URL/token/fingerprint) — skip if only the
# local desktop app uses this daemon; see "Pairing a remote client" below:
intentd pair
```

### Direct download

Download the archive for your platform from the fixed
[`sitter-latest`](https://github.com/intent-hq/intentd-releases/releases/tag/sitter-latest)
release on the public mirror — `intentd-<triple>.tar.xz` on macOS/Linux,
`intentd-<triple>.zip` on Windows, each with a `.sha256` sidecar — extract it, and put
`intentd` on your `PATH`:

```sh
curl -fLO https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/intentd-aarch64-apple-darwin.tar.xz
tar -xJf intentd-aarch64-apple-darwin.tar.xz
# → intentd-aarch64-apple-darwin/intentd
```

Once the daemon runs, `intentd pair` prints the pairing info remote clients need
(URL/token/fingerprint) — skip it if only the local desktop app uses this daemon;
see [Pairing a remote client](#pairing-a-remote-client-wss).

### Auto-resume on start

Whether the daemon resumes interrupted agents when it starts (as a service or via
`intentd serve`) is governed by the `agents.resumeInterruptedOnStart` setting:

- **`auto`** (default) — resume only on headless hosts (no display detected).
  Servers keep resuming on start; desktop hosts (macOS/Windows/Linux with a
  display) do **not** silently resume — the desktop app's resume prompt is the
  resume path there.
- **`on`** — always resume interrupted agents on start.
- **`off`** — never resume on start.

Note for Linux services: a systemd user unit (the `.deb` unit, the one
`install.sh` writes, and `brew services` on Linux) does not inherit the
graphical session's `DISPLAY`/`WAYLAND_DISPLAY`, so under `auto` the daemon
classifies the service as headless and resumes on start even on a desktop.
Set the setting to `off` to keep a Linux desktop service from resuming.

Change it with the settings CLI (applies at the next daemon start):

```sh
intentd settings agents.resumeInterruptedOnStart on|off|auto
```

`intentd serve --resume-all` force-enables the sweep for that single run,
regardless of the setting.

### Channels

The sitter follows the **stable** channel by default. To durably switch a machine to
beta (or back), use the sitter-owned `intentd sitter channel` command:

```sh
intentd sitter channel        # print the effective channel and its origin, e.g. "beta (from config)"
intentd sitter channel beta   # pin beta in <data-dir>/sitter/config.toml
intentd sitter channel beta --redownload && intentd restart   # switch and activate now
```

- **Setting a channel** writes the pin to `<data-dir>/sitter/config.toml`
  (user-editable; unlike `state.json`, never rewritten by the updater). Services
  started via `brew services` or the .deb's systemd user unit pass no channel flag or
  env, so they follow the pin — no formula or unit edits needed. A running service
  picks a new pin up at its next periodic update check; `intentd restart` applies it
  immediately.
- **`--redownload`** (set form only) additionally fetches the new channel's manifest
  right away and force-installs its version, **bypassing the newer-only comparison** —
  this is the explicit downgrade path for beta → stable. It never touches the running
  daemon; the new binary becomes active only after a restart. If the install fails,
  the command exits non-zero but the channel pin is still written.
- **`intentd restart`** restarts the supervised daemon in place — the sitter and the
  service manager stay put. It signals (SIGHUP) the serve-mode sitter found via
  `<data-dir>/sitter/sitter.pid`; the sitter gracefully stops the daemon and respawns
  it on the currently installed version and channel pin. Unix only — on Windows,
  restart the service instead. With no running supervised `serve`, it exits non-zero
  with guidance to start the service first.
- **`intentd update`** forces an update check on the effective channel right now,
  instead of waiting for the periodic serve-mode check. When a newer version is
  available it downloads and installs it (newer-only — never a downgrade), then
  restarts a running supervised daemon via the same SIGHUP path as `intentd restart`
  so the new version takes effect immediately (with no running service, the new
  binary simply takes effect on the next start; on Windows the install still
  happens — restart the service to activate it). `intentd update --check` is the
  dry-run form: it reports the installed and latest versions without downloading or
  installing anything. Exit 0 means the check succeeded, whether or not an update
  is available — parse stdout to tell the two apart.

Per-launch overrides still work and take precedence over the pin — pass
`--sitter-channel beta` or set `INTENTD_CHANNEL=beta`:

```sh
intentd --sitter-channel beta serve
```

Effective-channel precedence: `--sitter-channel` flag > `INTENTD_CHANNEL` env >
`sitter/config.toml` > stable default. A flag/env selection stays pinned for that
process's lifetime (its periodic checks do not re-read the config file).

`--sitter-*` flags and the intercepted `sitter` / `restart` / `update` commands belong
to the sitter and are never forwarded; everything else (e.g. `serve`, `--resume-all`,
`--version`) goes to the daemon verbatim. A leading `--` forwards even those
literally (`intentd -- restart` sends `restart` to the daemon).

### How updates work

- On `intentd serve` the sitter checks the channel manifest at startup and then on a
  randomized 12–24 h cadence; when a newer daemon version lands it downloads,
  sha256-verifies, and installs it atomically, then restarts the daemon on the new
  version. Unless the channel was pinned by flag/env at launch, each check re-reads
  the `config.toml` pin first, so `intentd sitter channel` takes effect on a running
  service without a restart.
- Manifest fetches try the public `intent-hq/intentd-releases` mirror first and fall
  back to this repo's release URLs if the mirror fetch fails (see
  [Channel manifests](#channel-manifests)). Archive URLs come from inside the chosen
  manifest, so downloads need no separate fallback.
- Automatic checks are strictly **newer-only** — they never downgrade. The only
  downgrade path is an explicit `intentd sitter channel <channel> --redownload`
  (see [Channels](#channels)).
- `intentd update` forces a check right away — no waiting on the 12–24 h cadence and
  no restart required; `intentd update --check` reports what would happen without
  installing (see [Channels](#channels)).
- One-shot subcommands (`doctor`, `status`, `stop`, `call`, …) never check for or
  install updates: they run the already-installed daemon directly. If no daemon is
  installed yet, they fail fast with guidance to start it first (`intentd serve` or
  `brew services start intentd`) so it gets installed.
- Sitter state lives under `<data-dir>/sitter/` (`versions/<version>/intentd`,
  `state.json`, `config.toml` — the channel pin, `sitter.pid` — the serve-mode
  sitter's pid while it runs, `tmp/`). The current and previous daemon versions are
  kept; older ones are pruned.
- If a `serve` update check fails (e.g. offline), the sitter falls back to the last
  installed daemon; only a first `serve` with nothing installed and no network exits
  with an error.

## Quickstart

```bash
# 1. Build the workspace
cargo build --workspace

# 2. Start the daemon (UDS) in one shell
cargo run -p intentd -- serve
#   intentd listening on UDS path=~/Library/Application Support/intentd/intentd.sock

# 3. In another shell, make a JSON-RPC call
cargo run -p intentd -- call workspace.list
#   { "workspaces": [] }
cargo run -p intentd -- call note.list --params '{"workspaceId":"ws-1"}'
#   { "notes": [] }

# 4. Probe liveness and run diagnostics
cargo run -p intentd -- status   # intentd: up / workspaces: N
cargo run -p intentd -- doctor   # data-dir writable + SQLite/migrations current
```

Paths are resolved via the `directories` crate and can be overridden with the
`INTENTD_DATA_DIR` and `INTENTD_CONFIG` environment variables. The data dir holds the SQLite
database (`intentd.db`) and the socket (`intentd.sock`).

### Settings

`intentd settings` reads and changes daemon settings on a running daemon — a friendlier
front for the `settings.list` / `settings.get` / `settings.update` RPCs:

```bash
intentd settings                                    # list every setting: path, type, current value
intentd settings agents.resumeInterruptedOnStart    # print one setting (value, type, default, description)
intentd settings agents.resumeInterruptedOnStart on # validate + apply a change
intentd settings linear.token                       # sensitive: prompt for the value with input hidden
op read op://vault/linear/token | intentd settings linear.token --stdin  # sensitive: read from stdin
```

Values are coerced to the setting's declared type — booleans take `true`/`false`,
numbers a numeric literal, enums one of their allowed strings, and object/array
settings a JSON document. Unknown names, invalid values (bad enum value, out-of-range
number, read-only setting), and a stopped daemon all fail with a clear message and a
non-zero exit; sensitive values are printed pre-redacted by the daemon.

> **Secrets never need to touch argv.** For a `sensitive` setting, omit the value
> to be prompted for it interactively with echo disabled (`read -s` style), or pipe
> it via `--stdin` (equivalently, pass `-` as the value) for scripted use — stdin
> input is read to EOF with exactly one trailing newline trimmed, and empty input
> is rejected (so a failed upstream producer never blanks a stored secret). On a
> non-TTY with no value, the command errors with guidance instead of hanging.
> Passing the plaintext as an argument still works but prints a warning, since
> argv lands in shell history and is visible in process listings (`ps`) while the
> command runs. `--stdin` / `-` are accepted for non-sensitive settings too.
> Because a bare `intentd settings <name>` prompts for sensitive settings instead
> of printing them, view a sensitive setting's redacted value/origin via the list
> (`intentd settings`) or `intentd call settings.get '{"path":"<name>"}'`.

### Pairing a remote client (WSS)

`intentd pair` prints everything a client needs to pair with this machine over the
WSS/TLS listener: the `intent://pair?…` QR code the Intent iOS app scans, followed by
labeled URL, bearer token, and TLS certificate fingerprint lines (each with a short
usage note — the token and fingerprint are what the desktop app's remote-connection
flow takes manually). The payload embeds the machine's LAN IP(s), the WSS port
(`server.wsApi.port`, default **5181**), the TLS certificate fingerprint (clients pin
it), and the bearer token — so the command is local-only (it queries `pairing.getInfo`
over the UDS socket).

```bash
intentd pair                  # QR code + labeled URL/token/fingerprint lines
intentd pair --png pair.png   # also export the QR code as an image (0600)
intentd pair --rotate         # mint a NEW bearer token (invalidates the old one)
```

`--rotate` rotates the token through the daemon (`server.rotateToken`), so live WSS
auth picks up the new token immediately. Rotation only happens once the listener is
confirmed up — declining the enable prompt (or an unattended run without `--yes`)
never invalidates existing clients' tokens. The daemon is the authority on whether
rotation is possible: when the daemon's token is fixed by its `INTENTD_AUTH_TOKEN`
env var it cannot be rotated (a note is printed to stderr).

If external connections (the WSS listener) are disabled, `pair` offers to enable them
on the spot: it persists `server.wsApi.enabled = true` to `config.toml` via the
daemon's `settings.update` pipeline (the same path the settings UI uses), which also
starts the listener immediately — no restart needed. Interactively this is a `[Y/n]`
prompt; unattended runs (non-TTY stdin) must pass `--yes`/`-y` to opt in, otherwise the
command fails with guidance.

## Current status

The backend port is well past a vertical slice: Milestones 1–10 are implemented, plus the
recent-repository registry, ACP session resume-on-respawn, and iOS-driven wire-parity fixes.
The initial porting effort concluded on 2026-07-13; its dated progress log is preserved
in the monorepo's git history.

**Implemented**

- **JSON-RPC surface (~143 request methods + 1 server-initiated `events.event` notification),
  served from SQLite,** across these namespaces:
  - Domain CRUD: `workspace.*` (9), `repo.*` (1, recent-repository registry), `note.*` (12),
    `task.*` (8), `comment.*` (5).
  - Events: `event.*` (7) + the `events.*` subscribe/unsubscribe fast-path and `events.event`
    push notification.
  - Source control & review: `git.*` (6), `pr.*` (12, active-PR gated), `file-tracking.*` (8),
    `metrics.*` (4), `accept-changes.*` (5).
  - Search, terminals & scripts: `search.*` (8), `terminal.*` (6), `script.*` (9).
  - Agent ecosystem: `settings.*` (4), `rules.*` (3), `specialist.*` (5), `mcp.servers.*` (7).
  - intentd transport extensions: `host.status`, `forward.*` (3), `client.hello`,
    `drafts.*` (3).
  - Plus the `agent.*` ACP runtime surface (24).
- **Transports:** UDS (default, mode `0600`) and a **WSS/TLS** LAN listener with bearer auth,
  origin allow-list, and TLS fingerprint pinning.
- **ACP agent runtime:** provider registry, ACP client, `AgentManager`, the agent→BE MCP
  callback server, live spawn-wiring, and the `mcp-bridge` stdio↔TCP proxy.
- **Source control:** GitHub/PR via `octocrab`, local git via `libgit2`, the change-tracking
  / accept-changes pipeline, and ripgrep-backed search.
- **Terminals & scripts:** a unified `intent-pty` host backing both interactive terminals and
  scripts (back-fill-then-tail scrollback, multi-client fan-out, process-group reaping).
- **CLI:** `serve`, `call`, `status`, `stop`, `doctor`, `token`, `pair` (auto-enables the
  WSS listener on demand), `import`, and `mcp-bridge`. The daemon does not manage its own
  service unit: supervision is owned by the platform package manager —
  `brew services start intentd` on macOS, and the .deb's systemd user unit on Linux.
- **Persistence:** SQLite via `sqlx` with embedded migrations through `0012_known_repo`
  (WAL, `foreign_keys`, `busy_timeout`).
- Standard JSON-RPC error codes: `-32700`, `-32600`, `-32601`, `-32602`, `-32603`.

**Won't port**

- auggie codebase-retrieval (no structured CLI; `search.codebase` stays ripgrep/symbol-backed).
- `mcp.servers` **http/sse** transports (stdio only).
- the `accept-changes.execute` **`export`** action and the legacy `memories.*` RPC.

**Planned**

- The Tauri/Svelte desktop frontend (not yet a submodule; will live in the monorepo).

## Releases & channels

Releases are tag-driven and built by [dist (cargo-dist)](https://axodotdev.github.io/cargo-dist/)
(`dist-workspace.toml` + the generated `.github/workflows/v-release.yml`). Pushing a tag
publishes a GitHub Release with per-platform archives and `.sha256` checksums for:
`aarch64-apple-darwin`, `x86_64-apple-darwin`, `x86_64-unknown-linux-musl`,
`aarch64-unknown-linux-musl`, `x86_64-pc-windows-msvc`. User-facing installers
(Homebrew formula, `.deb` packages) ship the sitter and are owned by the sitter
release pipeline (`.github/workflows/release-sitter.yml`); the daemon pipeline
publishes archives, checksums, and channel manifests only.

After each release, when the `INTENTD_RELEASES_TOKEN` secret is configured, the
pipeline also **mirrors** the platform archives and `.sha256` sidecars to an
identically-tagged release on the public
[intent-hq/intentd-releases](https://github.com/intent-hq/intentd-releases) repo
(`scripts/mirror-release-assets.sh`; without the secret the mirror steps are skipped
with a warning), so the daemon can be installed and auto-updated without access to
this private repo. Sitter releases are mirrored the same way: `release-sitter.yml`
copies the `sitter-vX.Y.Z` and refreshed `sitter-latest` assets (archives, `.deb`
packages, `.sha256` sidecars) to identically-tagged releases on intentd-releases,
which the Homebrew formula and the install URLs above point at. The mirror is a
temporary bridge until this repo is open-sourced.

Channels follow a **promotion model** — channel routing does not depend on prerelease
version suffixes (the release process cuts plain `vX.Y.Z` tags, no `-beta.N`):

- **Alpha**: every `vX.Y.Z` tag (e.g. `v0.1.0`) lands on the alpha channel automatically.
  The GitHub release is published as a **Pre-release** (also on the mirror).
- **Beta**: a manual **promotion** of an existing release — run the
  [Promote beta](.github/workflows/promote-beta.yml) workflow (Actions → Promote beta)
  with the version to promote; it validates that the release exists and is not a draft
  and updates the beta channel manifest to point at it. Beta promotion never touches
  the Pre-release/Latest flags — those move only on stable promotion.
- **Stable**: a manual **promotion** of an existing release — run the
  [Promote stable](.github/workflows/promote-stable.yml) workflow (Actions → Promote
  stable) with the version to promote; it validates that the release exists and is not a
  draft, updates the stable channel manifest to point at it, and clears the Pre-release
  flag while marking that release **Latest** on this repo and on the mirror. The Latest
  badge therefore always tracks the newest stable version.

### Cutting a release

Versioning is automated by [release-plz](https://release-plz.dev) in Release-PR mode
(`release-plz.toml` + `.github/workflows/release-plz.yml`):

1. On every push to `main`, release-plz computes the next semver from the conventional
   commits since the last tag (0.x rules: breaking → minor, `feat`/`fix` → patch) and
   opens or updates a **Release PR** that bumps `version` in `crates/intentd/Cargo.toml`
   and updates `CHANGELOG.md` (created on the first run).
2. **Merging the Release PR cuts the release**: release-plz pushes the `vX.Y.Z` tag,
   which triggers the cargo-dist `release.yml` pipeline above. Release timing stays
   human-controlled — merge the Release PR when you want to ship. Only Release-PR
   merges are tagged (`release_always = false`); a manual version bump in a regular
   PR is never tagged automatically.

Because PRs are squash-merged, only the PR title survives as the commit title on
`main`. **Breaking changes must be signaled with `!` in the PR title** (e.g.
`feat!: drop the legacy memories RPC`); a `BREAKING CHANGE:` footer buried in the
squashed body is not a reliable signal.

**One-time bootstrap**: release-plz replays `cargo package` against the latest tag to
diff released contents, so it requires that tag's manifests to carry version specs on
internal path dependencies. The existing `v0.2.0` tag predates those specs, so the
`Release-plz PR` job fails (expected red runs) until a release is cut manually once:
bump `version` in `crates/intentd/Cargo.toml` (plus `Cargo.lock`) in a regular PR,
merge it, and push the matching `vX.Y.Z` tag by hand. With `release_always = false`
the release job never tags that bootstrap bump itself — the manual tag push described
here is guaranteed to be the actual flow. Every subsequent release is fully automated.

The workflow authenticates with the `RELEASE_PLZ_TOKEN` repository secret — a PAT with
`contents` and `pull-requests` write access. It must be a PAT (not the default
`GITHUB_TOKEN`) because tags pushed with `GITHUB_TOKEN` do not trigger other workflows,
and the `vX.Y.Z` tag has to fire `release.yml`. Provisioning/rotating this secret is a
manual step (repo Settings → Secrets and variables → Actions).

### Channel manifests

After each release, CI updates a machine-readable channel manifest on a fixed release:
every tag updates the `alpha.json` asset on the `channel-alpha` release
(`.github/workflows/publish-channel-manifest.yml`, run as a dist post-announce hook),
promoting a version to beta updates `beta.json` on `channel-beta`
(`.github/workflows/promote-beta.yml`), and promoting to stable updates `stable.json`
on `channel-stable` (`.github/workflows/promote-stable.yml`). Each manifest is published **twice**: on the
public [intent-hq/intentd-releases](https://github.com/intent-hq/intentd-releases)
mirror with platform `url`s pointing at the mirrored assets there, and on this repo
with `url`s pointing at this repo's releases (byte-identical to the pre-mirror
manifests). Consumers (the sitter's update checks, cloudlands-fe pin-bump automation)
resolve "latest per channel" from these fixed URLs — the sitter fetches the
`intent-hq/intentd-releases` manifest first and falls back to this repo's copy if the
mirror fetch fails. Schema (version 1):

```json
{
  "schema": 1,
  "channel": "stable",
  "version": "0.1.0",
  "tag": "v0.1.0",
  "published_at": "2026-07-21T00:00:00Z",
  "platforms": {
    "aarch64-apple-darwin": {
      "asset": "intentd-aarch64-apple-darwin.tar.xz",
      "url": "https://github.com/intent-hq/intentd-releases/releases/download/v0.1.0/intentd-aarch64-apple-darwin.tar.xz",
      "sha256": "<hex digest of the archive>"
    }
  }
}
```

`platforms` is keyed by Rust target triple. The mirror repo is public, so the
manifests and archives there download unauthenticated; the copies on this repo need
the GitHub API with a token while the repo is private. The dual publish is temporary —
once this repo is open-sourced the mirror can be retired and the fallback copies serve
directly.

## Development

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo build --workspace
cargo test --workspace
```

**Conventional commits** are required (`feat`, `fix`, `chore`, `docs`, `refactor`, `test`,
`ci`, `perf`); PR titles are validated in CI and changelogs are generated with `git-cliff`.

## Documentation

The design docs live in the monorepo under `docs/`:

- `ARCHITECTURE.md` — system overview, crate layout, module responsibilities, and
  dependency-direction rules.
- `protocol/` — the canonical wire contract (current version in `protocol/README.md`),
  split into per-section files: transport, JSON-RPC envelope, full method catalog,
  events, and error codes.

## Related Repositories

- [intent-hq/monorepo](https://github.com/intent-hq/monorepo) — engineering monorepo
  that mounts this repo at `packages/intentd` and holds the cross-cutting docs and tooling.

