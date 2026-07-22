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
| `intentd` | Binary composition root + CLI (`serve`/`call`/`status`/`stop`/`doctor`/`import`/`mcp-bridge`). |
| `intent-acp` | ACP client core + `AgentManager` orchestration, agent→BE MCP callback server, and the loopback MCP bridge. |
| `intent-providers` | Provider registry + model resolution for spawning agent runtimes. |
| `intent-sourcecontrol` | GitHub/PR via `octocrab` (REST + GraphQL), token resolution, GHE support. |
| `intent-git` | Local git over `libgit2` (status/stage/commit/branches/merge-conflict + worktree/diff helpers). |
| `intent-context` | Ripgrep/symbol-backed context engine (auggie codebase-retrieval is **won't-port**). |
| `intent-pty` | Unified `portable-pty` host for interactive terminals **and** scripts. |
| `intent-search` | Gitignore-aware content + filename search over ripgrep/`ignore`/`globset`. |

## Install

> **Note:** the installers below download artifacts from public GitHub Release URLs, so
> they require this repository (and `intent-hq/homebrew-tap`) to be **public**. Both are
> currently **private**, so these commands will fail with 404/authentication errors until
> the repos are opened up. In the meantime, build from source (see
> [Quickstart](#quickstart)) or download release assets via the GitHub API with a token.

### Homebrew (macOS / Linux)

```sh
brew tap intent-hq/tap
brew install intentd
# or in one step:
brew install intent-hq/tap/intentd
```

The formula lives in [intent-hq/homebrew-tap](https://github.com/intent-hq/homebrew-tap)
and covers both macOS targets (Apple Silicon `aarch64-apple-darwin` and Intel
`x86_64-apple-darwin`) as well as Linux. Every `vX.Y.Z` release updates the formula
automatically, so **the tap tracks the latest release — i.e. the beta channel**;
promoting a release to stable (see [Releases & channels](#releases--channels)) does not
touch the tap.

### Shell installer (macOS / Linux)

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/intent-hq/intentd/releases/latest/download/intentd-installer.sh | sh
```

### PowerShell installer (Windows)

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/intent-hq/intentd/releases/latest/download/intentd-installer.ps1 | iex"
```

### Debian / Ubuntu (.deb)

Every release also ships Debian packages (`intentd_<version>_amd64.deb` and
`intentd_<version>_arm64.deb`, built by `.github/workflows/build-deb.yml`) installing
the binary at `/usr/bin/intentd` and a systemd **user** unit at
`/usr/lib/systemd/user/intentd.service`. Download the .deb for your architecture from
the [releases page](https://github.com/intent-hq/intentd/releases), then:

```sh
sudo apt install ./intentd_<version>_amd64.deb
# The package does not auto-enable the unit (it is per-user); start it at login with:
systemctl --user enable --now intentd
```

## Quickstart

```bash
# 1. Build the workspace
cargo build --workspace

# 2. Start the daemon (UDS) in one shell
cargo run -p intentd -- serve --listen uds
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
- **CLI:** `serve`, `call`, `status`, `stop`, `doctor`, `import`, and `mcp-bridge`. The daemon
  does not manage its own service unit: supervision is owned by the platform package manager —
  `brew services start intentd` on macOS, and the distro package (future .deb) on Linux.
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
(`dist-workspace.toml` + the generated `.github/workflows/release.yml`). Pushing a tag
publishes a GitHub Release with per-platform archives, `.sha256` checksums, and shell /
PowerShell installer scripts for: `aarch64-apple-darwin`, `x86_64-apple-darwin`,
`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-pc-windows-msvc`.
It also pushes an updated Homebrew formula to
[intent-hq/homebrew-tap](https://github.com/intent-hq/homebrew-tap) (the
`publish-homebrew-formula` job, authenticated via the `HOMEBREW_TAP_TOKEN` secret). The
tap always reflects the **latest release (beta channel)**; the stable promotion workflow
below only updates the stable channel manifest and never touches the tap.

Channels follow a **promotion model** — channel routing does not depend on prerelease
version suffixes (the release process cuts plain `vX.Y.Z` tags, no `-beta.N`):

- **Beta**: every `vX.Y.Z` tag (e.g. `v0.1.0`) lands on the beta channel automatically.
- **Stable**: a manual **promotion** of an existing release — run the
  [Promote stable](.github/workflows/promote-stable.yml) workflow (Actions → Promote
  stable) with the version to promote; it validates that the release exists and updates
  the stable channel manifest to point at it.

To cut a release: bump `version` in `crates/intentd/Cargo.toml` to the release version
via a normal PR (branch protection requires it); once that PR merges, push the tag
pointing at the merged `main` commit.

### Channel manifests

After each release, CI updates a machine-readable channel manifest on a fixed release:
every tag updates the `beta.json` asset on the `channel-beta` release
(`.github/workflows/publish-channel-manifest.yml`, run as a dist post-announce hook),
and promoting a version updates `stable.json` on `channel-stable`
(`.github/workflows/promote-stable.yml`). Consumers (e.g. cloudlands-fe pin-bump
automation, installer scripts) resolve "latest per channel" from these fixed URLs.
Schema (version 1):

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
      "url": "https://github.com/intent-hq/intentd/releases/download/v0.1.0/intentd-aarch64-apple-darwin.tar.xz",
      "sha256": "<hex digest of the archive>"
    }
  }
}
```

`platforms` is keyed by Rust target triple. While this repo is private, download the
manifest and artifacts via the GitHub API with a token (the `url` fields work unauthenticated
once the repo is public).

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
- `PROTOCOL.md` — the canonical wire contract (protocol v2.0): transport, JSON-RPC
  envelope, full method catalog, events, and error codes.

## Related Repositories

- [intent-hq/monorepo](https://github.com/intent-hq/monorepo) — engineering monorepo
  that mounts this repo at `packages/intentd` and holds the cross-cutting docs and tooling.

