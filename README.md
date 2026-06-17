# intentd

`intentd` is a local-first daemon that owns the Intent domain model — workspaces, notes,
tasks, comments, agents, git, pull requests, scripts, terminals, files, and events — and
exposes it over a JSON-RPC 2.0 API. Clients (a desktop UI, a CLI, or an agent acting as an
MCP client) are thin: all business logic lives in the daemon.

> Status: **bootstrap skeleton.** This is a compiling cargo workspace only — no daemon
> behavior, transport, or persistence is implemented yet. See the engineering spec in the
> monorepo (`docs/00_initial_porting/IMPLEMENTATION_SPEC.md`) for the full design.

## Layout

```text
intentd/                  # cargo workspace root
├── Cargo.toml            # [workspace] members + shared dependency versions
└── crates/
    └── intentd/          # binary: CLI + daemon entrypoint (clap)
        └── src/main.rs
```

Additional library crates (`intent-core`, `intent-store`, `intent-services`, `intent-acp`,
`intent-providers`, `intent-sourcecontrol`, `intent-git`, `intent-context`, `intent-pty`,
`intent-search`, `intent-transport`) are added in a later wave per the spec's §3 layout.

## Build & verify

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo build --workspace
```

## Run

```bash
cargo run -p intentd -- --help
cargo run -p intentd -- --version
```
