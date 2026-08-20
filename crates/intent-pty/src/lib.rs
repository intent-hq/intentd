//! intent-pty — unified `portable-pty` host for terminals **and** scripts (§12.1).
//!
//! A single [`host::PtyHost`] owns every spawned process: interactive terminals
//! and scripts alike run as real PTYs in this host, so a terminal can attach to
//! a running script. The host provides:
//!
//! - **PTY spawn / resize / signal** over [`portable_pty`] (command, args, cwd,
//!   env, initial size); signal delivery (SIGINT/Ctrl-C) and resize.
//! - **Bounded server-side scrollback** ([`scrollback::Scrollback`], porting
//!   `script-output-buffer.ts`): output survives client disconnects and a
//!   late-attaching subscriber back-fills recent history before tailing live.
//! - **Multi-client output fan-out** (broadcast, porting
//!   `MainProcessTerminalManager.ts`): every attached subscriber receives
//!   identical output; stdin from any client is serialized into the single PTY
//!   master.
//! - **Session/workspace-scoped lifetime**: each PTY carries a scope key, and
//!   killing a scope kills its PTYs. Teardown reuses the M5 process-group
//!   reaping pattern (the PTY child is a `setsid` session leader, so
//!   `killpg(pgid, …)` reaches the whole tree — no orphaned grandchildren).
//!
//! Per §3.2 this crate depends only on `intent-core` (plus the external
//! `portable-pty` / `nix` / `tokio` utilities). It exposes a clean engine API
//! for the wire layers (T2 `terminal.*`, T3 `script.*`) to consume; it contains
//! no wire methods, script policy, or status model itself.

pub use intent_core::Result;

pub mod host;
pub mod scrollback;

pub use host::{Attachment, PtyExit, PtyHost, PtyId, PtySize, SpawnSpec};
pub use scrollback::Scrollback;
