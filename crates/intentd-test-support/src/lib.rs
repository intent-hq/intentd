//! Shared test support for the intentd / intentd-sitter integration suites.
//!
//! Workspace-internal and never published: pull it in as a `[dev-dependencies]`
//! entry only. It owns the two helpers every e2e suite that spawns a daemon,
//! sitter, or fake sidecar used to hand-roll privately:
//!
//! - [`GuardedChild`] (unix only): a [`std::process::Child`] spawned as the
//!   leader of its own process group and torn down — the whole group and
//!   then the child pid itself, with `SIGKILL` — if it is dropped while
//!   still running (the pid kill covers a child that moved to another group,
//!   whose empty group `killpg` can no longer reach). A test that panics
//!   halfway through would otherwise drop a plain `Child` (no kill on drop)
//!   and leave the process, plus anything it spawned, behind forever. Use it
//!   for every long-lived process a test owns; call [`GuardedChild::disarm`]
//!   only when something else takes over the teardown.
//! - [`Barrier`]: a release-file handshake between a test and a shell-script
//!   fake, so a held state stays stable for as long as the assertions take
//!   instead of racing a fixed `sleep`.
//!
//! # The reaped-pid caveat
//!
//! Once a child has been reaped (`wait` / `try_wait` returned a status) its
//! pid — and therefore its process-group id — may already have been handed
//! to an unrelated process. [`GuardedChild`]'s `Drop` therefore signals only
//! while `try_wait()` still reports the child as running; after the test has
//! reaped the child itself, dropping the guard is a no-op. Anything the child
//! spawned that outlived a reaped child is the test's own business.
//!
//! Detach a guarded child's stdio to `Stdio::null()` unless the test reads it:
//! an inherited capture pipe is what nextest's leak detector keys on
//! (intent-hq/intent#4284).

mod barrier;
#[cfg(unix)]
mod guarded_child;

pub use barrier::Barrier;
#[cfg(unix)]
pub use guarded_child::GuardedChild;
