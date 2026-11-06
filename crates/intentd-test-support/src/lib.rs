//! Shared test support for the intentd / intentd-sitter integration suites.
//!
//! Workspace-internal and never published: pull it in as a `[dev-dependencies]`
//! entry only. It owns the scaffolding the source-scanning lints share
//! ([`source_lint`]: lexer, `#[cfg(test)]` blanking, statement splitting,
//! opt-out markers, workspace walkers) and the two helpers every e2e suite
//! that spawns a daemon, sitter, or fake sidecar used to hand-roll privately:
//!
//! - [`GuardedChild`] (unix only): a [`std::process::Child`] spawned as the
//!   leader of its own process group and torn down — the whole group and
//!   then the child pid itself, with `SIGKILL` — if it is dropped while
//!   still running or exited but unreaped (the pid kill covers a child that
//!   moved to another group, whose empty group `killpg` can no longer
//!   reach). A test that panics
//!   halfway through would otherwise drop a plain `Child` (no kill on drop)
//!   and leave the process, plus anything it spawned, behind forever. Use it
//!   for every long-lived process a test owns; call [`GuardedChild::disarm`]
//!   only when something else takes over the teardown.
//! - [`Barrier`]: a release-file handshake between a test and a shell-script
//!   fake, so a held state stays stable for as long as the assertions take
//!   instead of racing a fixed `sleep`. A hold gives up on its own (exit
//!   [`Barrier::ABANDONED_EXIT_CODE`]) once the test's directory is gone or
//!   its deadline passes, so a fake can never outlive its test.
//! - [`test_timeout`]: the one place test budgets read
//!   `INTENTD_TEST_TIMEOUT_MULTIPLIER`, so hang guards stretch on slow
//!   coverage runners instead of flaking (`fixed_deadline_lint`).
//!
//! # The reaped-pid caveat
//!
//! Once a child has been reaped (`wait` / `try_wait` returned a status) its
//! pid — and therefore its process-group id — may already have been handed
//! to an unrelated process. [`GuardedChild`]'s `Drop` and
//! [`GuardedChild::kill_tree`] therefore signal only while the kernel still
//! owns the child — running, or exited but not yet reaped, checked with a
//! non-reaping `waitid(WNOWAIT)` peek; after the test has reaped the child
//! itself, dropping the guard is a no-op. Anything the child spawned that
//! outlived a child the test reaped is the test's own business. (The one
//! residual: a test that reaps the child through a raw `&mut Child` and then,
//! in the same process, spawns another child that wraps around onto the same
//! pid would have that child's group swept instead — pids wrap only after
//! `pid_max` allocations.)
//!
//! Detach a guarded child's stdio to `Stdio::null()` unless the test reads it:
//! an inherited capture pipe is what nextest's leak detector keys on
//! (intent-hq/intent#4284).

mod barrier;
#[cfg(unix)]
mod guarded_child;
pub mod source_lint;
mod timeout;

pub use barrier::{Barrier, DEFAULT_HOLD_DEADLINE};
#[cfg(unix)]
pub use guarded_child::GuardedChild;
pub use timeout::{
    scale_test_timeout, test_timeout, test_timeout_multiplier, timeout_multiplier,
    MAX_TIMEOUT_MULTIPLIER, TIMEOUT_MULTIPLIER_ENV,
};
