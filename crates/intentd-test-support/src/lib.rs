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
//! - [`ProcessGroup`] (unix only): a watchdog-anchored process group for
//!   processes a test starts but does not parent (a script's `nohup ... &`
//!   daemon). Its teardown asks the members to stop, then SIGKILLs the rest,
//!   and the anchor kills the group once the test process dies, so nothing
//!   started into it outlives the test. [`is_running`] tells a live process
//!   from a gone or zombie one.
//! - [`Barrier`]: a release-file handshake between a test and a shell-script
//!   fake, so a held state stays stable for as long as the assertions take
//!   instead of racing a fixed `sleep`. A hold gives up on its own (exit
//!   [`Barrier::ABANDONED_EXIT_CODE`]) once the test's directory is gone or
//!   its deadline passes, so a fake can never outlive its test.
//! - [`PausePoint`]: hold an in-process operation at a chosen spot until the
//!   test releases it. Every wait is bounded by [`test_timeout`] even under
//!   plain `cargo test` (no nextest slow-timeout), and the test side reports
//!   the operation's error when it ends before the pause point — a
//!   hand-rolled `Gate` / `Pause` / `Park` once waited six hours on such an
//!   operation (`pause_point_lint`).
//! - [`assert_link_supports`]: a node link's agreed protocol carries a
//!   feature, as `agreed >= FEATURE_PROTOCOL`; an exact match breaks on the
//!   next protocol bump (`link_protocol_exact_lint`).
//! - [`test_timeout`]: the one place test budgets read
//!   `INTENTD_TEST_TIMEOUT_MULTIPLIER`, so hang guards stretch on slow
//!   coverage runners instead of flaking (`fixed_deadline_lint`).
//! - [`block_test_egress`] / [`apply_test_egress`]: route a test binary and
//!   the daemons it spawns through a dead local proxy, so a forgotten fake
//!   forge server fails at once instead of reaching GitHub or GitLab.
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
mod egress;
#[cfg(unix)]
mod guarded_child;
mod link_protocol;
mod pause_point;
#[cfg(unix)]
mod process_group;
pub mod source_lint;
mod timeout;

pub use barrier::{Barrier, DEFAULT_HOLD_DEADLINE};
pub use egress::{
    apply_test_egress, block_test_egress, test_egress_allowed, ALLOW_TEST_EGRESS_ENV,
    TEST_EGRESS_ENV, TEST_NPM_ENV,
};
#[cfg(unix)]
pub use guarded_child::GuardedChild;
pub use link_protocol::assert_link_supports;
pub use pause_point::{PausePoint, DEFAULT_HOLD_TIMEOUT, DEFAULT_REACH_TIMEOUT};
#[cfg(unix)]
pub use process_group::{is_running, ProcessGroup};
pub use timeout::{
    scale_test_timeout, test_timeout, test_timeout_multiplier, timeout_multiplier,
    MAX_TIMEOUT_MULTIPLIER, TIMEOUT_MULTIPLIER_ENV,
};
