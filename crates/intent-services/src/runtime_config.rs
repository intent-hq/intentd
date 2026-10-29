//! Values and test seams core `Services` holds on behalf of the runtime
//! feature: the script and hook timing defaults, where script output logs
//! live, the `script.*` park seams and the hook-store fault injector. Moved
//! out of the runtime modules unchanged, so core state names no runtime type;
//! the runtime builds its own objects from these.

use std::path::PathBuf;
use std::sync::Arc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::Mutex;
use std::time::Duration;

#[cfg(any(test, feature = "test-hooks"))]
use intent_core::Error;

/// A run shorter than this is treated as a config error — do not auto-restart.
/// The production floor; tests can raise it via
/// `Services::with_script_too_fast_ms` so the decision is
/// load-independent (monorepo#514).
pub(crate) const TOO_FAST_MS: u128 = 2000;

/// Wall-clock budget for one hook script run (spec: any run exceeding 60 s is
/// killed and the hook evicted). Tests compress it via the `#[cfg(test)]`-only
/// `Services::with_hook_eval_timeout`.
pub(crate) const HOOK_EVAL_TIMEOUT: Duration = Duration::from_secs(60);

/// Base backoff between hook-run persistence retries (doubled per attempt;
/// see `hook_manager::HOOK_STORE_RETRY_ATTEMPTS`).
pub(crate) const HOOK_STORE_RETRY_BASE: Duration = Duration::from_secs(1);

/// Test-only fault injector for hook-run persistence steps: called with the
/// step name before each attempt; `Some(err)` fails that attempt in place of
/// the real store call (see `Services::with_hook_store_fault`).
#[cfg(any(test, feature = "test-hooks"))]
pub type HookStoreFault = Arc<Mutex<Box<dyn FnMut(&str) -> Option<Error> + Send>>>;

/// Test seam for a race window: lets a test hold a task inside a window
/// (signal `entered`, await `release`). Used for `supervise()`'s
/// pre-registration window (monorepo#1180).
#[derive(Default)]
pub struct SupervisePark {
    /// Signaled by the parked task on entering the window.
    pub entered: tokio::sync::Notify,
    /// Held by the parked task inside the window until the test releases it.
    pub release: tokio::sync::Notify,
}

/// The set of `script.*` test park seams (all `None` in production wiring),
/// grouped so the manager constructor stays within arity limits.
#[derive(Clone, Default)]
pub struct ScriptParks {
    /// Signals that a request passed the public gate and is waiting for admission.
    pub admission_wait: Option<Arc<tokio::sync::Notify>>,
    /// Parks a first-use list before its atomic bootstrap claim.
    pub bootstrap_persist: Option<Arc<SupervisePark>>,
    pub monitor_clock: Option<Arc<std::sync::atomic::AtomicI64>>,
    /// Parks before process admission; cancellation must prevent the spawn.
    pub before_spawn: Option<Arc<SupervisePark>>,
    /// Parks `supervise()` in its pre-registration window — after
    /// `pty.spawn`, before `mark_running` records the id (monorepo#1180).
    pub supervise: Option<Arc<SupervisePark>>,
    /// Parks `mark_running` after its eligibility check, before the
    /// `was_running` marker write and the in-memory flip to `running`
    /// (monorepo#4952).
    pub mark_running_persist: Option<Arc<SupervisePark>>,
    /// Parks archive after eligibility, while holding the admission lock.
    pub archive_persist: Option<Arc<SupervisePark>>,
    /// Parks after archive/restore commits, before the registry is updated.
    pub archive_committed: Option<Arc<SupervisePark>>,
    /// Parks after automatic settlement commits, before registry publication.
    pub settlement_committed: Option<Arc<SupervisePark>>,
    /// Parks settled process ownership before the outcome persistence decision.
    pub settlement_ready: Option<Arc<SupervisePark>>,
    /// Parks a terminal runtime transition before marker persistence/events.
    pub terminal_persist: Option<Arc<SupervisePark>>,
    /// Parks workspace deletion after its scripts stop, before the output
    /// folder is deleted.
    pub workspace_closed: Option<Arc<SupervisePark>>,
    /// Parks `script.create` after its retired/closing checks, before it
    /// persists the definition.
    pub create_admitted: Option<Arc<SupervisePark>>,
}

/// Where script output logs live: `<data_dir>/script-output`, each run log
/// capped at `max_bytes_per_run` bytes. The runtime builds its
/// `ScriptOutputStore` from this.
#[derive(Clone, Debug)]
pub struct ScriptOutputConfig {
    pub data_dir: PathBuf,
    pub max_bytes_per_run: u64,
}
