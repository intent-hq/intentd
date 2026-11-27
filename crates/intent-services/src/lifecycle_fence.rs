//! Lifecycle operations fenced by their leases.
//!
//! Agent retirement, workspace retirement, archive and workspace deletion
//! each hold a lifecycle lease on a store that other processes share
//! (`intent_store::lifecycle_leases`). Each runs the writes it makes under
//! that lease inside the hold's `guard_writes`, so once the lease expired by
//! the database clock or another process took the key, each later write
//! fails with `Error::LeaseLost` and changes nothing; the operation returns
//! that error, and a retry takes the lease again. Between steps that do not
//! write (stopping runtimes, publishing events) it calls the hold's `check`.
//!
//! Tests hold an operation at a named step ([`steps`]) with
//! [`park_lifecycle_step_for_test`] (test builds and `test-hooks` only), to
//! lose its lease there.

/// The steps a test can hold a lifecycle operation at.
pub mod steps {
    /// `workspace.delete`, under its deletion lease, right before the
    /// workspace row is deleted (key: workspace id).
    pub const DELETE_BEFORE_ROW_DELETE: &str = "workspace-delete:before-row-delete";
    /// An agent retire, under the agent's retirement lease (and the
    /// workspace-retirement lease when the last-top-level guard applies),
    /// right before the retired mark is written (key: agent id).
    pub const RETIRE_BEFORE_MARK: &str = "agent-retire:before-mark";
    /// An idempotent retire retry of a retired agent, under the hold that
    /// read its retired mark, right before it re-runs the resource release
    /// (key: agent id).
    pub const RETIRE_RETRY_CLEANUP: &str = "agent-retire:retry-cleanup";
    /// `workspace.archive`, under the archive-fence lease, right before the
    /// Archived flip commits (key: workspace id).
    pub const ARCHIVE_BEFORE_COMMIT: &str = "workspace-archive:before-commit";
}

/// Holds the operation at `step` of `key` while a test parks it there; a
/// no-op otherwise.
#[cfg_attr(
    not(any(test, feature = "test-hooks")),
    expect(clippy::unused_async, reason = "the park awaits in test builds")
)]
pub(crate) async fn park(step: &'static str, key: &str) {
    #[cfg(any(test, feature = "test-hooks"))]
    if let Some(park) = parks::take(step, key) {
        park.entered.notify_one();
        park.release.notified().await;
    }
    #[cfg(not(any(test, feature = "test-hooks")))]
    let _ = (step, key);
}

/// Test seam: the next run of lifecycle `step` ([`steps`]) for `key` signals
/// `entered` and waits for `release`. One-shot; parks are per process, so a
/// test uses keys of its own.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
#[must_use]
pub fn park_lifecycle_step_for_test(
    step: &'static str,
    key: &str,
) -> std::sync::Arc<LifecyclePark> {
    parks::add(step, key)
}

/// A parked lifecycle step: `entered` fires when the operation reaches
/// it, `release` lets it go on.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub type LifecyclePark = crate::runtime_config::SupervisePark;

#[cfg(any(test, feature = "test-hooks"))]
mod parks {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock, PoisonError};

    use crate::runtime_config::SupervisePark;

    type Parks = Mutex<HashMap<(&'static str, String), Arc<SupervisePark>>>;

    fn parks() -> &'static Parks {
        static PARKS: OnceLock<Parks> = OnceLock::new();
        PARKS.get_or_init(Parks::default)
    }

    pub(super) fn add(step: &'static str, key: &str) -> Arc<SupervisePark> {
        let park = Arc::new(SupervisePark::default());
        parks()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((step, key.to_owned()), Arc::clone(&park));
        park
    }

    pub(super) fn take(step: &'static str, key: &str) -> Option<Arc<SupervisePark>> {
        parks()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(step, key.to_owned()))
    }
}
