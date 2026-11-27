//! Writes that admit one more row under a cap (a quota).
//!
//! The count and the write run in one write transaction: `SQLite` has one
//! writer, so its transaction alone keeps another admission from counting
//! in between; Postgres first locks the quota's `quota_lock` row
//! (`postgres/quotas.rs`), so the processes of a tenant admit one at a time.

/// The outcome of a write admitted under a quota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaWrite {
    /// The write landed.
    Written,
    /// The write's own guard refused it (a unique key or a compare-and-swap
    /// lost to a concurrent write); nothing changed.
    Lost,
    /// The quota was full: `active` rows counted under the lock, at or above
    /// the cap. Nothing changed.
    Full {
        /// The rows the quota held when the write counted them.
        active: u64,
    },
}
