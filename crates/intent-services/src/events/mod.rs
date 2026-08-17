//! Event subscription surface (§10): the pure [`filter`] matching engine and
//! the in-process [`bus`] that appends to the store then broadcasts to
//! filtered, batched subscribers. Lives inside `intent-services` and talks only
//! to `intent-store` / `intent-core` (§3.2 rule 4); no transport coupling.

pub mod bus;
pub mod filter;
pub mod git_metadata_watcher;
pub mod git_status_refresher;
pub mod registry;
mod root_watch;
mod shared_watch;
pub mod skills_watcher;
pub mod specialists_watcher;
pub mod watcher;

pub use bus::{Delivery, EventBus, Subscription};
pub use filter::{
    event_matches, event_type_matches, is_agent_restricted_event_type, resolve_event_types,
    resolve_event_types_for_agent, SubscriptionFilter, AGENT_SUBSCRIBABLE_CATEGORY_WILDCARDS,
    DEFAULT_BATCH_WINDOW, VALID_EVENT_CATEGORY_WILDCARDS,
};
pub use git_status_refresher::GitStatusRefresher;
pub use registry::WatcherRegistry;

/// Serializes every test that starts a real filesystem watcher (the
/// `notify`-backed `RootWatch` / `FileWatcher` / `ConfigWatcher` probes across
/// `events/*` and `config_watcher`). Under full-suite parallel load, ~26
/// concurrent real watchers starve each other's OS-level startup and event
/// delivery, flaking the timing-sensitive tests (in isolation each passes).
/// Holding this guard for a test's whole duration keeps only one real watcher
/// live at a time. Mirrors the `CHILD_SPAWN_SERIAL` pattern in
/// `provider_models`. `unwrap_or_else(into_inner)` recovers from a poisoned
/// lock so one panicking test does not cascade.
#[cfg(test)]
pub(crate) static WATCHER_TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Pure-liveness deadline for the watcher tests' POSITIVE event-driven waits
/// (watch establishment, promotion, event delivery, catch-up flushes). Those
/// waits return as soon as the awaited condition holds, so this bound only
/// has to outlast a worst-case full-suite parallel `cargo test` stall — OS
/// watch registration plus fsevents delivery under load (monorepo#1630) —
/// never a passing run. Mirrors `script_ops`' `LIVENESS` (monorepo#515).
/// Negative assertions (things that must NOT happen) keep their own short
/// bounds; do not use this constant for them.
#[cfg(test)]
pub(crate) const LIVENESS: std::time::Duration = std::time::Duration::from_secs(300);

#[cfg(test)]
mod bus_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod watcher_tests;
