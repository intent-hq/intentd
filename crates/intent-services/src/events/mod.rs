//! Event subscription surface (§10): the pure [`filter`] matching engine and
//! the in-process [`bus`] that appends to the store then broadcasts to
//! filtered, batched subscribers. Lives inside `intent-services` and talks only
//! to `intent-store` / `intent-core` (§3.2 rule 4); no transport coupling.

pub mod bus;
pub mod filter;
pub mod watcher;

pub use bus::{EventBus, Subscription};
pub use filter::{
    event_matches, event_type_matches, resolve_event_types, SubscriptionFilter,
    DEFAULT_BATCH_WINDOW, VALID_EVENT_CATEGORY_WILDCARDS,
};
pub use watcher::FileWatcher;

#[cfg(test)]
mod bus_tests;

#[cfg(test)]
mod watcher_tests;
