//! Event subscription surface (§10): the pure [`filter`] matching engine and
//! the in-process [`bus`] that appends to the store then broadcasts to
//! filtered, batched subscribers. Lives inside `intent-services` and talks only
//! to `intent-store` / `intent-core` (§3.2 rule 4); no transport coupling.

pub mod bus;
pub mod filter;
pub mod registry;
mod root_watch;
pub mod skills_watcher;
pub mod specialists_watcher;
pub mod watcher;

pub use bus::{EventBus, Subscription};
pub use filter::{
    event_matches, event_type_matches, is_agent_restricted_event_type, resolve_event_types,
    resolve_event_types_for_agent, SubscriptionFilter, AGENT_SUBSCRIBABLE_CATEGORY_WILDCARDS,
    DEFAULT_BATCH_WINDOW, VALID_EVENT_CATEGORY_WILDCARDS,
};
pub use registry::WatcherRegistry;
pub use skills_watcher::SkillsWatcher;
pub use specialists_watcher::SpecialistsWatcher;
pub use watcher::FileWatcher;

#[cfg(test)]
mod bus_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod watcher_tests;
