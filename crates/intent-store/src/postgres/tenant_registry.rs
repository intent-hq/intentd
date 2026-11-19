//! Process-local state that every open of one tenant shares.
//!
//! `SQLite` keeps note write lanes, task-event baselines, the displayed-tab
//! overlay and the desktop teardown gate on the store, and every clone of a
//! store shares them. Postgres adds the tenant writer slot (see the pool
//! contract in `postgres`). A Postgres tenant can be opened more than once in one
//! process ([`Store::open_postgres`](crate::Store::open_postgres) per
//! caller), so the state lives here instead, keyed like the repository
//! lifecycle domain (database plus tenant). Two opens of one tenant get the
//! same gates; other tenants and other databases do not.
//!
//! The registry holds weak references: the state lives while any store for
//! the tenant does, and the next open after the last one drops starts fresh.
//! A `NoteWriteLock` or a writer's slot permit can outlive every store; the
//! next open then gets a fresh lane and slot. Such a stale note lock cannot
//! write through the new store (`NoteWriteLock::ensure_covers` rejects it).
//!
//! The key is the server address and database as the pool reaches them, so
//! it is only as precise as the connect options:
//! - It excludes the user and `search_path`. Every tenant lives in one
//!   shared schema today; a store per schema would need them in the key.
//! - One database reached under two host names (`localhost` and
//!   `127.0.0.1`) gets two entries, and so two sets of gates. Give every
//!   pool for a database the same address.
//!
//! This only covers one process; gates across daemons need database-visible
//! state.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use super::{PgPool, TenantId, WriterSlot};
use crate::browser_tab_repo::DisplayedOverlay;
use crate::desktop_repo::DesktopWrites;
use crate::note_write_lock::NoteWriteLanes;
use crate::TaskEventBaselines;

/// The gates one tenant's stores share in this process.
#[derive(Default)]
pub(crate) struct TenantState {
    pub(crate) note_write_lanes: Arc<NoteWriteLanes>,
    pub(crate) task_event_baselines: Arc<TaskEventBaselines>,
    pub(crate) task_events_enabled: Arc<AtomicBool>,
    pub(crate) browser_tab_displayed: DisplayedOverlay,
    pub(crate) desktop_writes: DesktopWrites,
    pub(crate) writer_slot: WriterSlot,
}

static TENANTS: OnceLock<Mutex<HashMap<String, Weak<TenantState>>>> = OnceLock::new();

/// Names one tenant in one database: host, port, database name and socket,
/// then the tenant. The repository lifecycle domain uses the same key.
pub(crate) fn tenant_key(pool: &PgPool, tenant: &TenantId) -> String {
    let options = pool.connect_options();
    format!(
        "{}:{}/{:?}/{:?}#{}",
        options.get_host(),
        options.get_port(),
        options.get_database(),
        options.get_socket(),
        tenant.as_str()
    )
}

/// The state every live store for `key` shares, created on first use.
pub(crate) fn tenant_state(key: String) -> Arc<TenantState> {
    // Nothing panics while the lock is held, and the map stays consistent
    // even if something did, so a poisoned lock is safe to reuse.
    let mut tenants = TENANTS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    tenants.retain(|_, state| state.strong_count() > 0);
    if let Some(state) = tenants.get(&key).and_then(Weak::upgrade) {
        return state;
    }
    let state = Arc::new(TenantState::default());
    tenants.insert(key, Arc::downgrade(&state));
    state
}

#[cfg(feature = "test-support")]
impl crate::Store {
    /// Test-only: for each process-local gate, whether this store and
    /// `other` share it. `None` unless both are Postgres stores.
    #[must_use]
    pub fn postgres_shared_gates(&self, other: &crate::Store) -> Option<[(&'static str, bool); 6]> {
        let (crate::Backend::Postgres(a), crate::Backend::Postgres(b)) =
            (&self.backend, &other.backend)
        else {
            return None;
        };
        // Name every field, with no `..`: a new `PgStore` field does not
        // compile here until it is listed as per-open (`_`) or as a gate
        // below that must come from the tenant's `TenantState`.
        let super::PgStore {
            pool: _,
            tenant: _,
            options: _,
            note_write_lanes,
            task_event_baselines,
            task_events_enabled,
            browser_tab_displayed,
            desktop_writes,
            writer_slot,
            tenant_state,
        } = a;
        // A gate is shared when both stores hold the same one and it is the
        // one in this store's registry entry.
        Some([
            (
                "note_write_lanes",
                Arc::ptr_eq(note_write_lanes, &b.note_write_lanes)
                    && Arc::ptr_eq(note_write_lanes, &tenant_state.note_write_lanes),
            ),
            (
                "task_event_baselines",
                Arc::ptr_eq(task_event_baselines, &b.task_event_baselines)
                    && Arc::ptr_eq(task_event_baselines, &tenant_state.task_event_baselines),
            ),
            (
                "task_events_enabled",
                Arc::ptr_eq(task_events_enabled, &b.task_events_enabled)
                    && Arc::ptr_eq(task_events_enabled, &tenant_state.task_events_enabled),
            ),
            (
                "browser_tab_displayed",
                browser_tab_displayed.same_as(&b.browser_tab_displayed)
                    && browser_tab_displayed.same_as(&tenant_state.browser_tab_displayed),
            ),
            (
                "desktop_writes",
                desktop_writes.same_as(&b.desktop_writes)
                    && desktop_writes.same_as(&tenant_state.desktop_writes),
            ),
            (
                "writer_slot",
                writer_slot.same_as(&b.writer_slot)
                    && writer_slot.same_as(&tenant_state.writer_slot),
            ),
        ])
    }

    /// Test-only: how many live Postgres stores (opens and clones) hold this
    /// store's tenant state. The registry itself holds none. `None` on `SQLite`.
    #[must_use]
    pub fn postgres_tenant_state_holders(&self) -> Option<usize> {
        match &self.backend {
            crate::Backend::Postgres(store) => Some(Arc::strong_count(&store.tenant_state)),
            crate::Backend::Sqlite(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tenant_state;
    use std::sync::Arc;

    #[test]
    fn one_key_shares_state_until_its_last_holder_drops() {
        let key = format!("test-host:1/a#{}", uuid::Uuid::new_v4());
        let first = tenant_state(key.clone());
        let second = tenant_state(key.clone());
        assert!(Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(
            &first.note_write_lanes,
            &second.note_write_lanes
        ));

        let other = tenant_state(format!("{key}-other"));
        assert!(!Arc::ptr_eq(&first, &other));

        let weak = Arc::downgrade(&first);
        drop((first, second));
        assert!(
            weak.upgrade().is_none(),
            "the registry kept the state alive"
        );
        assert_eq!(Arc::strong_count(&tenant_state(key)), 1);
    }
}
