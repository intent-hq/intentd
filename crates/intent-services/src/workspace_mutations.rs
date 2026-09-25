//! Workspace-scoped admission while incremental deletion yields the writer.
//!
//! Producers hold shared permits only for their service operation. Deletion
//! takes the exclusive permit BEFORE capturing sessions, so admitted creates
//! and sends finish before `stop_many`, and later producers fail without waiting.
//! Duplicate deletes serialize here. No registry mutex or database connection
//! is held while waiting for an operation or runtime teardown.

use intent_core::{Error, Result, WorkspaceId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

type Gates = Arc<Mutex<HashMap<WorkspaceId, Arc<RwLock<()>>>>>;

#[derive(Clone, Default)]
pub(crate) struct WorkspaceMutations(Gates);

struct Entry {
    gates: Gates,
    id: WorkspaceId,
    gate: Arc<RwLock<()>>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Only the map and this entry remain. Lookup shares this same mutex,
        // so nobody can race eviction and obtain a second gate for this id.
        if Arc::strong_count(&self.gate) == 2 {
            gates.remove(&self.id);
        }
    }
}

// Field order matters: release the permit before dropping its map entry.
pub(crate) struct Mutation {
    _permit: OwnedRwLockReadGuard<()>,
    _entry: Entry,
}

pub(crate) struct Deletion {
    _permit: OwnedRwLockWriteGuard<()>,
    _entry: Entry,
}

impl WorkspaceMutations {
    fn entry(&self, id: &WorkspaceId) -> Entry {
        let gate = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(id.clone())
            .or_default()
            .clone();
        Entry {
            gates: self.0.clone(),
            id: id.clone(),
            gate,
        }
    }

    pub(crate) fn enter(&self, id: &WorkspaceId) -> Result<Mutation> {
        let entry = self.entry(id);
        let permit = entry
            .gate
            .clone()
            .try_read_owned()
            .map_err(|_| Error::NotFound(format!("workspace {id} is being deleted")))?;
        Ok(Mutation {
            _permit: permit,
            _entry: entry,
        })
    }

    pub(crate) async fn delete(&self, id: &WorkspaceId) -> Deletion {
        let entry = self.entry(id);
        let permit = entry.gate.clone().write_owned().await;
        Deletion {
            _permit: permit,
            _entry: entry,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    #[tokio::test]
    async fn waiting_delete_closes_admission_and_cancel_releases_only_its_workspace() {
        let gates = WorkspaceMutations::default();
        let ws = WorkspaceId::from("deleting");
        let producer = gates.enter(&ws).unwrap();
        let mut deleting = Box::pin(gates.delete(&ws));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                deleting.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        assert!(gates.enter(&ws).is_err());
        let other = gates.enter(&WorkspaceId::from("unrelated")).unwrap();
        drop(deleting);
        drop(
            gates
                .enter(&ws)
                .expect("cancelled waiter releases admission"),
        );
        drop(producer);
        let delete = gates.delete(&ws).await;
        assert!(gates.enter(&ws).is_err());
        drop(delete);
        drop(
            gates
                .enter(&ws)
                .expect("completed delete releases admission"),
        );
        drop(other);
        assert!(
            gates.0.lock().unwrap().is_empty(),
            "no per-workspace gate leak"
        );
    }
}
