//! Workspace-scoped admission while incremental deletion yields the writer.
//!
//! Producers hold shared permits only for their service operation. Deletion
//! takes the exclusive permit BEFORE capturing sessions, so admitted creates
//! and sends finish before `stop_many`, and later producers fail without waiting.
//! Duplicate deletes serialize here. No registry mutex or database connection
//! is held while waiting for an operation or runtime teardown.

use intent_core::{Error, Result, WorkspaceId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
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
struct Permit {
    _permit: OwnedRwLockReadGuard<()>,
    entry: Entry,
}

#[derive(Clone)]
pub(crate) struct Mutation {
    _permit: Arc<Permit>,
}

tokio::task_local! {
    static ADMITTED: RefCell<Vec<Weak<Permit>>>;
}

/// Reuse live admission within one logical mutation, including nested service
/// calls. Scope is polled with the future, so siblings in `join!` do not share
/// admission and spawned tasks do not inherit it. Weak entries cannot prolong
/// admission after cancellation; existing background writes carry their own
/// explicit `Mutation` guards.
pub(crate) async fn scope<F: Future>(future: F) -> F::Output {
    let inherited = ADMITTED
        .try_with(|held| held.borrow().clone())
        .unwrap_or_default();
    ADMITTED.scope(RefCell::new(inherited), future).await
}

pub(crate) fn boxed<'a, T>(
    future: impl Future<Output = T> + Send + 'a,
) -> intent_core::BoxFuture<'a, T> {
    Box::pin(scope(future))
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
        if let Some(permit) = ADMITTED
            .try_with(|held| {
                held.borrow()
                    .iter()
                    .filter_map(Weak::upgrade)
                    .find(|permit| {
                        permit.entry.id == *id && Arc::ptr_eq(&permit.entry.gates, &self.0)
                    })
            })
            .ok()
            .flatten()
        {
            return Ok(Mutation { _permit: permit });
        }
        let entry = self.entry(id);
        let permit = entry
            .gate
            .clone()
            .try_read_owned()
            .map_err(|_| Error::NotFound(format!("workspace {id} is being deleted")))?;
        let permit = Arc::new(Permit {
            _permit: permit,
            entry,
        });
        let _ = ADMITTED.try_with(|held| {
            let mut held = held.borrow_mut();
            held.retain(|entry| entry.strong_count() > 0);
            held.push(Arc::downgrade(&permit));
        });
        Ok(Mutation { _permit: permit })
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
    async fn nested_admission_reuses_only_live_permits_in_its_logical_scope() {
        let gates = WorkspaceMutations::default();
        let ws = WorkspaceId::from("nested");
        let (entered, seen) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let operation = scope(async {
            let outer = gates.enter(&ws).unwrap();
            entered.send(()).unwrap();
            resumed.await.unwrap();
            scope(async {
                let nested = gates
                    .enter(&ws)
                    .expect("queued writer cannot revoke outer admission");
                drop(nested);
                // Equal workspace IDs in independent service instances cannot
                // inherit each other's permits.
                let other = WorkspaceMutations::default();
                let exclusive = other.delete(&ws).await;
                assert!(other.enter(&ws).is_err());
                drop(exclusive);
            })
            .await;
            let spawned_gates = gates.clone();
            let spawned_ws = ws.clone();
            assert!(
                tokio::spawn(async move { spawned_gates.enter(&spawned_ws).is_err() })
                    .await
                    .unwrap()
            );
            drop(outer);
            assert!(
                gates.enter(&ws).is_err(),
                "weak context must not retain a released permit"
            );
        });
        let competitor = async {
            seen.await.unwrap();
            let mut deletion = Box::pin(gates.delete(&ws));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(deletion.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert!(
                gates.enter(&ws).is_err(),
                "sibling join future must not inherit admission"
            );
            resume.send(()).unwrap();
            deletion.await
        };
        let ((), deleted) = tokio::join!(operation, competitor);
        drop(deleted);
        assert!(gates.0.lock().unwrap().is_empty());
        let mut cancelled = Box::pin(scope(async {
            let _held = gates.enter(&ws).unwrap();
            std::future::pending::<()>().await;
        }));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(cancelled.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        drop(cancelled);
        assert!(
            gates.0.lock().unwrap().is_empty(),
            "cancelled scope releases its live permit"
        );
    }

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
