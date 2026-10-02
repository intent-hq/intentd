//! Shutdown ownership for automatic agent deliveries. Durable watches, hooks,
//! and queued messages stay in the store for the next daemon.

use std::{future::Future, sync::Mutex};

use tokio::{
    sync::oneshot,
    task::{AbortHandle, JoinHandle},
};

#[derive(Default)]
pub(crate) struct DeliveryTasks(
    Mutex<State>,
    tokio::sync::Notify,
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    tokio::sync::Mutex<()>,
);

#[derive(Default)]
struct State {
    closed: bool,
    tasks: Vec<DeliveryTask>,
    draining: Vec<tokio::task::Id>,
}

struct DeliveryTask {
    abort: AbortHandle,
    finished: oneshot::Receiver<()>,
    finish_on_shutdown: bool,
}

impl DeliveryTasks {
    pub(crate) fn cancellation_flag(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.2.clone()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.lock().unwrap().closed
    }

    pub(crate) fn spawn(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Option<JoinHandle<()>> {
        self.spawn_owned(future, false)
    }

    /// A committed hook outcome must finish queueing its wake. Its scheduler
    /// observes closure between runs instead of being aborted mid-commit.
    pub(crate) fn spawn_draining<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Option<JoinHandle<T>> {
        self.spawn_owned(future, true)
    }

    pub(crate) async fn closed(&self) {
        let notified = self.1.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_closed() {
            notified.await;
        }
    }

    fn spawn_owned<T: Send + 'static>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
        finish_on_shutdown: bool,
    ) -> Option<JoinHandle<T>> {
        let mut state = self.0.lock().unwrap();
        if state.closed {
            return None;
        }
        state.tasks.retain(|task| !task.abort.is_finished());
        let (finished, receiver) = oneshot::channel();
        // Capture the sender BEFORE spawning: cancellation before the first poll
        // must also signal completion. Callers may still own/abort the JoinHandle.
        let handle = intent_core::spawn_daemon(async move {
            let _finished = finished;
            future.await
        });
        state.tasks.push(DeliveryTask {
            abort: handle.abort_handle(),
            finished: receiver,
            finish_on_shutdown,
        });
        Some(handle)
    }

    /// Drain finite service tails after all root admission and recurrence have
    /// stopped. Parents may still register children until the final empty check
    /// closes registration under the same lock. Never use this for schedulers
    /// or tasks that await `closed()`; automatic-delivery shutdown is separate.
    pub(crate) async fn drain_finite(&self) {
        let is_owned = {
            let state = self.0.lock().unwrap();
            tokio::task::try_id().is_some_and(|current| {
                state.tasks.iter().any(|task| task.abort.id() == current)
                    || state.draining.contains(&current)
            })
        };
        // Refuse self-join without poisoning the state needed by the real
        // external shutdown owner.
        assert!(
            !is_owned,
            "finite task drain must be called by an external owner"
        );
        // One external drainer owns all removed completion receivers. Another
        // drainer must wait for it, not mistake the temporarily empty list for
        // completion while the first owner is still joining parents.
        let _drainer = self.3.lock().await;
        loop {
            let tasks = {
                let mut state = self.0.lock().unwrap();
                assert!(state.draining.is_empty(), "finite drainer was cancelled");
                if state.tasks.is_empty() {
                    state.closed = true;
                    self.2.store(true, std::sync::atomic::Ordering::Relaxed);
                    self.1.notify_waiters();
                    return;
                }
                assert!(
                    state.tasks.iter().all(|task| task.finish_on_shutdown),
                    "finite drain cannot own cancellable schedulers"
                );
                let tasks = std::mem::take(&mut state.tasks);
                state.draining = tasks.iter().map(|task| task.abort.id()).collect();
                tasks
            };
            for task in tasks {
                let _ = task.finished.await;
            }
            self.0.lock().unwrap().draining.clear();
        }
    }

    /// Close synchronously so recovery can be persisted before any joins.
    pub(crate) fn close(&self) {
        let mut state = self.0.lock().unwrap();
        state.closed = true;
        self.2.store(true, std::sync::atomic::Ordering::Relaxed);
        self.1.notify_waiters();
        let current = tokio::task::try_id();
        for task in &state.tasks {
            if !task.finish_on_shutdown && Some(task.abort.id()) != current {
                task.abort.abort();
            }
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.close();
        let tasks = {
            let mut state = self.0.lock().unwrap();
            state.closed = true;
            self.1.notify_waiters();
            let current = tokio::task::try_id();
            let (callers, others): (Vec<_>, Vec<_>) = std::mem::take(&mut state.tasks)
                .into_iter()
                .partition(|task| Some(task.abort.id()) == current);
            // A callback may request shutdown itself. Its caller owns its tail;
            // retain it so a later external shutdown can still drain it.
            state.tasks = callers;
            others
        };
        for task in &tasks {
            if !task.finish_on_shutdown {
                task.abort.abort();
            }
        }
        for task in tasks {
            let _ = task.finished.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn finite_drain_joins_children_registered_by_admitted_parents() {
        let dir = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let tasks = std::sync::Arc::new(DeliveryTasks::default());
        let (release_parent, parent_wait) = oneshot::channel();
        let (child_admitted, child_wait) = oneshot::channel();
        let (release_child, child_release) = oneshot::channel();
        let owner = tasks.clone();
        let writer = store.clone();
        tasks
            .spawn_draining(async move {
                parent_wait.await.unwrap();
                owner
                    .spawn_draining(async move {
                        child_admitted.send(()).unwrap();
                        child_release.await.unwrap();
                        writer
                            .set_setting("test.nested", "persisted")
                            .await
                            .unwrap();
                    })
                    .unwrap();
            })
            .unwrap();
        let drain = tasks.drain_finite();
        tokio::pin!(drain);
        tokio::select! {
            biased;
            () = &mut drain => panic!("parent escaped the drain"),
            () = std::future::ready(()) => {}
        }
        release_parent.send(()).unwrap();
        child_wait.await.unwrap();
        let second = tasks.drain_finite();
        tokio::pin!(second);
        tokio::select! {
            biased;
            () = &mut drain => panic!("nested writer escaped the drain"),
            () = &mut second => panic!("concurrent drain returned early"),
            () = std::future::ready(()) => {}
        }
        release_child.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(drain, second);
        })
        .await
        .unwrap();
        assert_eq!(
            store.get_setting("test.nested").await.unwrap().as_deref(),
            Some("persisted")
        );
        assert!(tasks.spawn_draining(async {}).is_none());
        store.close().await;
    }

    #[tokio::test]
    async fn finite_drain_refuses_self_join() {
        let tasks = std::sync::Arc::new(DeliveryTasks::default());
        let inside = tasks.clone();
        let task = tasks
            .spawn_draining(async move {
                inside.drain_finite().await;
            })
            .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap();
        assert!(result.unwrap_err().is_panic());
        tasks.drain_finite().await;
        assert!(tasks.is_closed());
    }

    #[tokio::test]
    async fn shutdown_from_tracked_callback_does_not_join_itself() {
        let tasks = std::sync::Arc::new(DeliveryTasks::default());
        let inside = tasks.clone();
        let callback = tasks
            .spawn(async move {
                inside.shutdown().await;
            })
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), callback)
            .await
            .unwrap()
            .unwrap();
        tasks.shutdown().await;
        assert!(tasks.is_closed());
    }

    #[tokio::test]
    async fn shutdown_draining_callback_does_not_join_itself() {
        let tasks = std::sync::Arc::new(DeliveryTasks::default());
        let inside = tasks.clone();
        let callback = tasks
            .spawn_draining(async move {
                inside.shutdown().await;
            })
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), callback)
            .await
            .unwrap()
            .unwrap();
        tasks.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_notifies_unpolled_graceful_scheduler() {
        let tasks = std::sync::Arc::new(DeliveryTasks::default());
        let inside = tasks.clone();
        let scheduler = tasks
            .spawn_draining(async move {
                inside.closed().await;
            })
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), tasks.shutdown())
            .await
            .unwrap();
        scheduler.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_even_unpolled_tasks_and_refuses_late_registration() {
        let tasks = DeliveryTasks::default();
        let (held, released) = oneshot::channel::<()>();
        let handle = tasks
            .spawn(async move {
                let _held = held;
                std::future::pending::<()>().await;
            })
            .unwrap();
        tasks.shutdown().await;
        assert!(released.await.is_err());
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(tasks
            .spawn(async { panic!("closed delivery ran") })
            .is_none());
    }
}
