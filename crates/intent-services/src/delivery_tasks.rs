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
);

#[derive(Default)]
struct State {
    closed: bool,
    tasks: Vec<DeliveryTask>,
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
    pub(crate) fn spawn_draining(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Option<JoinHandle<()>> {
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

    fn spawn_owned(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
        finish_on_shutdown: bool,
    ) -> Option<JoinHandle<()>> {
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
            future.await;
        });
        state.tasks.push(DeliveryTask {
            abort: handle.abort_handle(),
            finished: receiver,
            finish_on_shutdown,
        });
        Some(handle)
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
