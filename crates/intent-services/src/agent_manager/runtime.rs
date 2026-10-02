//! Local execution resources behind the process-independent runtime contract.
//!
//! Session/model configuration is deliberately still a local-only seam. The
//! manager uses `RuntimeHandle::connection` for that setup; turn orchestration
//! uses the runtime interface and works without a connection or process.

use super::{
    kill_child_tree, kill_child_trees, mpsc, spawn_owned_cleanup, AgentHandle, Arc, AtomicOrdering,
    BoxFuture, Child, Connection, ContentBlock, Duration, IncomingNotification, JoinHandle,
    McpBridge, Mutex, NpxLaunchDir, PiExtensionDelivery, RetainUnlessSwept, TempConfigFile,
    TokioMutex,
};
use intent_acp::session::{ActivityTracker, PromptOutcome};
use intent_core::agent_runtime::AgentRuntime;

pub(crate) type Notifications = Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>;
pub(crate) type Runtime<'a> = dyn AgentRuntime<
        Prompt = Vec<ContentBlock>,
        PromptOutcome = PromptOutcome,
        Activity = ActivityTracker,
        Error = intent_acp::AcpError,
        Notifications = Notifications,
    > + 'a;

/// Keeps the concrete local setup capability separate from the execution
/// interface. A custom runtime has no local resource handle at all.
pub(super) struct RuntimeHandle {
    pub(super) runtime: Arc<Runtime<'static>>,
    pub(super) local: Option<Arc<LocalAgentRuntime>>,
}

impl RuntimeHandle {
    pub(super) fn local(resources: LocalResources) -> Self {
        let local = Arc::new(LocalAgentRuntime {
            resources: Mutex::new(resources),
            stopped: std::sync::atomic::AtomicBool::new(false),
        });
        Self {
            runtime: local.clone(),
            local: Some(local),
        }
    }

    #[cfg(test)]
    pub(super) fn custom(runtime: Arc<Runtime<'static>>) -> Self {
        Self {
            runtime,
            local: None,
        }
    }

    pub(super) fn connection(&self) -> Option<Arc<Connection>> {
        self.local.as_ref().map(|local| local.connection())
    }

    pub(super) fn child_exit(&self, expected_pid: Option<u32>) -> ChildState {
        let Some(local) = &self.local else {
            return ChildState::Absent;
        };
        let mut resources = local.resources.lock().unwrap();
        let Some(child) = resources.child.as_mut() else {
            return ChildState::Absent;
        };
        if child.id().is_some_and(|pid| Some(pid) != expected_pid) {
            return ChildState::Absent;
        }
        match child.try_wait() {
            Ok(Some(status)) => ChildState::Exited(status),
            Ok(None) | Err(_) => ChildState::Alive,
        }
    }
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        // Removing orchestration ownership must stop execution even if a
        // prompt still holds an Arc to the runtime. stop starts owned cleanup.
        drop(self.runtime.stop());
    }
}

pub(super) enum ChildState {
    Absent,
    Alive,
    Exited(std::process::ExitStatus),
}

/// The single owner of all local provider resources. Never exposed by the core
/// runtime interface; a remote implementation cannot contribute local PIDs.
pub(super) struct LocalAgentRuntime {
    stopped: std::sync::atomic::AtomicBool,
    pub(super) resources: Mutex<LocalResources>,
}

pub(super) struct LocalResources {
    pub(super) connection: Arc<Connection>,
    pub(super) notifications: Notifications,
    pub(super) serve_task: JoinHandle<()>,
    pub(super) child: Option<Child>,
    pub(super) child_pid: Option<u32>,
    pub(super) _mcp_bridge: Option<McpBridge>,
    pub(super) _mcp_config: Option<TempConfigFile>,
    pub(super) _rules_config: Option<TempConfigFile>,
    pub(super) _pi_extension: Option<PiExtensionDelivery>,
    pub(super) npx_launch_dir: Option<NpxLaunchDir>,
    pub(super) cleanup_lease: Option<tokio::sync::oneshot::Sender<()>>,
    #[cfg(test)]
    pub(super) cleanup_services: Option<crate::Services>,
}

impl LocalAgentRuntime {
    fn connection(&self) -> Arc<Connection> {
        self.resources.lock().unwrap().connection.clone()
    }

    fn take_child(&self) -> Option<DetachedChild> {
        DetachedChild::take_resources(&mut self.resources.lock().unwrap())
    }
}

impl AgentRuntime for LocalAgentRuntime {
    type Prompt = Vec<ContentBlock>;
    type PromptOutcome = PromptOutcome;
    type Activity = ActivityTracker;
    type Error = intent_acp::AcpError;
    type Notifications = Notifications;

    fn prompt<'a>(
        &'a self,
        session_id: &'a str,
        prompt: Self::Prompt,
        activity: &'a ActivityTracker,
    ) -> BoxFuture<'a, intent_acp::AcpResult<PromptOutcome>> {
        let connection = self.connection();
        Box::pin(async move {
            intent_acp::session::prompt(&connection, session_id, prompt, activity).await
        })
    }

    fn cancel<'a>(&'a self, session_id: &'a str) -> BoxFuture<'a, intent_acp::AcpResult<()>> {
        let connection = self.connection();
        Box::pin(async move { intent_acp::session::cancel(&connection, session_id).await })
    }

    fn notifications(&self) -> Notifications {
        self.resources.lock().unwrap().notifications.clone()
    }

    fn is_alive(&self) -> bool {
        if self.stopped.load(AtomicOrdering::SeqCst) {
            return false;
        }
        let mut resources = self.resources.lock().unwrap();
        if !resources.connection.is_alive() {
            return false;
        }
        resources
            .child
            .as_mut()
            .is_none_or(|child| !matches!(child.try_wait(), Ok(Some(_))))
    }

    fn client_request_seq(&self) -> u64 {
        self.connection().client_request_seq()
    }
    fn response_seq(&self) -> u64 {
        self.connection().response_seq()
    }
    fn await_response_after(&self, since: u64, timeout: Duration) -> BoxFuture<'_, bool> {
        let connection = self.connection();
        Box::pin(async move { connection.await_response_after(since, timeout).await })
    }

    fn spawned_pid(&self) -> Option<u32> {
        self.resources.lock().unwrap().child_pid
    }
    fn root_pid(&self) -> Option<u32> {
        let mut resources = self.resources.lock().unwrap();
        let pid = resources.child_pid?;
        if let Some(child) = resources.child.as_mut() {
            if !matches!(child.try_wait(), Ok(None)) {
                return None;
            }
        }
        Some(pid)
    }

    fn stop(&self) -> BoxFuture<'static, ()> {
        self.stopped.store(true, AtomicOrdering::SeqCst);
        self.resources.lock().unwrap().serve_task.abort();
        // Start cleanup before returning: even a never-polled/dropped future
        // cannot strand the child or release its launch directory too soon.
        let cleanup = self
            .take_child()
            .and_then(|mut child| child.start_cleanup());
        Box::pin(async move {
            if let Some(cleanup) = cleanup {
                let _ = cleanup.await;
            }
        })
    }
}

impl Drop for LocalAgentRuntime {
    fn drop(&mut self) {
        let resources = self.resources.get_mut().unwrap();
        resources.serve_task.abort();
        drop(DetachedChild::take_resources(resources));
    }
}

/// Opaque teardown carried by orchestration. Local batches retain the existing
/// shared process-tree sweep; custom runtimes supply their own owned cleanup.
pub(super) enum RuntimeTeardown {
    Local(Box<DetachedChild>),
    Custom(BoxFuture<'static, ()>),
}

impl RuntimeTeardown {
    pub(super) fn take(handle: &mut AgentHandle) -> Option<Self> {
        match &handle.execution.local {
            Some(local) => local.take_child().map(Box::new).map(Self::Local),
            None => Some(Self::Custom(handle.execution.runtime.stop())),
        }
    }

    pub(super) async fn kill_tree(self) {
        match self {
            Self::Local(child) => (*child).kill_tree().await,
            Self::Custom(cleanup) => cleanup.await,
        }
    }

    pub(super) async fn kill_trees(teardowns: Vec<Self>) {
        let mut children = Vec::new();
        let mut other = Vec::new();
        for teardown in teardowns {
            match teardown {
                Self::Local(child) => children.push(*child),
                Self::Custom(cleanup) => other.push(cleanup),
            }
        }
        // Every cleanup has already detached. Local children still use one
        // descendant snapshot and grace window, matching stop_many/shutdown.
        tokio::join!(DetachedChild::kill_trees(children), async {
            for cleanup in other {
                cleanup.await;
            }
        });
    }
}

/// Existing transcript tests exercise a connection directly. This borrowed
/// adapter keeps those tests on the same runtime-driven turn implementation;
/// it owns no process and is never installed in an `AgentManager`.
#[cfg(test)]
pub(crate) struct ConnectionRuntime<'a>(pub(crate) &'a Connection);

#[cfg(test)]
impl AgentRuntime for ConnectionRuntime<'_> {
    type Prompt = Vec<ContentBlock>;
    type PromptOutcome = PromptOutcome;
    type Activity = ActivityTracker;
    type Error = intent_acp::AcpError;
    type Notifications = Notifications;

    fn prompt<'a>(
        &'a self,
        session_id: &'a str,
        prompt: Self::Prompt,
        activity: &'a ActivityTracker,
    ) -> BoxFuture<'a, intent_acp::AcpResult<PromptOutcome>> {
        Box::pin(intent_acp::session::prompt(
            self.0, session_id, prompt, activity,
        ))
    }
    fn cancel<'a>(&'a self, session_id: &'a str) -> BoxFuture<'a, intent_acp::AcpResult<()>> {
        Box::pin(intent_acp::session::cancel(self.0, session_id))
    }
    fn notifications(&self) -> Notifications {
        let (_, rx) = mpsc::unbounded_channel();
        Arc::new(TokioMutex::new(rx))
    }
    fn is_alive(&self) -> bool {
        self.0.is_alive()
    }
    fn client_request_seq(&self) -> u64 {
        self.0.client_request_seq()
    }
    fn response_seq(&self) -> u64 {
        self.0.response_seq()
    }
    fn await_response_after(&self, since: u64, timeout: Duration) -> BoxFuture<'_, bool> {
        Box::pin(self.0.await_response_after(since, timeout))
    }
    fn spawned_pid(&self) -> Option<u32> {
        None
    }
    fn root_pid(&self) -> Option<u32> {
        None
    }
    fn stop(&self) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// A provider child handed out of its [`AgentHandle`] for a bounded
/// process-tree kill, together with its spawn-time pid (the process-group
/// id, still valid once the leader has been `try_wait`ed) and the npx launch
/// dir it runs in (intent-hq/intent#5738): that dir is the live tree's cwd,
/// so it must outlive [`kill_child_tree`] / [`kill_child_trees`] rather than
/// drop with the handle before the tree has been signalled.
///
/// Cleanup ownership is persistent, not the awaiting caller's: [`Self::kill_tree`]
/// and [`Self::kill_trees`] move the child, its pgid and the launch dir into
/// ONE owned task on the current runtime and await that task. A completion
/// lease acquired by the manager before child creation travels with these
/// resources through that task; final manager shutdown joins it even after
/// the handle has left the map. Cancelling the
/// caller — `stop()` aborting a worker inside `kill_child_only` after the
/// handle left the map, an RPC deadline dropping a `stop` / `stop_many`
/// future — leaves the task, and the descendant snapshot it already took,
/// running to completion (a second kill could not rediscover escaped
/// descendants once the leader is dead). [`Drop`] starts the same task for a
/// detached child that was never killed explicitly. When no runtime can run
/// the task, or it shuts down before the task finishes, the launch dir is
/// retained on disk rather than removed from under a possibly live tree, and
/// the child falls back to `kill_on_drop` — the same boundary as the
/// ephemeral adapter's `AdapterChild`.
pub(super) struct DetachedChild {
    /// `None` once moved into the owned cleanup task.
    pub(super) child: Option<Child>,
    pub(super) spawn_pid: Option<u32>,
    /// `None` once moved into the owned cleanup task.
    pub(super) npx_launch_dir: Option<NpxLaunchDir>,
    pub(super) cleanup_lease: Option<tokio::sync::oneshot::Sender<()>>,
    #[cfg(test)]
    pub(super) cleanup_services: Option<crate::Services>,
}

impl DetachedChild {
    /// Move the child (and its launch dir) out of `handle`; `None` when the
    /// handle owns no child.
    #[cfg(all(test, unix))]
    pub(super) fn take(handle: &mut AgentHandle) -> Option<Self> {
        handle.execution.local.as_ref()?.take_child()
    }

    fn take_resources(resources: &mut LocalResources) -> Option<Self> {
        let child = resources.child.take()?;
        Some(Self {
            child: Some(child),
            spawn_pid: resources.child_pid,
            npx_launch_dir: resources.npx_launch_dir.take(),
            cleanup_lease: resources.cleanup_lease.take(),
            #[cfg(test)]
            cleanup_services: resources.cleanup_services.take(),
        })
    }

    /// [`kill_child_tree`] on an owned task, releasing the launch dir only
    /// afterwards; awaits the task, but the task outlives a cancelled await.
    pub(super) async fn kill_tree(mut self) {
        if let Some(cleanup) = self.start_cleanup() {
            let _ = cleanup.await;
        }
    }

    /// Move the child and the launch dir into the owned cleanup task. `None`
    /// when there is nothing left to clean up or no runtime to run it on
    /// (then the dir is retained and the child left to `kill_on_drop`).
    pub(super) fn start_cleanup(&mut self) -> Option<JoinHandle<()>> {
        let child = self.child.take()?;
        let spawn_pid = self.spawn_pid;
        let lease = self.cleanup_lease.take();
        #[cfg(test)]
        let services = self.cleanup_services.take();
        let launch_dir = RetainUnlessSwept(self.npx_launch_dir.take());
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            drop(launch_dir);
            drop(child);
            return None;
        };
        Some(spawn_owned_cleanup(&runtime, async move {
            let _lease = lease;
            #[cfg(test)]
            if let Some(services) = services {
                services.hold_periodic_commit("physical-cleanup").await;
            }
            kill_child_tree(child, spawn_pid).await;
            launch_dir.remove();
        }))
    }

    /// [`kill_child_trees`] over the batch on ONE owned task, releasing every
    /// launch dir only after the shared sweep completes; a cancelled await
    /// leaves the batch sweep running.
    pub(super) async fn kill_trees(children: Vec<Self>) {
        let mut trees = Vec::with_capacity(children.len());
        let mut leases = Vec::with_capacity(children.len());
        let mut launch_dirs = Vec::with_capacity(children.len());
        for mut detached in children {
            leases.push(detached.cleanup_lease.take());
            if let Some(child) = detached.child.take() {
                trees.push((child, detached.spawn_pid));
            }
            launch_dirs.push(RetainUnlessSwept(detached.npx_launch_dir.take()));
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let _ = spawn_owned_cleanup(&runtime, async move {
            let _leases = leases;
            kill_child_trees(trees).await;
            for dir in launch_dirs {
                dir.remove();
            }
        })
        .await;
    }
}

impl Drop for DetachedChild {
    fn drop(&mut self) {
        drop(self.start_cleanup());
    }
}
