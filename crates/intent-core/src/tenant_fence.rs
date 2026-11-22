//! Self-fencing: a process stops serving a tenant the moment it may no
//! longer own it.
//!
//! On Postgres an orchestrator owns a tenant through a fenced lease
//! (`intent_store::postgres::lease`). Each claim of the lease gets its own
//! [`TenantFence`]. It fires, once and for good, when:
//!
//! - the lease heartbeat cannot renew by the lease's expiry minus a margin,
//!   is told the lease was taken over (`NotOwner`), or is dropped;
//! - any write or renewal of the tenant in this process meets `NotOwner`
//!   (another process claimed the tenant);
//! - a later claim of the tenant by this process replaces the claim;
//! - the caller fires it ([`TenantFence::fire`]).
//!
//! Once it fired, every write of the tenant in this process fails
//! `Error::NotOwner` before it starts, and so do renewals.
//! [`TenantFence::settled`] waits until no write that passed the fence is
//! still open, so nothing the process does afterwards lands a row for the
//! tenant (the event bus writer included: it writes through the same path).
//!
//! [`SelfFence`] owns what serves one tenant and stops it when the fence
//! fires, in this order: the tenant's client connections, its machine
//! links, its background tasks (`startup_work` and orchestration), its MCP
//! servers, then its `Services` (dropped); then it waits for
//! [`TenantFence::settled`].
//!
//! `SQLite` has no lease and no fence: one daemon owns a database file.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{watch, Semaphore};
use tokio::task::JoinHandle;

/// This process's stop signal for one tenant claim; cheap to clone, every
/// clone is the same fence. See the module docs.
#[derive(Clone)]
pub struct TenantFence(Arc<Inner>);

struct Inner {
    /// `None` until fired; then why.
    reason: watch::Sender<Option<Arc<str>>>,
    /// The tenant's writer slot (one permit): every write holds it from
    /// before its fence check until its transaction ends.
    writer: Option<Arc<Semaphore>>,
}

impl Default for TenantFence {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantFence {
    /// A fence with no writes behind it: [`Self::settled`] waits only for
    /// the fence to fire.
    #[must_use]
    pub fn new() -> Self {
        Self::build(None)
    }

    /// A fence over the writes that each hold one permit of `writer` (a
    /// one-permit semaphore, the tenant's writer slot) from before they
    /// check the fence until their transaction ends. The store makes these.
    #[must_use]
    pub fn guarding_writes(writer: Arc<Semaphore>) -> Self {
        Self::build(Some(writer))
    }

    fn build(writer: Option<Arc<Semaphore>>) -> Self {
        let (reason, _) = watch::channel(None);
        Self(Arc::new(Inner { reason, writer }))
    }

    /// Fire the fence: this process stops serving the tenant. Only the first
    /// call counts (its `reason` is kept); returns whether this call fired.
    pub fn fire(&self, reason: impl Into<String>) -> bool {
        let reason: Arc<str> = reason.into().into();
        let fired = self.0.reason.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(reason.clone());
            true
        });
        if fired {
            tracing::warn!(%reason, "tenant fence fired: this process stops serving the tenant");
        }
        fired
    }

    /// Whether the fence fired.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.0.reason.borrow().is_some()
    }

    /// Why the fence fired, once it did.
    #[must_use]
    pub fn reason(&self) -> Option<String> {
        self.0.reason.borrow().as_deref().map(str::to_owned)
    }

    /// Wait until the fence fires. Cancel-safe.
    pub async fn fenced(&self) {
        let mut rx = self.0.reason.subscribe();
        // The sender lives in `self`, so the wait cannot end with an error.
        let _ = rx.wait_for(Option::is_some).await;
    }

    /// Wait until the fence fired and every write that passed the fence
    /// before it fired has ended (committed or rolled back). After this no
    /// row of the tenant can land from this process: a later write fails
    /// before it starts, and a write that queued for the writer slot checks
    /// the fence once it holds it. Bounded by the writer's statement and
    /// idle-in-transaction timeouts. Cancel-safe.
    pub async fn settled(&self) {
        self.fenced().await;
        if let Some(writer) = &self.0.writer {
            // Writers queue fairly on the slot: once this permit is granted,
            // every writer that held or queued for it before has finished.
            // The slot is never closed, so the error arm cannot happen.
            let _ = writer.acquire().await;
        }
    }

    /// Whether both handles are the same fence.
    #[must_use]
    pub fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::fmt::Debug for TenantFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantFence")
            .field("reason", &self.reason())
            .finish_non_exhaustive()
    }
}

impl PartialEq for TenantFence {
    fn eq(&self, other: &Self) -> bool {
        self.same_as(other)
    }
}

impl Eq for TenantFence {}

/// One stop of [`SelfFence`], in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FenceStep {
    /// The tenant's client connections are closed.
    ClientConnections,
    /// The tenant's machine links are closed.
    MachineLinks,
    /// The tenant's `startup_work` and orchestration tasks are cancelled
    /// and joined.
    Tasks,
    /// The tenant's MCP servers are stopped.
    McpServers,
    /// The runner's handles on the tenant's `Services` (and anything else
    /// owned) are dropped.
    Services,
    /// No write that passed the fence is still open
    /// ([`TenantFence::settled`]).
    WritesSettled,
}

/// What [`SelfFence`] did, for logs and tests.
#[derive(Debug, Clone)]
pub struct SelfFenceReport {
    /// Why the fence fired.
    pub reason: String,
    /// When the runner saw the fence fire.
    pub fenced_at: Instant,
    /// Each step and when it finished, in order.
    pub steps: Vec<(FenceStep, Instant)>,
    /// Steps that ran past [`SelfFence::with_step_budget`]: the runner moved
    /// on, leaving them to finish (or not) in the background. Their work
    /// may still run after the lease expires, so the composition must treat
    /// a non-empty list as fatal for the tenant (stop the process when it
    /// cannot stop the work).
    pub overran: Vec<FenceStep>,
}

/// A step's stop: an async close.
pub type StopFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type Stop = Box<dyn FnOnce() -> StopFuture + Send>;

/// The default time each [`SelfFence`] step may take before the runner
/// moves on. The budgeted steps together must fit inside the lease's fence
/// margin (`intent_store::postgres::LEASE_FENCE_MARGIN`, 5 s); a store test
/// checks it.
pub const SELF_FENCE_STEP_BUDGET: Duration = Duration::from_secs(1);

/// How many [`SelfFence`] steps are budgeted (all but dropping and
/// settling).
pub const SELF_FENCE_BUDGETED_STEPS: u32 = 4;

/// Owns what serves one tenant in this process and stops it when the
/// tenant's fence fires (see the module docs for the order). Build it with
/// the tenant's composition, then [`Self::spawn`] it.
pub struct SelfFence {
    fence: TenantFence,
    connections: Vec<Stop>,
    links: Vec<Stop>,
    tasks: Vec<JoinHandle<()>>,
    task_stops: Vec<Stop>,
    mcp: Vec<Stop>,
    owned: Vec<Box<dyn Any + Send>>,
    step_budget: Duration,
}

impl SelfFence {
    /// A runner for the tenant behind `fence`, owning nothing yet.
    #[must_use]
    pub fn new(fence: TenantFence) -> Self {
        Self {
            fence,
            connections: Vec::new(),
            links: Vec::new(),
            tasks: Vec::new(),
            task_stops: Vec::new(),
            mcp: Vec::new(),
            owned: Vec::new(),
            step_budget: SELF_FENCE_STEP_BUDGET,
        }
    }

    /// How long each step may take before the runner moves on.
    #[must_use]
    pub fn with_step_budget(mut self, budget: Duration) -> Self {
        self.step_budget = budget;
        self
    }

    /// The fence this runner waits on.
    #[must_use]
    pub fn fence(&self) -> &TenantFence {
        &self.fence
    }

    /// Close the tenant's client connections with `close` on the fence.
    pub fn on_client_connections<F>(&mut self, close: impl FnOnce() -> F + Send + 'static)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.connections.push(boxed(close));
    }

    /// Close the tenant's machine links with `close` on the fence.
    pub fn on_machine_links<F>(&mut self, close: impl FnOnce() -> F + Send + 'static)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.links.push(boxed(close));
    }

    /// Stop the tenant's MCP servers with `stop` on the fence.
    pub fn on_mcp_servers<F>(&mut self, stop: impl FnOnce() -> F + Send + 'static)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.mcp.push(boxed(stop));
    }

    /// Cancel (abort and join) `task`, one of the tenant's `startup_work` or
    /// orchestration tasks, on the fence. Aborting a handle does not reach
    /// tasks it spawned (agent turns, subprocesses): register their owner's
    /// stop with [`Self::on_tasks`].
    pub fn adopt_task(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    /// Stop work the adopted handles do not own (the agent manager's turns
    /// and child processes, PTYs) with `stop` on the fence, in the tasks
    /// step, after the adopted handles are cancelled.
    pub fn on_tasks<F>(&mut self, stop: impl FnOnce() -> F + Send + 'static)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_stops.push(boxed(stop));
    }

    /// Own `value` (the tenant's `Services`, its store handles) until the
    /// fence, then drop it. This releases the runner's handle only: work
    /// holding other clones must be stopped by the steps above.
    pub fn own<T: Send + 'static>(&mut self, value: T) {
        self.owned.push(Box::new(value));
    }

    /// Wait for the fence on a task of its own, then stop the tenant.
    #[must_use]
    pub fn spawn(self) -> JoinHandle<SelfFenceReport> {
        tokio::spawn(self.run())
    }

    /// Wait for the fence, then stop the tenant; see the module docs.
    pub async fn run(self) -> SelfFenceReport {
        self.fence.fenced().await;
        self.stop().await
    }

    /// Stop the tenant now, in order, without waiting for the fence (which
    /// it fires if it has not fired yet).
    pub async fn stop(self) -> SelfFenceReport {
        self.fence
            .fire("the tenant's work was stopped by its owner");
        let Self {
            fence,
            connections,
            links,
            tasks,
            task_stops,
            mcp,
            owned,
            step_budget,
        } = self;
        let mut report = SelfFenceReport {
            reason: fence.reason().unwrap_or_default(),
            fenced_at: Instant::now(),
            steps: Vec::new(),
            overran: Vec::new(),
        };
        tracing::warn!(reason = %report.reason, "self-fence: stopping the tenant's work");
        let budget = step_budget;
        report
            .step(FenceStep::ClientConnections, budget, run_all(connections))
            .await;
        report
            .step(FenceStep::MachineLinks, budget, run_all(links))
            .await;
        report
            .step(FenceStep::Tasks, budget, async move {
                cancel_all(tasks).await;
                run_all(task_stops).await;
            })
            .await;
        report
            .step(FenceStep::McpServers, budget, run_all(mcp))
            .await;
        drop(owned);
        report.steps.push((FenceStep::Services, Instant::now()));
        // Not budgeted: the writes' own timeouts bound it, and the report
        // must not claim a settled fence before it is.
        fence.settled().await;
        report
            .steps
            .push((FenceStep::WritesSettled, Instant::now()));
        tracing::warn!(
            elapsed_ms = u64::try_from(report.fenced_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            overran = ?report.overran,
            "self-fence: the tenant's work stopped"
        );
        report
    }
}

impl SelfFenceReport {
    async fn step(&mut self, step: FenceStep, budget: Duration, work: impl Future<Output = ()>) {
        if tokio::time::timeout(budget, work).await.is_err() {
            tracing::error!(
                ?step,
                budget_ms = budget.as_millis(),
                "self-fence step overran"
            );
            self.overran.push(step);
        }
        self.steps.push((step, Instant::now()));
    }

    /// When `step` finished, if it ran.
    #[must_use]
    pub fn finished(&self, step: FenceStep) -> Option<Instant> {
        self.steps
            .iter()
            .find(|(s, _)| *s == step)
            .map(|(_, at)| *at)
    }
}

fn boxed<F>(stop: impl FnOnce() -> F + Send + 'static) -> Stop
where
    F: Future<Output = ()> + Send + 'static,
{
    Box::new(move || Box::pin(stop()) as StopFuture)
}

/// Run every stop concurrently: one slow close does not hold up the rest.
async fn run_all(stops: Vec<Stop>) {
    let running: Vec<JoinHandle<()>> = stops.into_iter().map(|s| tokio::spawn(s())).collect();
    for stop in running {
        if let Err(e) = stop.await {
            tracing::error!(error = %e, "self-fence stop failed");
        }
    }
}

async fn cancel_all(tasks: Vec<JoinHandle<()>>) {
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        match task.await {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => tracing::error!(error = %e, "tenant task failed while self-fencing"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FenceStep, SelfFence, TenantFence};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    fn wait() -> Duration {
        intentd_test_support::test_timeout(Duration::from_secs(10))
    }

    #[tokio::test]
    async fn the_first_fire_wins_and_wakes_waiters() {
        let fence = TenantFence::new();
        let waiter = tokio::spawn({
            let fence = fence.clone();
            async move { fence.fenced().await }
        });
        assert!(!fence.is_fenced());
        assert!(fence.fire("first"));
        assert!(!fence.fire("second"));
        assert_eq!(fence.reason().as_deref(), Some("first"));
        tokio::time::timeout(wait(), waiter)
            .await
            .expect("the waiter wakes")
            .unwrap();
        fence.settled().await;
    }

    #[tokio::test]
    async fn settled_waits_for_the_open_write() {
        let slot = Arc::new(Semaphore::new(1));
        let fence = TenantFence::guarding_writes(slot.clone());
        let write = slot.clone().acquire_owned().await.unwrap();
        fence.fire("lost");
        let settled = tokio::spawn({
            let fence = fence.clone();
            async move { fence.settled().await }
        });
        // timing-guard: give the settle wait a chance to (wrongly) finish while the write is open
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!settled.is_finished(), "an open write holds the slot");
        drop(write);
        tokio::time::timeout(wait(), settled)
            .await
            .expect("settles once the write ends")
            .unwrap();
    }

    /// Records the order the stops ran in.
    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<Vec<&'static str>>>);

    impl Log {
        fn push(&self, what: &'static str) {
            self.0.lock().unwrap().push(what);
        }
        fn get(&self) -> Vec<&'static str> {
            self.0.lock().unwrap().clone()
        }
    }

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn the_fence_stops_everything_in_order() {
        let fence = TenantFence::new();
        let log = Log::default();
        let mut runner = SelfFence::new(fence.clone());
        let l = log.clone();
        runner.on_client_connections(move || async move { l.push("connections") });
        let l = log.clone();
        runner.on_machine_links(move || async move { l.push("links") });
        let l = log.clone();
        runner.on_mcp_servers(move || async move { l.push("mcp") });
        let l = log.clone();
        runner.on_tasks(move || async move { l.push("agents") });
        let task = tokio::spawn(std::future::pending::<()>());
        let aborted = task.abort_handle();
        runner.adopt_task(task);
        let dropped = Arc::new(AtomicBool::new(false));
        runner.own(DropFlag(dropped.clone()));
        let running = runner.spawn();

        // timing-guard: nothing stops before the fence fires
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(log.get().is_empty());
        assert!(!dropped.load(Ordering::SeqCst));

        fence.fire("lease lost");
        let report = tokio::time::timeout(wait(), running)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.reason, "lease lost");
        assert_eq!(log.get(), ["connections", "links", "agents", "mcp"]);
        assert!(aborted.is_finished(), "the tenant's task was cancelled");
        assert!(
            dropped.load(Ordering::SeqCst),
            "the tenant's Services were dropped"
        );
        let order: Vec<FenceStep> = report.steps.iter().map(|(s, _)| *s).collect();
        assert_eq!(
            order,
            [
                FenceStep::ClientConnections,
                FenceStep::MachineLinks,
                FenceStep::Tasks,
                FenceStep::McpServers,
                FenceStep::Services,
                FenceStep::WritesSettled,
            ]
        );
        assert!(report.overran.is_empty());
    }

    #[tokio::test]
    async fn a_hung_close_does_not_hold_up_the_rest() {
        let fence = TenantFence::new();
        let log = Log::default();
        let mut runner = SelfFence::new(fence.clone()).with_step_budget(Duration::from_millis(50));
        runner.on_client_connections(std::future::pending::<()>);
        let l = log.clone();
        runner.on_client_connections(move || async move { l.push("other connections") });
        let l = log.clone();
        runner.on_machine_links(move || async move { l.push("links") });
        let report = tokio::time::timeout(wait(), runner.stop()).await.unwrap();
        assert!(fence.is_fenced(), "stop() fires the fence");
        assert_eq!(log.get(), ["other connections", "links"]);
        assert_eq!(report.overran, [FenceStep::ClientConnections]);
    }
}
