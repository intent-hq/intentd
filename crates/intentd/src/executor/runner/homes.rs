//! Workspace homes on the machine (monorepo `docs/protocol/node-link.md`,
//! "Workspace homes (nodeProtocol 19)"): the workspace checkout owner of
//! each `homeId` the backend opened here, its setup, and its continuous
//! backup to the workspace's `home/head` and `home/wip`.
//!
//! A home checkout belongs to the workspace, not to an agent: no agent stop
//! removes it. Only the bind-time sweep (a home missing from the hello's
//! `leaseHomes`) does here.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch};

use intent_core::node_link::{
    HomeOpen, HomeOpenOutcome, HomeOpenReply, HomePush, HomePushOutcome, HomePushReply, HomeRef,
    JournalBody, RecordOwner, SetupCommand, SubmodulePin, HOMES_PROTOCOL, NODE_HOME_UNKNOWN,
    NODE_HOME_UNSUPPORTED,
};
use intent_core::HomeId;
use intent_nodes::journal::JournalEntry;

use super::{codes, lock, Runner};
use crate::executor::checkout::HomePush as Pushed;
use crate::executor::setup::{self, SetupOutput, SetupStatus};

/// The latest a `homeOpenReply` may come after its `homeOpen`.
const OPEN_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// The bound on a synchronous `homePush`.
const PUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long after a commit `home/head` is pushed.
const WATCH_INTERVAL: Duration = Duration::from_secs(2);

/// How often a changing checkout's `home/wip` is pushed at least.
const WIP_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Whether this build holds workspace homes. Homes ship in steps; until
/// every home frame is served, a machine reports `homes: false` and refuses
/// `homeOpen`, so no backend makes it a home it cannot fully serve.
/// `INTENTD_UNSTABLE_HOMES=1` lifts the gate for tests.
pub(crate) fn homes_ready() -> bool {
    std::env::var_os("INTENTD_UNSTABLE_HOMES").is_some_and(|value| value == "1")
}

/// The homes of the bound backend.
#[derive(Default)]
pub(super) struct Homes {
    /// The open home checkouts.
    open: HashMap<HomeId, Arc<Home>>,
    /// The bound hello's `leaseHomes`, plus each home a `homeOpen` opened
    /// since: a home frame for any other `homeId` is refused.
    lease: HashSet<HomeId>,
}

/// One open home checkout.
pub(super) struct Home {
    of: HomeRef,
    worktree: PathBuf,
    /// The last `home/head` and `home/wip` this machine pushed; a push holds
    /// it, so the checkout's pushes run one at a time.
    pushed: tokio::sync::Mutex<Option<Pushed>>,
    /// When `home/wip` was last pushed.
    wip_pushed_at: Mutex<Instant>,
    /// `true` once the checkout's setup has finished (or was skipped).
    setup_done: watch::Sender<bool>,
    /// The setup's output while it runs, resent after a reconnect.
    output: Mutex<Option<Arc<SetupOutput>>>,
    /// Taken (dropped) to stop a running setup.
    setup_stop: Mutex<Option<oneshot::Sender<()>>>,
    /// Set when the home is closed: its backup watcher ends.
    closed: watch::Sender<bool>,
}

impl Home {
    /// The commit the checkout's `HEAD` is at was pushed last.
    async fn last_head(&self) -> Option<String> {
        self.pushed.lock().await.as_ref().map(|p| p.head.clone())
    }
}

impl Runner {
    /// Adopt a bound hello's `leaseHomes`, and return the homes the
    /// bind-time sweep removes: those with a directory here that are
    /// neither listed nor open. A hello without `leaseHomes` (an older
    /// backend) sweeps none.
    pub(in crate::executor) fn adopt_home_lease(&self, lease: Option<&[HomeRef]>) -> Vec<HomeId> {
        let Some(lease) = lease else {
            lock(&self.inner.homes).lease.clear();
            return Vec::new();
        };
        let leased: HashSet<HomeId> = lease.iter().map(|home| home.home_id.clone()).collect();
        let mut homes = lock(&self.inner.homes);
        homes.lease.clone_from(&leased);
        let open: HashSet<HomeId> = homes.open.keys().cloned().collect();
        drop(homes);
        self.inner
            .checkouts
            .homes()
            .into_iter()
            .map(|(home_id, _)| home_id)
            .filter(|home_id| !leased.contains(home_id) && !open.contains(home_id))
            .collect()
    }

    /// Reopen the leased homes that have a checkout here but are not open
    /// (after this machine restarted): their watcher resumes, and a setup
    /// a restart interrupted is journaled as failed.
    pub(in crate::executor) async fn resume_leased_homes(&self, lease: &[HomeRef]) {
        for home in lease {
            // Under the open lock, so a `homeOpen` resent after the restart
            // and this reopen never both open the same home.
            let _opening = self.inner.home_opens.lock().await;
            if lock(&self.inner.homes).open.contains_key(&home.home_id) {
                continue;
            }
            let Some(worktree) = self.inner.checkouts.existing_home(&home.home_id) else {
                continue;
            };
            let status = match setup::read_status(&worktree) {
                Some(status) if status.state == setup::Recorded::Running => {
                    // What the interrupted setup made stays out of `home/wip`.
                    let pins = crate::executor::checkout::recorded_pins(&worktree);
                    setup::exclude_interrupted(&worktree, &pins).await;
                    Some(status.interrupted())
                }
                _ => None,
            };
            let (opened, _) = self.insert_home(home.clone(), worktree.clone());
            if let Some(status) = status {
                setup::write_status(&worktree, &status);
                self.journal_setup_finished(home, &status).await;
            }
            opened.setup_done.send_replace(true);
            self.watch_home(&opened);
        }
    }

    /// Remove the swept homes' checkouts.
    pub(in crate::executor) async fn sweep_homes(&self, candidates: Vec<HomeId>) {
        for home_id in candidates {
            if lock(&self.inner.homes).lease.contains(&home_id) {
                continue;
            }
            if let Err(error) = self.inner.checkouts.remove_home(&home_id).await {
                tracing::warn!(home = %home_id, %error, "node: could not remove a swept home checkout");
            }
        }
    }

    /// Make the home checkout `frame` names and answer once it exists (at
    /// most [`OPEN_TIMEOUT`] after the frame); its setup runs after.
    pub(in crate::executor) async fn home_open(&self, frame: HomeOpen) -> HomeOpenReply {
        let deadline = tokio::time::Instant::now() + OPEN_TIMEOUT;
        let home = frame.home();
        let reply = |outcome, error: Option<String>| HomeOpenReply {
            request_id: frame.request_id,
            workspace_id: home.workspace_id.clone(),
            home_id: home.home_id.clone(),
            outcome,
            error,
        };
        if !self.inner.launch.allows_homes() {
            return reply(
                HomeOpenOutcome::Failed,
                Some(format!(
                    "{NODE_HOME_UNSUPPORTED}: this machine does not hold workspace homes"
                )),
            );
        }
        let other_workspace = || {
            reply(
                HomeOpenOutcome::Failed,
                Some(format!(
                    "{NODE_HOME_UNKNOWN}: home {} belongs to another workspace",
                    home.home_id
                )),
            )
        };
        let timed_out = || {
            reply(
                HomeOpenOutcome::Failed,
                Some(format!(
                    "{}: the checkout took longer than {} minutes",
                    codes::CHECKOUT,
                    OPEN_TIMEOUT.as_secs() / 60
                )),
            )
        };
        // An open home answers at once, without waiting for another open.
        match self.open_state(&home) {
            OpenState::Open => return reply(HomeOpenOutcome::Ready, None),
            OpenState::OtherWorkspace => return other_workspace(),
            OpenState::Closed => {}
        }
        let Ok(opening) =
            tokio::time::timeout_at(deadline, self.inner.home_opens.clone().lock_owned()).await
        else {
            return timed_out();
        };
        match self.open_state(&home) {
            OpenState::Open => return reply(HomeOpenOutcome::Ready, None),
            OpenState::OtherWorkspace => return other_workspace(),
            OpenState::Closed => {}
        }
        // A checkout on disk recorded for another workspace is not this
        // home either.
        let recorded = self
            .inner
            .checkouts
            .homes()
            .into_iter()
            .find(|(id, _)| id == &home.home_id)
            .and_then(|(_, workspace)| workspace);
        if recorded.is_some_and(|workspace| workspace != home.workspace_id) {
            return other_workspace();
        }
        lock(&self.inner.homes).lease.insert(home.home_id.clone());
        let existed = self.inner.checkouts.existing_home(&home.home_id).is_some();
        let output = SetupOutput::new(
            intent_core::node_link::SetupOwner::Home(home.clone()),
            self.inner.head.clone(),
        );
        let progress = |text: &str| output.line(text);
        let prepared = tokio::time::timeout_at(
            deadline,
            self.inner.checkouts.prepare_home(
                &home,
                &frame.repo_key,
                &frame.checkout.submodules,
                &progress,
            ),
        )
        .await;
        let failed = match prepared {
            Ok(Ok(prepared)) => Ok(prepared),
            Ok(Err(error)) => Err(reply(
                HomeOpenOutcome::Failed,
                Some(format!("{}: {error:#}", codes::CHECKOUT)),
            )),
            Err(_) => Err(timed_out()),
        };
        let prepared = match failed {
            Ok(prepared) => prepared,
            Err(failed) => {
                // A checkout this open started and could not finish is
                // removed, so the next `homeOpen` makes it again instead of
                // keeping it without its submodules or setup. The reply does
                // not wait for it; the next open does (it holds the lock).
                if !existed {
                    let checkouts = self.inner.checkouts.clone();
                    let home_id = home.home_id.clone();
                    // caller-binding: allow — node-local removal of a partial home checkout
                    tokio::spawn(async move {
                        if let Err(error) = checkouts.remove_home(&home_id).await {
                            tracing::warn!(home = %home_id, %error, "node: could not remove a partial home checkout");
                        }
                        drop(opening);
                    });
                }
                return failed;
            }
        };
        let (opened, stop) = self.insert_home(home.clone(), prepared.worktree.clone());
        *lock(&opened.output) = Some(output.clone());
        let runner = self.clone();
        let setup = SetupJob {
            command: frame.checkout.setup.clone(),
            fresh: prepared.fresh,
            repo_key: frame.repo_key.clone(),
            pins: frame.checkout.submodules.clone(),
            tmp: prepared.tmp,
        };
        // caller-binding: allow — node-local setup and backup of one home checkout
        tokio::spawn(async move {
            runner
                .settle_home_setup(&opened, setup, &output, stop)
                .await;
            lock(&opened.output).take();
            opened.setup_done.send_replace(true);
            runner.watch_home(&opened);
        });
        reply(HomeOpenOutcome::Ready, None)
    }

    /// Push the home checkout now and answer with what was pushed.
    pub(in crate::executor) async fn home_push(&self, frame: HomePush) -> HomePushReply {
        let reply = |outcome, pushed: Option<&Pushed>, error: Option<String>| HomePushReply {
            request_id: frame.request_id,
            workspace_id: frame.workspace_id.clone(),
            home_id: frame.home_id.clone(),
            outcome,
            head: pushed.map(|p| p.head.clone()),
            wip: pushed.map(|p| p.wip.clone()),
            error,
        };
        let Some(home) = self.leased_home(&frame.workspace_id, &frame.home_id) else {
            return reply(
                HomePushOutcome::Failed,
                None,
                Some(format!(
                    "{NODE_HOME_UNKNOWN}: this machine holds no home {}",
                    frame.home_id
                )),
            );
        };
        let deadline = tokio::time::Instant::now() + PUSH_TIMEOUT;
        // The reply is due by the deadline, journaling included; a push cut
        // short there is journaled by the next one (`pushed` is not moved).
        let pushed = tokio::time::timeout_at(deadline, self.push_home_now(&home, deadline))
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("the home push did not end in time")));
        match pushed {
            Ok(pushed) => reply(HomePushOutcome::Pushed, Some(&pushed), None),
            Err(error) => reply(
                HomePushOutcome::Failed,
                None,
                Some(format!("node-home-push-failed: {error:#}")),
            ),
        }
    }

    /// Resend the kept setup output of every home whose setup still runs.
    pub(in crate::executor) fn resend_home_setup_output(
        &self,
        link: &intent_transport::node_link::NodeLink,
    ) {
        let outputs: Vec<Arc<SetupOutput>> = lock(&self.inner.homes)
            .open
            .values()
            .filter_map(|home| lock(&home.output).clone())
            .collect();
        for output in outputs {
            output.resend(link);
        }
    }

    /// Stop every home's setup and watcher (machine shutdown).
    pub(in crate::executor) fn close_homes(&self) {
        let homes: Vec<Arc<Home>> = lock(&self.inner.homes)
            .open
            .drain()
            .map(|(_, h)| h)
            .collect();
        for home in homes {
            lock(&home.setup_stop).take();
            home.closed.send_replace(true);
        }
    }

    fn open_state(&self, home: &HomeRef) -> OpenState {
        match lock(&self.inner.homes).open.get(&home.home_id) {
            None => OpenState::Closed,
            Some(open) if open.of.workspace_id == home.workspace_id => OpenState::Open,
            Some(_) => OpenState::OtherWorkspace,
        }
    }

    /// The open home `home_id` of `workspace_id`, when the lease names it.
    fn leased_home(
        &self,
        workspace_id: &intent_core::WorkspaceId,
        home_id: &HomeId,
    ) -> Option<Arc<Home>> {
        let homes = lock(&self.inner.homes);
        if !homes.lease.contains(home_id) {
            return None;
        }
        homes
            .open
            .get(home_id)
            .filter(|home| &home.of.workspace_id == workspace_id)
            .cloned()
    }

    /// Record `home` open at `worktree`; the receiver resolves when its
    /// setup must stop.
    fn insert_home(&self, home: HomeRef, worktree: PathBuf) -> (Arc<Home>, oneshot::Receiver<()>) {
        let (stop, stopped) = oneshot::channel();
        let opened = Arc::new(Home {
            of: home,
            worktree,
            pushed: tokio::sync::Mutex::new(None),
            wip_pushed_at: Mutex::new(Instant::now()),
            setup_done: watch::channel(false).0,
            output: Mutex::new(None),
            setup_stop: Mutex::new(Some(stop)),
            closed: watch::channel(false).0,
        });
        // Callers hold `home_opens`; an entry already there is never
        // replaced, so a home never has two watchers.
        let opened = lock(&self.inner.homes)
            .open
            .entry(opened.of.home_id.clone())
            .or_insert(opened)
            .clone();
        (opened, stopped)
    }

    /// Run a fresh checkout's setup (or record `skipped`), or account for a
    /// kept checkout's interrupted one, and journal it for the home.
    async fn settle_home_setup(
        &self,
        home: &Home,
        job: SetupJob,
        output: &Arc<SetupOutput>,
        mut stop: oneshot::Receiver<()>,
    ) {
        let cwd = home.worktree.as_path();
        let status = match (job.fresh, job.command) {
            (false, _) => match setup::read_status(cwd) {
                Some(status) if status.state == setup::Recorded::Running => {
                    setup::exclude_interrupted(cwd, &job.pins).await;
                    status.interrupted()
                }
                Some(_) => return,
                None => SetupStatus::skipped(),
            },
            (true, None) => SetupStatus::skipped(),
            (true, Some(command)) => {
                match self
                    .run_home_setup(
                        home,
                        &job.repo_key,
                        &job.pins,
                        &job.tmp,
                        &command,
                        output,
                        &mut stop,
                    )
                    .await
                {
                    setup::Ran::Finished(status) => status,
                    setup::Ran::Stopped => return,
                }
            }
        };
        setup::write_status(cwd, &status);
        self.journal_setup_finished(&home.of, &status).await;
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one setup run's inputs, as the agent's run_setup takes them"
    )]
    async fn run_home_setup(
        &self,
        home: &Home,
        repo_key: &str,
        pins: &[SubmodulePin],
        tmp: &std::path::Path,
        command: &SetupCommand,
        output: &Arc<SetupOutput>,
        stop: &mut oneshot::Receiver<()>,
    ) -> setup::Ran {
        let cwd = home.worktree.as_path();
        let main_checkout = match self.inner.checkouts.main_checkout(repo_key) {
            Ok(dir) => dir,
            Err(error) => {
                return setup::Ran::Finished(SetupStatus {
                    state: setup::Recorded::Failed,
                    exit_code: None,
                    error: Some(format!(
                        "{}: could not create MAIN_CHECKOUT: {error:#}",
                        setup::codes::FAILED
                    )),
                    started_at: None,
                    finished_at: Some(intent_core::now_iso()),
                });
            }
        };
        let started_at = intent_core::now_iso();
        setup::write_status(
            cwd,
            &SetupStatus {
                state: setup::Recorded::Running,
                exit_code: None,
                error: None,
                started_at: Some(started_at.clone()),
                finished_at: None,
            },
        );
        self.append(home_entry(
            &home.of,
            JournalBody::SetupStarted {
                started_at: started_at.clone(),
            },
        ))
        .await;
        setup::run(
            &self.inner.pty,
            setup::SetupRun {
                command,
                worktree: cwd,
                tmp,
                main_checkout: &main_checkout,
                pins,
            },
            output,
            started_at,
            stop,
        )
        .await
    }

    async fn journal_setup_finished(&self, home: &HomeRef, status: &SetupStatus) {
        if let Some(state) = status.finished_state() {
            self.append(home_entry(
                home,
                JournalBody::SetupFinished {
                    state,
                    exit_code: status.exit_code,
                    error: status.error.clone(),
                    finished_at: status
                        .finished_at
                        .clone()
                        .unwrap_or_else(intent_core::now_iso),
                },
            ))
            .await;
        }
    }

    /// Push the home checkout and journal `homePushed` when it moved.
    /// The push, waiting for one under way first, ends by `deadline`.
    async fn push_home_now(
        &self,
        home: &Home,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<Pushed> {
        let mut last = tokio::time::timeout_at(deadline, home.pushed.lock())
            .await
            .map_err(|_| anyhow::anyhow!("another push of the home did not end in time"))?;
        let previous = last.as_ref().map(|p| p.head.clone());
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let pushed = tokio::time::timeout_at(
            deadline,
            self.inner
                .checkouts
                .push_home(&home.of, &home.worktree, previous.as_deref(), left),
        )
        .await
        .map_err(|_| anyhow::anyhow!("the home push did not end in time"))??;
        *lock(&home.wip_pushed_at) = Instant::now();
        let moved = last
            .as_ref()
            .is_none_or(|was| was.head != pushed.head || was.wip != pushed.wip);
        if moved {
            self.append(home_entry(
                &home.of,
                JournalBody::HomePushed {
                    head: Some(pushed.head.clone()),
                    wip: Some(pushed.wip.clone()),
                    rewritten: pushed.rewritten.clone(),
                },
            ))
            .await;
        }
        *last = Some(pushed.clone());
        Ok(pushed)
    }

    /// Back the home checkout up until it is closed: `home/head` about
    /// [`WATCH_INTERVAL`] after its `HEAD` moves, and both refs at least
    /// every [`WIP_INTERVAL`]. Nothing is pushed while no link to the
    /// backend speaks [`HOMES_PROTOCOL`].
    fn watch_home(&self, home: &Arc<Home>) {
        let runner = self.clone();
        let home = home.clone();
        let mut closed = home.closed.subscribe();
        // caller-binding: allow — node-local backup watcher of one home checkout
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WATCH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = closed.wait_for(|closed| *closed) => return,
                    _ = tick.tick() => {}
                }
                let linked = runner
                    .inner
                    .head
                    .get()
                    .is_some_and(|link| link.protocol() >= HOMES_PROTOCOL);
                if !linked {
                    continue;
                }
                let head = runner.inner.checkouts.head_commit(&home.worktree).await;
                let moved = head.is_some() && head != home.last_head().await;
                let due = lock(&home.wip_pushed_at).elapsed() >= WIP_INTERVAL;
                if !(moved || due) {
                    continue;
                }
                let deadline = tokio::time::Instant::now() + PUSH_TIMEOUT;
                if let Err(error) = runner.push_home_now(&home, deadline).await {
                    tracing::info!(home = %home.of.home_id, %error, "node: home backup push failed; retrying");
                }
            }
        });
    }
}

/// Whether a home is open here, for a `homeOpen`.
enum OpenState {
    Open,
    OtherWorkspace,
    Closed,
}

/// A setup run's inputs for a home checkout.
struct SetupJob {
    command: Option<SetupCommand>,
    fresh: bool,
    repo_key: String,
    pins: Vec<SubmodulePin>,
    tmp: PathBuf,
}

/// A journal entry of `home` with `body`.
fn home_entry(home: &HomeRef, body: JournalBody) -> JournalEntry {
    JournalEntry {
        owner: RecordOwner::Home(home.home_id.clone()),
        workspace_id: home.workspace_id.clone(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body,
    }
}
