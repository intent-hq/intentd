//! Home runs of saved scripts, backend side (nodeProtocol 19, node-link.md
//! § Home runs and setup). The script supervisor keeps each run as a
//! remote PTY in the backend's [`PtyHost`], so its state, restarts, output
//! log and monitors are the home run's as before; only the process runs in
//! the home checkout on the machine, over the script frames naming the
//! workspace and its home. `scriptOutput` feeds the PTY, `scriptExited`
//! ends it; a kill of the PTY is the run's `scriptStop`.
//!
//! As for a machine run, a lost link loses the run at once
//! (`remote-script-lost`): the machine ends the binding's processes then.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use intent_core::node_link::{
    CheckoutOwner, HomeRef, LinkFrame, ScriptEnvVar, ScriptExited, ScriptOutput, ScriptProcessRef,
    ScriptStartCommand, HOMES_PROTOCOL,
};
use intent_core::{Result, WorkspaceId};
use intent_pty::{PtyHost, PtyId, PtySize, RemotePty, RemoteSpec};
use intent_services::feature_api::feature_ports::HomeScriptOpen;
use intent_services::Services;
use tokio::sync::mpsc;

use crate::connector::Shared;
use crate::terminals::home_unavailable;
use crate::RemoteServicesExt as _;

/// The `error` a run lost with its link ends with, as a machine run's.
pub(crate) const LOST_DISCONNECTED: &str = "remote-script-lost: the machine disconnected";

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One machine's home runs, by `spawnId`.
#[derive(Default)]
pub(crate) struct HomeScripts {
    runs: Mutex<HashMap<String, Run>>,
    /// The homes whose `autoStart` scripts were started by this process.
    auto_started: Mutex<std::collections::HashSet<intent_core::HomeId>>,
}

struct Run {
    pty: Arc<PtyHost>,
    id: PtyId,
    home: HomeRef,
    script_id: String,
    /// The next `seq` taken; a frame from before it is a duplicate.
    next_seq: u64,
    /// The home's name, for a start failure or loss shown in the output.
    home_name: String,
    /// The run's `scriptStop`, and the queue it goes out on (after its
    /// `scriptStart`).
    stop: ScriptProcessRef,
    frames: mpsc::UnboundedSender<LinkFrame>,
    /// Set once its `scriptStop` was sent: no second one goes out.
    stopping: bool,
    /// Turns `true` when the run ended (its `scriptExited`, or a loss).
    done: tokio::sync::watch::Sender<bool>,
}

impl Run {
    /// Send the run's `scriptStop`, once.
    fn send_stop(&mut self) {
        if !self.stopping {
            self.stopping = true;
            let _ = self.frames.send(LinkFrame::ScriptStop(self.stop.clone()));
        }
    }

    /// End the PTY: `exit_code`; or, with `error`, without one, `error`
    /// shown as the run's last output line and kept as why it ended.
    fn end(&self, exit_code: Option<i32>, error: Option<&str>) {
        self.done.send_replace(true);
        let Some(error) = error else {
            let _ = self.pty.remote_exited(self.id, exit_code);
            return;
        };
        let line = format!(
            "\r\n[intent] the run on {} ended: {error}\r\n",
            self.home_name
        );
        let _ = self.pty.push_remote_output(self.id, line.as_bytes());
        // The supervisor records `error` as the run's, and a service lost
        // this way is not restarted, as a machine run's is not.
        let _ = self.pty.remote_lost(self.id, error);
    }
}

impl Drop for HomeScripts {
    /// The machine's connector is going away (it was removed or replaced):
    /// its runs end here rather than stay live with no machine.
    fn drop(&mut self) {
        let runs = self
            .runs
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, run) in runs.drain() {
            run.end(None, Some("remote-script-lost: the machine was removed"));
        }
    }
}

impl HomeScripts {
    /// Whether `home_id`'s `autoStart` scripts are due: `true` the first
    /// time only.
    pub(crate) fn first_auto_start(&self, home_id: &intent_core::HomeId) -> bool {
        lock(&self.auto_started).insert(home_id.clone())
    }

    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.runs).is_empty()
    }

    /// Take a home run's script frame from the machine. Frames of a spawn
    /// not held here (an ended run) are dropped.
    pub(crate) fn frame(&self, frame: LinkFrame) {
        match frame {
            LinkFrame::ScriptStarted(_) => {}
            LinkFrame::ScriptOutput(output) => self.output(&output),
            LinkFrame::ScriptExited(exited) => self.exited(&exited),
            other => {
                tracing::debug!(channel = ?other.channel(), "remote machine: ignoring an unexpected home run frame");
            }
        }
    }

    fn output(&self, frame: &ScriptOutput) {
        let target = {
            let mut runs = lock(&self.runs);
            let Some(run) = runs
                .get_mut(&frame.spawn_id)
                .filter(|run| owns(run, &frame.owner, &frame.script_id))
            else {
                return;
            };
            if frame.seq < run.next_seq {
                return;
            }
            if frame.seq > run.next_seq {
                // The machine numbers every frame it sends; a gap is output
                // the link lost, which the log cannot get back.
                tracing::warn!(spawn = %frame.spawn_id, seq = frame.seq, expected = run.next_seq, "remote machine: home run output out of order");
            }
            run.next_seq = frame.seq + 1;
            (run.pty.clone(), run.id)
        };
        // Outside the lock: the PTY host calls back into it on a kill.
        let (pty, id) = target;
        let _ = pty.push_remote_output(id, frame.data.as_bytes());
    }

    fn exited(&self, frame: &ScriptExited) {
        let mut runs = lock(&self.runs);
        if !runs
            .get(&frame.spawn_id)
            .is_some_and(|run| owns(run, &frame.owner, &frame.script_id))
        {
            return;
        }
        let Some(run) = runs.remove(&frame.spawn_id) else {
            return;
        };
        drop(runs);
        // A refused start or stop carries `error` without an exit code.
        let error = frame.error.as_deref().filter(|_| frame.exit_code.is_none());
        run.end(frame.exit_code, error);
    }

    /// Stop the run of PTY `id` in `pty` and wait, up to `wait`, for the
    /// machine to report its end, as a machine run's stop does. The run
    /// stays held until its `scriptExited` (or a lost link) ends it, so a
    /// late end still settles it, and no new run of the same script starts
    /// on the home meanwhile ([`Self::stopping`]).
    pub(crate) async fn stop(&self, pty: &Arc<PtyHost>, id: PtyId, wait: Duration) -> Stopped {
        let mut done = {
            let mut runs = lock(&self.runs);
            let Some(run) = runs
                .values_mut()
                .find(|run| run.id == id && Arc::ptr_eq(&run.pty, pty))
            else {
                return Stopped::NotHeld;
            };
            run.send_stop();
            run.done.subscribe()
        };
        let ended = tokio::time::timeout(wait, done.wait_for(|done| *done))
            .await
            .is_ok();
        if ended {
            Stopped::Ended
        } else {
            Stopped::Unconfirmed
        }
    }

    /// Whether a run of `script_id` on `home` was stopped and has not ended
    /// yet: its process may still hold what a new run needs.
    pub(crate) fn stopping(&self, home: &HomeRef, script_id: &str) -> bool {
        lock(&self.runs)
            .values()
            .any(|run| &run.home == home && run.script_id == script_id && run.stopping)
    }

    /// The link closed: every home run on it is lost (the machine ends the
    /// binding's processes as its link closes).
    pub(crate) fn link_lost(&self, reason: &str) {
        let lost: Vec<Run> = lock(&self.runs).drain().map(|(_, run)| run).collect();
        for run in lost {
            run.end(None, Some(reason));
        }
    }
}

fn owns(run: &Run, owner: &CheckoutOwner, script_id: &str) -> bool {
    owner.home() == Some(&run.home) && run.script_id == script_id
}

/// Stops one home run: the PTY's kill. A home run takes no input.
struct LinkRun {
    shared: Weak<Shared>,
    spawn_id: String,
}

impl RemotePty for LinkRun {
    fn write(&self, _data: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("a home run takes no input"))
    }

    fn resize(&self, _size: PtySize) -> std::io::Result<()> {
        Ok(())
    }

    fn kill(&self) {
        // The PTY is being released. The run stays held, stopping, until
        // the machine reports its end (or the link is lost), so no new run
        // of the script starts on the home while it may still run. A run
        // that already ended, or whose stop already went out, sends none.
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let mut runs = lock(&shared.home_scripts.runs);
        if let Some(run) = runs.get_mut(&self.spawn_id) {
            run.send_stop();
        }
        drop(runs);
    }
}

/// Start one process of `workspace_id`'s home run of a saved script as a
/// remote PTY in `pty`. `None` when the workspace has no home on a
/// machine, so the caller runs it here.
///
/// # Errors
///
/// `home-unavailable` when the home's machine is not linked now (the
/// caller dials it first, outside its locks) or does not speak
/// nodeProtocol 19.
pub(crate) async fn open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    pty: &Arc<PtyHost>,
    open: HomeScriptOpen,
) -> Result<Option<PtyId>> {
    if !svc.workspace_homes() {
        return Ok(None);
    }
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(None);
    };
    let Some(node_id) = home.node_id.clone() else {
        return Ok(None);
    };
    let feature = svc
        .remote_feature()
        .ok_or_else(|| home_unavailable(&home, "no remote machine feature"))?;
    let node = feature
        .node(svc, &node_id)
        .await
        .map_err(|error| home_unavailable(&home, &error.to_string()))?;
    // No dial here: the caller may hold a lock other scripts wait on.
    let shared = node.shared_ref();
    if shared.protocol() < HOMES_PROTOCOL {
        return Err(home_unavailable(
            &home,
            "its machine needs an intentd update for workspace homes",
        ));
    }
    let Some(link) = shared.current_link() else {
        return Err(home_unavailable(&home, "its machine is not connected"));
    };
    let of = HomeRef {
        workspace_id: workspace_id.clone(),
        home_id: home.home_id.clone(),
    };
    if shared.home_scripts.stopping(&of, &open.script_id) {
        return Err(home_unavailable(
            &home,
            "the script's last run has not ended there yet",
        ));
    }
    let spawn_id = uuid::Uuid::new_v4().to_string();
    let owner = CheckoutOwner::Home(of.clone());
    let (frames, mut queued) = mpsc::unbounded_channel::<LinkFrame>();
    let io = Arc::new(LinkRun {
        shared: shared.weak(),
        spawn_id: spawn_id.clone(),
    });
    let mut spec = RemoteSpec::new(workspace_id.as_str());
    spec.listed = false;
    spec.scrollback_bytes = open.scrollback_bytes;
    spec.output_sink = open.output_sink;
    let id = pty.spawn_remote(spec, io)?;
    lock(&shared.home_scripts.runs).insert(
        spawn_id.clone(),
        Run {
            pty: pty.clone(),
            id,
            home: of,
            script_id: open.script_id.clone(),
            next_seq: 0,
            home_name: home.name.clone(),
            stop: ScriptProcessRef {
                owner: owner.clone(),
                script_id: open.script_id.clone(),
                spawn_id: spawn_id.clone(),
            },
            frames: frames.clone(),
            stopping: false,
            done: tokio::sync::watch::channel(false).0,
        },
    );
    // `scriptStart` goes first on the run's own queue, so its stop never
    // reaches the machine ahead of it.
    let _ = frames.send(LinkFrame::ScriptStart(ScriptStartCommand {
        owner,
        script_id: open.script_id,
        spawn_id: spawn_id.clone(),
        command: open.command,
        mode: open.mode,
        cwd: open.cwd.filter(|cwd| !cwd.is_empty()),
        env: open
            .env
            .into_iter()
            .map(|(key, value)| ScriptEnvVar { key, value })
            .collect(),
    }));
    let forwarder = shared.weak();
    let binding = link.binding().clone();
    drop(link);
    // caller-binding: allow — sends one home run's start and stop in order on the link it started on
    tokio::spawn(async move {
        while let Some(frame) = queued.recv().await {
            let Some(shared) = forwarder.upgrade() else {
                return;
            };
            // Only on the binding the run started on: the machine ended the
            // run with that link, so a later link has nothing to stop.
            match shared.current_link() {
                Some(link) if link.binding() == &binding => {
                    if link.send(&frame).await.is_err() {
                        lose(&shared, &spawn_id);
                        return;
                    }
                }
                _ => {
                    lose(&shared, &spawn_id);
                    return;
                }
            }
        }
    });
    shared.wake();
    Ok(Some(id))
}

/// How [`HomeScripts::stop`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stopped {
    /// The machine reported the run's end.
    Ended,
    /// Not a run held here (it already ended).
    NotHeld,
    /// No end within the wait: the run stays held, stopping.
    Unconfirmed,
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::stop_home_script`].
pub(crate) async fn stop(
    svc: &Services,
    workspace_id: &WorkspaceId,
    pty: &Arc<PtyHost>,
    id: PtyId,
    wait: Duration,
) -> Result<()> {
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(());
    };
    let Some(node_id) = home.node_id.clone() else {
        return Ok(());
    };
    let Some(feature) = svc.remote_feature() else {
        return Ok(());
    };
    let Ok(node) = feature.node(svc, &node_id).await else {
        return Ok(());
    };
    match node.shared_ref().home_scripts.stop(pty, id, wait).await {
        Stopped::Ended | Stopped::NotHeld => Ok(()),
        Stopped::Unconfirmed => Err(home_unavailable(
            &home,
            "the run's stop was not confirmed in time; it is still stopping there",
        )),
    }
}

/// The run's frame could not reach the machine: it is lost.
fn lose(shared: &Shared, spawn_id: &str) {
    let run = lock(&shared.home_scripts.runs).remove(spawn_id);
    if let Some(run) = run {
        run.end(None, Some(LOST_DISCONNECTED));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Quiet;

    impl RemotePty for Quiet {
        fn write(&self, _data: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
        fn resize(&self, _size: PtySize) -> std::io::Result<()> {
            Ok(())
        }
        fn kill(&self) {}
    }

    fn home() -> HomeRef {
        HomeRef {
            workspace_id: WorkspaceId::from("ws-1"),
            home_id: intent_core::HomeId::from("home-1"),
        }
    }

    /// One held run of script `s1`, spawn `p1`, on [`home`].
    fn held(pty: &Arc<PtyHost>) -> (HomeScripts, PtyId) {
        held_with(pty, mpsc::unbounded_channel().0)
    }

    fn held_with(
        pty: &Arc<PtyHost>,
        frames: mpsc::UnboundedSender<LinkFrame>,
    ) -> (HomeScripts, PtyId) {
        let mut spec = RemoteSpec::new("ws-1");
        spec.listed = false;
        let id = pty.spawn_remote(spec, Arc::new(Quiet)).unwrap();
        let scripts = HomeScripts::default();
        lock(&scripts.runs).insert(
            "p1".into(),
            Run {
                pty: pty.clone(),
                id,
                home: home(),
                script_id: "s1".into(),
                next_seq: 0,
                home_name: "Machine A".into(),
                stop: ScriptProcessRef {
                    owner: CheckoutOwner::Home(home()),
                    script_id: "s1".into(),
                    spawn_id: "p1".into(),
                },
                frames,
                stopping: false,
                done: tokio::sync::watch::channel(false).0,
            },
        );
        (scripts, id)
    }

    fn output(owner: CheckoutOwner, seq: u64, data: &str) -> LinkFrame {
        LinkFrame::ScriptOutput(ScriptOutput {
            owner,
            script_id: "s1".into(),
            spawn_id: "p1".into(),
            seq,
            data: data.into(),
        })
    }

    fn exited(exit_code: Option<i32>, error: Option<&str>) -> LinkFrame {
        LinkFrame::ScriptExited(ScriptExited {
            owner: CheckoutOwner::Home(home()),
            script_id: "s1".into(),
            spawn_id: "p1".into(),
            exit_code,
            error: error.map(str::to_owned),
        })
    }

    fn text(pty: &PtyHost, id: PtyId) -> String {
        String::from_utf8_lossy(&pty.scrollback(id).unwrap()).into_owned()
    }

    /// Output is taken in `seq` order (a resent frame once); a frame naming
    /// another home or an agent is not this run's; the exit code ends it.
    #[tokio::test]
    async fn output_feeds_the_run_in_order_and_the_exit_ends_it() {
        let pty = Arc::new(PtyHost::new());
        let (scripts, id) = held(&pty);
        let home = CheckoutOwner::Home(home());
        scripts.frame(output(home.clone(), 0, "one\n"));
        scripts.frame(output(home.clone(), 0, "one\n"));
        scripts.frame(output(home.clone(), 1, "two\n"));
        scripts.frame(output(
            intent_core::AgentId::from("a1").into(),
            2,
            "agent\n",
        ));
        let other = CheckoutOwner::Home(HomeRef {
            workspace_id: WorkspaceId::from("ws-2"),
            home_id: intent_core::HomeId::from("home-1"),
        });
        scripts.frame(output(other, 2, "other\n"));
        assert_eq!(text(&pty, id), "one\ntwo\n");

        scripts.frame(exited(Some(3), None));
        assert!(scripts.is_empty());
        let exit = pty.try_exit(id).unwrap().unwrap();
        assert!(exit.observed);
        assert_eq!(exit.exit_code, 3);
        assert_eq!(pty.remote_lost_reason(id), None);
        // A frame after the end is dropped.
        scripts.frame(output(home, 2, "late\n"));
        assert_eq!(text(&pty, id), "one\ntwo\n");
    }

    /// A start the machine refused ends the run with its error, shown in
    /// the output and kept as why it ended.
    #[tokio::test]
    async fn a_refused_start_ends_the_run_with_its_error() {
        let pty = Arc::new(PtyHost::new());
        let (scripts, id) = held(&pty);
        scripts.frame(exited(None, Some("node-home-unknown: no such home")));
        assert!(scripts.is_empty());
        assert!(text(&pty, id).contains("node-home-unknown: no such home"));
        assert_eq!(
            pty.remote_lost_reason(id).as_deref(),
            Some("node-home-unknown: no such home")
        );
        assert!(!pty.try_exit(id).unwrap().unwrap().observed);
    }

    /// A lost link loses every run at once, as a machine run's.
    #[tokio::test]
    async fn a_lost_link_loses_the_runs() {
        let pty = Arc::new(PtyHost::new());
        let (scripts, id) = held(&pty);
        scripts.link_lost(LOST_DISCONNECTED);
        assert!(scripts.is_empty());
        assert_eq!(
            pty.remote_lost_reason(id).as_deref(),
            Some(LOST_DISCONNECTED)
        );
        assert!(text(&pty, id).contains("Machine A"));
    }

    /// A stop sends one `scriptStop` and waits for the run's own exit; a
    /// second stop sends nothing more.
    #[tokio::test]
    async fn a_stop_waits_for_the_machine_and_is_sent_once() {
        let pty = Arc::new(PtyHost::new());
        let (frames, mut sent) = mpsc::unbounded_channel();
        let (scripts, id) = held_with(&pty, frames);
        let scripts = Arc::new(scripts);
        let stopping = {
            let scripts = scripts.clone();
            let pty = pty.clone();
            tokio::spawn(async move { scripts.stop(&pty, id, Duration::from_secs(30)).await })
        };
        let frame = sent.recv().await.unwrap();
        assert!(matches!(&frame, LinkFrame::ScriptStop(stop) if stop.spawn_id == "p1"));
        assert!(!stopping.is_finished(), "the stop waits for the machine");
        scripts.frame(exited(Some(143), None));
        assert_eq!(stopping.await.unwrap(), Stopped::Ended);
        assert_eq!(pty.try_exit(id).unwrap().unwrap().exit_code, 143);
        assert_eq!(
            scripts.stop(&pty, id, Duration::from_millis(10)).await,
            Stopped::NotHeld
        );
        assert!(sent.try_recv().is_err());
    }

    /// A stop the machine does not confirm in time is reported unconfirmed;
    /// the run stays held and stopping (a new run of the script is refused)
    /// even once its PTY is released, until its late `scriptExited` ends
    /// it; a second stop sends no second `scriptStop`.
    #[tokio::test]
    async fn an_unconfirmed_stop_keeps_the_run_until_its_late_exit() {
        let pty = Arc::new(PtyHost::new());
        let (frames, mut sent) = mpsc::unbounded_channel();
        let (scripts, id) = held_with(&pty, frames);
        assert!(!scripts.stopping(&home(), "s1"));
        assert_eq!(
            scripts.stop(&pty, id, Duration::from_millis(20)).await,
            Stopped::Unconfirmed
        );
        assert!(matches!(sent.try_recv(), Ok(LinkFrame::ScriptStop(_))));
        assert!(scripts.stopping(&home(), "s1"));
        assert!(!scripts.stopping(&home(), "s2"));
        // The supervisor's release of the PTY keeps the run held.
        pty.kill(id).await;
        assert!(scripts.stopping(&home(), "s1"));
        assert!(sent.try_recv().is_err(), "one scriptStop only");
        scripts.frame(exited(Some(143), None));
        assert!(!scripts.stopping(&home(), "s1"));
        assert!(scripts.is_empty());
    }

    /// A lost link ends a stopping run too.
    #[tokio::test]
    async fn a_lost_link_ends_a_stopping_run() {
        let pty = Arc::new(PtyHost::new());
        let (scripts, id) = held(&pty);
        assert_eq!(
            scripts.stop(&pty, id, Duration::from_millis(10)).await,
            Stopped::Unconfirmed
        );
        scripts.link_lost(LOST_DISCONNECTED);
        assert!(!scripts.stopping(&home(), "s1"));
    }
}
