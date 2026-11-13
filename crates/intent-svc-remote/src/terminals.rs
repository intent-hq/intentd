//! Terminals on a workspace home, backend side (nodeProtocol 19,
//! node-link.md § Terminals on the home). Each is a remote PTY in the
//! backend's [`PtyHost`]: its buffer, replay offsets, list row and every
//! client attach are served here, while the process runs in the home
//! checkout on the machine. `ptyOutput` is appended in `seq` order and
//! acknowledged; input, resizes and kills go to the machine in order.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use base64::Engine as _;
use intent_core::node_link::{
    LinkFrame, PtyAck, PtyEnvVar, PtyExited, PtyInput, PtyKill, PtyMode, PtyOpen, PtyOutput,
    PtyResize, HOMES_PROTOCOL,
};
use intent_core::{Error, Result, WorkspaceId};
use intent_pty::{PtyHost, PtyId, PtySize, RemotePty, RemoteSpec};
use intent_services::feature_api::feature_ports::HomeTerminalOpen;
use intent_services::Services;
use intent_transport::node_link::NodeLink;
use tokio::sync::mpsc;

use crate::connector::Shared;
use crate::RemoteServicesExt as _;

/// `error.data.code` of a home terminal the home's machine cannot run now.
pub(crate) const HOME_UNAVAILABLE: &str = "home-unavailable";

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One machine's home terminals.
#[derive(Default)]
pub(crate) struct HomeTerminals {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    terms: HashMap<String, Term>,
    /// Marks every terminal exited once the link-loss grace passes without
    /// a bind.
    grace: Option<tokio::task::JoinHandle<()>>,
}

struct Term {
    pty: Arc<PtyHost>,
    id: PtyId,
    /// The highest `seq` appended; output is taken only in order from 0.
    received: Option<u64>,
    size: Arc<Mutex<PtySize>>,
    /// Released here (`terminal.kill`, archive): its `ptyKill` is sent
    /// again after each bind until the machine reports the end.
    killed: bool,
    /// The home's name, for a start failure shown in the terminal.
    home_name: String,
}

impl Drop for HomeTerminals {
    /// The machine's connector is going away (it was removed or replaced):
    /// its terminals end here rather than stay open with no machine.
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(grace) = state.grace.take() {
            grace.abort();
        }
        for (_, term) in state.terms.drain() {
            let _ = term.pty.remote_exited(term.id, None);
        }
    }
}

impl HomeTerminals {
    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.state).terms.is_empty()
    }

    /// Append a `ptyOutput` that comes next in order, and acknowledge the
    /// highest contiguous `seq`. A resent frame is acknowledged again; one
    /// past a gap is not taken. A frame of a terminal released here is
    /// acknowledged too, so the machine can let it go.
    pub(crate) async fn output(&self, link: &NodeLink, frame: PtyOutput) {
        let ack = {
            let mut state = lock(&self.state);
            match state.terms.get_mut(&frame.terminal_id) {
                None => Some(frame.seq),
                Some(term) => {
                    let next = term.received.map_or(0, |seen| seen + 1);
                    if frame.seq == next {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(&frame.data)
                            .unwrap_or_else(|error| {
                                tracing::warn!(terminal = %frame.terminal_id, seq = frame.seq, %error, "remote machine: undecodable home terminal output dropped");
                                Vec::new()
                            });
                        term.received = Some(frame.seq);
                        if term.pty.push_remote_output(term.id, &bytes).is_err() {
                            state.terms.remove(&frame.terminal_id);
                        }
                        Some(frame.seq)
                    } else {
                        if frame.seq > next {
                            tracing::warn!(terminal = %frame.terminal_id, seq = frame.seq, expected = next, "remote machine: home terminal output out of order");
                        }
                        term.received
                    }
                }
            }
        };
        if let Some(seq) = ack {
            let _ = link
                .send(&LinkFrame::PtyAck(PtyAck {
                    terminal_id: frame.terminal_id,
                    seq,
                }))
                .await;
        }
    }

    /// A terminal ended on the machine. One that never started shows why.
    pub(crate) fn exited(&self, frame: &PtyExited) {
        let term = lock(&self.state).terms.remove(&frame.terminal_id);
        if let Some(term) = term {
            if let (Some(error), None) = (&frame.error, frame.exit_code) {
                let line = format!(
                    "\r\n[intent] the terminal on {} ended: {error}\r\n",
                    term.home_name
                );
                let _ = term.pty.push_remote_output(term.id, line.as_bytes());
            }
            let _ = term.pty.remote_exited(term.id, frame.exit_code);
        }
    }

    /// The terminal was released here: its process must end on the machine.
    fn killed(&self, terminal_id: &str) {
        if let Some(term) = lock(&self.state).terms.get_mut(terminal_id) {
            term.killed = true;
        }
    }

    /// A link bound: stop the grace, and ask after every terminal still
    /// held, so one the machine no longer runs is answered with its end.
    pub(crate) fn after_bind(&self, link: &NodeLink) {
        let probes: Vec<LinkFrame> = {
            let mut state = lock(&self.state);
            if let Some(grace) = state.grace.take() {
                grace.abort();
            }
            state
                .terms
                .iter()
                .map(|(terminal_id, term)| {
                    let terminal_id = terminal_id.clone();
                    if term.killed {
                        return LinkFrame::PtyKill(PtyKill { terminal_id });
                    }
                    term.received.map_or_else(
                        || {
                            let size = *lock(&term.size);
                            LinkFrame::PtyResize(PtyResize {
                                terminal_id: terminal_id.clone(),
                                cols: size.cols,
                                rows: size.rows,
                            })
                        },
                        |seq| {
                            LinkFrame::PtyAck(PtyAck {
                                terminal_id: terminal_id.clone(),
                                seq,
                            })
                        },
                    )
                })
                .collect()
        };
        if probes.is_empty() {
            return;
        }
        let link = link.clone();
        // caller-binding: allow — asks the machine after this backend's home terminals on one link
        tokio::spawn(async move {
            for probe in probes {
                if link.send(&probe).await.is_err() {
                    return;
                }
            }
        });
    }

    /// The link closed: the terminals end once `grace` passes without a
    /// bind (the machine ends their processes then too).
    pub(crate) fn link_lost(&self, shared: Weak<Shared>, grace: Duration) {
        let mut state = lock(&self.state);
        if state.terms.is_empty() {
            return;
        }
        if let Some(previous) = state.grace.take() {
            previous.abort();
        }
        // caller-binding: allow — ends this machine's home terminals after the link-loss grace
        state.grace = Some(tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let Some(shared) = shared.upgrade() else {
                return;
            };
            if shared.current_link().is_some() {
                return;
            }
            let ended: Vec<Term> = {
                let mut state = lock(&shared.terminals.state);
                state.grace = None;
                state.terms.drain().map(|(_, term)| term).collect()
            };
            for term in ended {
                let _ = term.pty.remote_exited(term.id, None);
            }
        }));
    }
}

/// Input, resizes and kills of one home terminal, sent in order on the
/// machine's current link.
struct LinkPty {
    shared: Weak<Shared>,
    terminal_id: OnceLock<String>,
    size: Arc<Mutex<PtySize>>,
    frames: mpsc::UnboundedSender<LinkFrame>,
    home: Box<intent_core::WorkspaceHome>,
}

impl LinkPty {
    fn id(&self) -> std::io::Result<String> {
        self.terminal_id
            .get()
            .cloned()
            .ok_or_else(|| std::io::Error::other("the terminal is not open yet"))
    }

    fn queue(&self, frame: LinkFrame) -> std::io::Result<()> {
        let linked = self
            .shared
            .upgrade()
            .is_some_and(|shared| shared.current_link().is_some());
        if !linked {
            return Err(std::io::Error::other(home_unavailable(
                &self.home,
                "its machine is not connected",
            )));
        }
        self.frames
            .send(frame)
            .map_err(|_| std::io::Error::other("the terminal is closed"))
    }
}

impl RemotePty for LinkPty {
    fn write(&self, data: &[u8]) -> std::io::Result<()> {
        self.queue(LinkFrame::PtyInput(PtyInput {
            terminal_id: self.id()?,
            data: base64::engine::general_purpose::STANDARD.encode(data),
        }))
    }

    fn resize(&self, size: PtySize) -> std::io::Result<()> {
        *lock(&self.size) = size;
        self.queue(LinkFrame::PtyResize(PtyResize {
            terminal_id: self.id()?,
            cols: size.cols,
            rows: size.rows,
        }))
    }

    fn kill(&self) {
        if let Ok(terminal_id) = self.id() {
            // Sent again after the next bind when the link is down now.
            if let Some(shared) = self.shared.upgrade() {
                shared.terminals.killed(&terminal_id);
            }
            let _ = self
                .frames
                .send(LinkFrame::PtyKill(PtyKill { terminal_id }));
        }
    }
}

fn home_unavailable(home: &intent_core::WorkspaceHome, detail: &str) -> Error {
    Error::remote_machine(
        true,
        format!(
            "{HOME_UNAVAILABLE}: the workspace's home {} is not reachable: {detail}",
            home.name
        ),
        serde_json::json!({
            "code": HOME_UNAVAILABLE,
            "nodeId": home.node_id,
            "cloudMachineId": home.cloud_machine_id,
            "name": home.name,
            "lastBackupAt": home.last_backup_at,
            "retryable": true,
        }),
    )
}

/// Dial `workspace_id`'s home machine ahead of a terminal start, outside
/// the workspace's archive fence; nothing when it has no home on one.
///
/// # Errors
///
/// `home-unavailable` when the home's machine cannot be reached.
pub(crate) async fn connect_home(svc: &Services, workspace_id: &WorkspaceId) -> Result<()> {
    if !svc.workspace_homes() {
        return Ok(());
    }
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(());
    };
    let Some(node_id) = home.node_id.clone() else {
        return Ok(());
    };
    let feature = svc
        .remote_feature()
        .ok_or_else(|| home_unavailable(&home, "no remote machine feature"))?;
    let node = feature
        .node(svc, &node_id)
        .await
        .map_err(|error| home_unavailable(&home, &error.to_string()))?;
    node.connect()
        .await
        .map_err(|error| home_unavailable(&home, &error.to_string()))
}

/// Start a terminal (or, in `exec` mode, a command) on `workspace_id`'s
/// home as a remote PTY in `pty`. `None` when the workspace has no home on
/// a machine, so the caller runs it here.
///
/// # Errors
///
/// `home-unavailable` when the home's machine cannot be reached or does
/// not speak nodeProtocol 19.
pub(crate) async fn open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    pty: &Arc<PtyHost>,
    open: HomeTerminalOpen,
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
    node.connect()
        .await
        .map_err(|error| home_unavailable(&home, &error.to_string()))?;
    let shared = node.shared_ref();
    if shared.protocol() < HOMES_PROTOCOL {
        return Err(home_unavailable(
            &home,
            "its machine needs an intentd update for workspace homes",
        ));
    }
    let size = PtySize {
        rows: open.rows,
        cols: open.cols,
    };
    let size_slot = Arc::new(Mutex::new(size));
    let (frames, mut queued) = mpsc::unbounded_channel::<LinkFrame>();
    let io = Arc::new(LinkPty {
        shared: shared.weak(),
        terminal_id: OnceLock::new(),
        size: size_slot.clone(),
        frames: frames.clone(),
        home: Box::new(home.clone()),
    });
    let mut spec = RemoteSpec::new(workspace_id.as_str());
    spec.size = size;
    spec.listed = open.mode == PtyMode::Terminal;
    spec.cwd.clone_from(&open.display_cwd);
    let id = pty.spawn_remote(spec, io.clone())?;
    let terminal_id = id.to_string();
    let _ = io.terminal_id.set(terminal_id.clone());
    lock(&shared.terminals.state).terms.insert(
        terminal_id.clone(),
        Term {
            pty: pty.clone(),
            id,
            received: None,
            size: size_slot,
            killed: false,
            home_name: home.name.clone(),
        },
    );
    // `ptyOpen` goes first on the terminal's own queue, so no input,
    // resize or kill a client sends meanwhile reaches the machine ahead of
    // it.
    let _ = frames.send(LinkFrame::PtyOpen(PtyOpen {
        terminal_id,
        workspace_id: workspace_id.clone(),
        home_id: home.home_id.clone(),
        mode: open.mode,
        cwd: open.cwd,
        command: open.command,
        env: open
            .env
            .into_iter()
            .map(|(key, value)| PtyEnvVar { key, value })
            .collect(),
        cols: Some(open.cols),
        rows: Some(open.rows),
    }));
    let forwarder = shared.weak();
    // caller-binding: allow — sends one home terminal's open, input, resizes and kill in order
    tokio::spawn(async move {
        while let Some(frame) = queued.recv().await {
            let Some(shared) = forwarder.upgrade() else {
                return;
            };
            let _ = shared.send(&frame).await;
        }
    });
    shared.wake();
    Ok(Some(id))
}
