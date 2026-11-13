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
    /// The highest `seq` appended.
    received: Option<u64>,
    size: Arc<Mutex<PtySize>>,
}

impl HomeTerminals {
    pub(crate) fn is_empty(&self) -> bool {
        lock(&self.state).terms.is_empty()
    }

    /// Append a `ptyOutput` and acknowledge it. A frame of a terminal
    /// released here is acknowledged too, so the machine can let it go.
    pub(crate) async fn output(&self, link: &NodeLink, frame: PtyOutput) {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&frame.data)
            .unwrap_or_default();
        {
            let mut state = lock(&self.state);
            let released = match state.terms.get_mut(&frame.terminal_id) {
                Some(term) if term.received.is_none_or(|seen| frame.seq > seen) => {
                    term.received = Some(frame.seq);
                    term.pty.push_remote_output(term.id, &bytes).is_err()
                }
                _ => false,
            };
            if released {
                state.terms.remove(&frame.terminal_id);
            }
        }
        let _ = link
            .send(&LinkFrame::PtyAck(PtyAck {
                terminal_id: frame.terminal_id,
                seq: frame.seq,
            }))
            .await;
    }

    /// A terminal ended on the machine.
    pub(crate) fn exited(&self, frame: &PtyExited) {
        let term = lock(&self.state).terms.remove(&frame.terminal_id);
        if let Some(term) = term {
            if let Some(error) = &frame.error {
                tracing::debug!(terminal = %frame.terminal_id, %error, "remote machine: home terminal ended");
            }
            let _ = term.pty.remote_exited(term.id, frame.exit_code);
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
            return Err(std::io::Error::other(format!(
                "{HOME_UNAVAILABLE}: the workspace's home machine is not connected"
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
        frames,
    });
    let mut spec = RemoteSpec::new(workspace_id.as_str());
    spec.size = size;
    spec.listed = open.mode == PtyMode::Terminal;
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
        },
    );
    let forwarder = shared.weak();
    // caller-binding: allow — sends one home terminal's input, resizes and kill in order
    tokio::spawn(async move {
        while let Some(frame) = queued.recv().await {
            let Some(shared) = forwarder.upgrade() else {
                return;
            };
            let _ = shared.send(&frame).await;
        }
    });
    let frame = LinkFrame::PtyOpen(PtyOpen {
        terminal_id: terminal_id.clone(),
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
    });
    if let Err(error) = shared.send(&frame).await {
        lock(&shared.terminals.state).terms.remove(&terminal_id);
        pty.kill(id).await;
        return Err(home_unavailable(&home, &error.to_string()));
    }
    shared.wake();
    Ok(Some(id))
}
