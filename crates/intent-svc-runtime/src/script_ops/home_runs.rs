//! Home runs on a workspace home (nodeProtocol 19, node-link.md § Home runs
//! and setup). On an orchestrator a workspace with a home runs its home
//! runs in the home checkout on the home's machine: the supervisor, state,
//! restarts, output log and monitors stay here unchanged, and only the
//! spawn differs: a remote PTY fed by the run's script frames
//! ([`RemoteLaunchPort::open_home_script`]) instead of a local one.
//!
//! [`RemoteLaunchPort::open_home_script`]: intent_services::feature_api::feature_ports::RemoteLaunchPort::open_home_script

use std::path::PathBuf;

use intent_core::{Error, Result, Script, WorkspaceId};
use intent_pty::{OutputSink, PtyId, SpawnSpec};
use intent_services::feature_api::feature_ports::HomeScriptOpen;

use super::{is_safe_relative, ScriptManager, SCRIPT_SCROLLBACK_BYTES};

/// How long a home run's stop waits for its machine to report the end,
/// as a machine run's (`machine_runs::Hooks::stop_wait`).
const HOME_STOP_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Where a run's processes run.
#[derive(Debug, Clone)]
pub(super) enum RunTarget {
    /// Here, in the workspace's worktree (or `cwd` inside it).
    Local(Option<PathBuf>),
    /// In the home checkout on the workspace's home, `cwd` relative to it.
    Home { cwd: Option<String> },
}

/// One process to spawn for a run.
pub(super) enum Launch {
    Local(SpawnSpec),
    Home(HomeScriptOpen),
}

impl Launch {
    pub(super) fn set_output_sink(&mut self, sink: Option<OutputSink>) {
        match self {
            Self::Local(spec) => spec.output_sink = sink,
            Self::Home(open) => open.output_sink = sink,
        }
    }
}

impl ScriptManager {
    /// Whether `ws`'s home runs run on its home: an orchestrator serving
    /// homes, with the workspace's home on a machine.
    async fn runs_on_home(&self, ws: &WorkspaceId) -> Result<bool> {
        let Some(svc) = &self.owner_services else {
            return Ok(false);
        };
        if !svc.workspace_homes() {
            return Ok(false);
        }
        Ok(svc
            .store()
            .get_workspace_home(ws)
            .await?
            .is_some_and(|home| home.node_id.is_some()))
    }

    /// Where `def`'s run in `ws` runs.
    ///
    /// # Errors
    ///
    /// A `cwd` that leaves the checkout; the store's error.
    pub(super) async fn resolve_target(&self, ws: &WorkspaceId, def: &Script) -> Result<RunTarget> {
        if !self.runs_on_home(ws).await? {
            return Ok(RunTarget::Local(self.resolve_cwd(ws, def).await?));
        }
        let cwd = def.cwd.clone().filter(|cwd| !cwd.is_empty());
        if let Some(rel) = cwd.as_deref().filter(|rel| !is_safe_relative(rel)) {
            return Err(Error::Internal(format!(
                "Script \"{}\" cwd escapes workspace root: {rel}",
                def.name
            )));
        }
        Ok(RunTarget::Home { cwd })
    }

    /// The process to spawn for `def` at `target`.
    pub(super) fn launch_for(&self, ws: &WorkspaceId, def: &Script, target: &RunTarget) -> Launch {
        match target {
            RunTarget::Local(cwd) => Launch::Local(self.build_spec(ws, def, cwd.as_ref())),
            // The machine applies the saved-script environment of its own
            // host; only the saved variables travel.
            RunTarget::Home { cwd } => Launch::Home(HomeScriptOpen {
                script_id: def.id.clone(),
                command: def.command.clone(),
                mode: def.mode,
                cwd: cwd.clone(),
                env: def
                    .env
                    .iter()
                    .flatten()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                scrollback_bytes: SCRIPT_SCROLLBACK_BYTES,
                output_sink: None,
            }),
        }
    }

    /// Spawn `launch` for `ws`.
    ///
    /// # Errors
    ///
    /// The PTY host's error; `home-unavailable` when the home's machine
    /// cannot run it now.
    pub(super) async fn spawn_launch(&self, ws: &WorkspaceId, launch: Launch) -> Result<PtyId> {
        match launch {
            Launch::Local(spec) => self.pty.spawn(spec),
            Launch::Home(open) => {
                let svc = self.owner_services.as_ref().ok_or_else(|| {
                    Error::Internal("a home run needs the daemon's services".into())
                })?;
                let id = svc
                    .feature_remote_launch_port()
                    .open_home_script(svc, ws, &self.pty, open)
                    .await?
                    .ok_or_else(|| {
                        Error::Internal(format!("workspace {ws} no longer has a home"))
                    })?;
                self.home_ptys.lock().unwrap().insert(id);
                Ok(id)
            }
        }
    }

    /// Before `pty_id` is killed: when it is a home run, stop it on the
    /// machine and wait (bounded) for the end, as a machine run's stop
    /// does, so a restart never overlaps the last process.
    pub(super) async fn stop_home_run(&self, ws: &WorkspaceId, pty_id: PtyId) {
        if !self.home_ptys.lock().unwrap().contains(&pty_id) {
            return;
        }
        let Some(svc) = &self.owner_services else {
            return;
        };
        let ended = svc
            .feature_remote_launch_port()
            .stop_home_script(svc, ws, &self.pty, pty_id, HOME_STOP_WAIT)
            .await;
        if !ended {
            tracing::warn!(workspace = %ws, "a home run's stop was not confirmed in time; releasing it");
        }
    }

    /// Dial `ws`'s home machine ahead of a start, outside every script
    /// lock, so a start fails `home-unavailable` at once and no lock waits
    /// on a dial; nothing for a workspace without a home on a machine.
    ///
    /// # Errors
    ///
    /// `home-unavailable` when the home's machine cannot be reached.
    pub(super) async fn connect_home(&self, ws: &WorkspaceId) -> Result<()> {
        let Some(svc) = &self.owner_services else {
            return Ok(());
        };
        svc.feature_remote_launch_port().connect_home(svc, ws).await
    }

    /// The workspace's home made a new checkout and finished its setup:
    /// start the home run of every saved `autoStart` script (nodes.md
    /// "Workspace homes"). Returns how many started; one failure does not
    /// keep the others from starting.
    ///
    /// # Errors
    ///
    /// The store's error listing the scripts.
    pub(crate) async fn start_auto_start(&self, ws: &WorkspaceId) -> Result<usize> {
        // Listing first registers the repository's configured scripts, as
        // the first list of a new workspace does.
        self.list(ws).await?;
        let ids: Vec<String> = self
            .scripts
            .lock()
            .unwrap()
            .iter()
            .filter(|((workspace, _), m)| {
                workspace == ws
                    && m.def.auto_start == Some(true)
                    && m.def.archived_at.is_none()
                    && m.def.purpose == intent_core::ScriptPurpose::Saved
            })
            .map(|((_, id), _)| id.clone())
            .collect();
        let mut started = 0;
        for id in ids {
            match self.start(ws, &id).await {
                Ok(_) => started += 1,
                Err(error) => {
                    tracing::warn!(workspace = %ws, script = %id, %error, "home autoStart script did not start");
                }
            }
        }
        Ok(started)
    }
}
