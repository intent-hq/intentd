//! A short-lived provider adapter with no agent behind it, for importing
//! existing provider sessions (`agent.importableSessions.list` /
//! `agent.importSession`): it spawns the provider exactly as an agent turn
//! would (same binary resolution, Default-configuration overlay and
//! installed-CLI launch), runs the handshake, and is killed when done.
//!
//! No MCP bridge, rules file or permission handler is attached: the adapter
//! only answers `session/list`, `session/fork` and `session/load`.

use std::path::Path;
use std::sync::Arc;

use intent_acp::session_import::ImportCapabilities;
use intent_acp::spawn::{spawn_provider, SpawnOptions, SpawnedAgent};
use intent_acp::transport::{Connection, ConnectionHooks, IncomingNotification};
use intent_core::{Error, Result};
use tokio::sync::mpsc;

use super::{prepare_installed_launch, resolve_spawn_parts, AgentManager};

/// The adapter's process tree with what must outlive it: the npx launch dir
/// and the daemon-wide ephemeral-adapter slot (`agents.maxConcurrentAdapters`).
///
/// Cancellation-safe: dropping it without [`ProcessGuard::kill`] (a listing
/// timeout, a dropped RPC future) hands the tree to a background task that
/// runs the same process-group kill and descendant sweep
/// ([`SpawnedAgent::kill`]) and only then releases the dir and the slot.
struct ProcessGuard {
    spawned: Option<SpawnedAgent>,
    launch_root: Option<tempfile::TempDir>,
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl ProcessGuard {
    fn connection(&self) -> &Connection {
        self.spawned
            .as_ref()
            .expect("scratch adapter process is live until killed")
            .connection()
    }

    /// Kill the tree and release the dir and the slot in an independently
    /// owned task, then wait for it. Everything moves into the task before
    /// the first await, so cancelling the caller (a listing timeout firing
    /// during close) cannot interrupt the reap or release anything early.
    async fn kill(mut self) {
        let reap = intent_core::spawn_daemon(Self::reap(
            self.spawned.take(),
            self.launch_root.take(),
            self.slot.take(),
        ));
        if let Err(error) = reap.await {
            tracing::debug!(%error, "scratch adapter reap task failed");
        }
    }

    async fn reap(
        spawned: Option<SpawnedAgent>,
        launch_root: Option<tempfile::TempDir>,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        if let Some(mut spawned) = spawned {
            if let Err(error) = spawned.kill().await {
                tracing::debug!(%error, "scratch adapter kill failed");
            }
        }
        drop(launch_root);
        drop(slot);
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let (spawned, launch_root, slot) = (
            self.spawned.take(),
            self.launch_root.take(),
            self.slot.take(),
        );
        if spawned.is_none() {
            return;
        }
        match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                intent_core::spawn_daemon(Self::reap(spawned, launch_root, slot));
            }
            // No runtime: the child's kill-on-drop still ends the adapter.
            Err(_) => drop((spawned, launch_root, slot)),
        }
    }
}

/// A live adapter process with its handshake done.
pub(crate) struct ScratchAdapter {
    process: ProcessGuard,
    /// Every `session/update` (and other notification) the adapter sends,
    /// in order — the replay of a `session/load`.
    pub(crate) notifications: mpsc::UnboundedReceiver<IncomingNotification>,
    /// What the adapter advertised for listing, loading and forking.
    pub(crate) capabilities: ImportCapabilities,
    /// The Default configuration's settings fingerprint the process runs
    /// under (`None`: the provider has no overlay seam).
    pub(crate) fingerprint: Option<String>,
    /// The environment overlay the process runs under.
    pub(crate) overlay: Option<Arc<intent_acp::ProfileEnv>>,
}

impl ScratchAdapter {
    pub(crate) fn connection(&self) -> &Connection {
        self.process.connection()
    }

    /// Kill the adapter's process group and reap it. Dropping the adapter
    /// instead does the same in the background.
    pub(crate) async fn close(self) {
        self.process.kill().await;
    }
}

impl AgentManager {
    /// Spawn `provider_id` (a bare provider id; its Default configuration)
    /// in `cwd` and run the handshake. A failed handshake kills the process.
    pub(crate) async fn open_scratch_adapter(
        &self,
        provider_id: &str,
        cwd: &Path,
    ) -> Result<ScratchAdapter> {
        self.services
            .refuse_in_node_mode("starting a provider adapter")?;
        if !self.services.may_spawn_provider_locally() {
            return Err(Error::OrchestratorMode(
                "starting a provider adapter".into(),
            ));
        }
        let slot = crate::acp_adapter::adapter_slots()
            .acquire(std::time::Duration::from_secs(30))
            .await
            .ok_or_else(|| {
                Error::Internal(format!(
                    "{provider_id}: no free adapter slot (agents.maxConcurrentAdapters)"
                ))
            })?;
        let settings = self.services.effective_settings();
        let selection = crate::provider_profiles::canonical_selection(provider_id);
        let mut resolved = resolve_spawn_parts(
            provider_id,
            selection.clone(),
            None,
            None,
            cwd.to_path_buf(),
            &settings,
        )?;
        if resolved.provider.id == "auggie" && resolved.provider_binary.is_some() {
            let explicit = super::auggie_explicit_path_setting(&settings);
            let selected = tokio::task::spawn_blocking(move || {
                crate::auggie_cli::select_auggie_for_spawn(explicit.as_deref())
            })
            .await
            .map_err(|e| Error::Internal(format!("auggie version probe task failed: {e}")))??;
            resolved.provider_binary = Some(selected);
        }
        if let Some(npx) = resolved.npx_fallback_binary.clone() {
            crate::npx_cli::check_npx_version(&npx).await?;
        }
        let overlay = if crate::provider_profiles::may_have_overlay(&resolved.selection) {
            Box::pin(self.services.resolve_selected_profile(&resolved.selection))
                .await?
                .overlay
        } else {
            None
        };
        let fingerprint = if crate::provider_profiles::may_have_overlay(&resolved.selection) {
            Some(
                overlay
                    .as_ref()
                    .map(|o| o.fingerprint().to_owned())
                    .unwrap_or_default(),
            )
        } else {
            None
        };
        let launch_root = tempfile::Builder::new()
            .prefix(intent_core::NPX_LAUNCH_DIR_PREFIX)
            .tempdir()
            .ok();
        let mut opts = SpawnOptions::new(&resolved.provider);
        opts.profile_env = overlay.as_deref();
        opts.cwd = Some(&resolved.cwd);
        opts.provider_binary = resolved.provider_binary.as_deref();
        opts.npx_fallback_binary = resolved.npx_fallback_binary.as_deref();
        opts.npx_fallback_package = resolved.npx_fallback_package;
        opts.extra_env = resolved.extra_env.clone();
        opts.npx_launch_root = launch_root.as_ref().map(tempfile::TempDir::path);
        opts.node_max_old_space_mb = settings.agents.acp_node_max_old_space_mb;
        let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
        let hooks = ConnectionHooks {
            notifications: Some(note_tx),
            auth_error_patterns: opts
                .provider
                .auth_error_patterns
                .map(|p| p.iter().map(std::string::ToString::to_string).collect())
                .unwrap_or_default(),
            ..ConnectionHooks::default()
        };
        let spawned = if let Some(cli) =
            intent_providers::installed_cli::InstalledCli::for_provider(opts.provider.id)
        {
            let mut context =
                crate::installed_cli::InstalledContext::discover_for(cli, overlay.clone())
                    .await
                    .map_err(Error::InvalidInput)?;
            let mut prepared = prepare_installed_launch(&context, &opts)?;
            context
                .select_launch(&prepared.command, None)
                .await
                .map_err(|error| Error::InvalidInput(error.to_string()))?;
            context.apply_selection(&mut prepared.command);
            intent_acp::spawn::spawn_prepared_provider(&opts, prepared, hooks)
        } else {
            spawn_provider(&opts, hooks)
        }
        .map_err(|e| Error::Internal(format!("spawn provider failed: {e}")))?;
        // From here on the guard owns the tree, so a cancelled handshake
        // still kills it.
        let process = ProcessGuard {
            spawned: Some(spawned),
            launch_root,
            slot: Some(slot),
        };
        let handshake =
            intent_acp::handshake::handshake_raw(process.connection(), &resolved.provider).await;
        let (_, initialize) = match handshake {
            Ok(done) => done,
            Err(error) => {
                process.kill().await;
                return Err(Error::Internal(format!("{provider_id}: {error}")));
            }
        };
        Ok(ScratchAdapter {
            process,
            notifications: note_rx,
            capabilities: ImportCapabilities::from_initialize(&initialize),
            fingerprint,
            overlay,
        })
    }
}
