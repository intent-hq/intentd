//! microVM orchestrator: per-agent libkrun VM lifecycle with the agent's ACP
//! stream bridged over vsock (monorepo#1120, EE-5).
//!
//! One VM per agent. Each VM boots the resolved guest image
//! ([`crate::sandbox_image`]) from a per-VM CoW clone of the extracted rootfs
//! tree, virtio-fs-mounts the agent's own CoW workspace clone
//! ([`crate::sandbox_ops`]) at `/workspace`, and runs the provider through the
//! image's `intent-exec/1` vsock exec agent. The resulting byte stream is
//! handed to the existing [`intent_acp::Connection`] machinery unchanged —
//! provider stdin is a pipe on the guest side, never a TTY.
//!
//! Module layout:
//! - [`exec`] — `intent-exec/1` client over the helper's forwarded unix socket
//! - [`rootfs`] — rootfs archive extraction cache + per-VM CoW clone
//! - [`auth`] — credential staging into the guest home, rotation watcher,
//!   teardown scrub (the per-VM directory removal deletes every staged copy)
//! - [`orchestrator`] — helper boot, guest setup, provider exec, teardown
//!
//! The backend is unix-only (macOS Apple Silicon + Linux/KVM). On other
//! targets the submodules are compiled out and [`orchestrator`] is a stub
//! whose entry points report microVM as unsupported — mirroring the
//! intentd-microvm-helper non-macOS stub (exit `EXIT_UNAVAILABLE`);
//! `microvm_platform_supported()` already reports the capability as false.

#[cfg(unix)]
pub mod auth;
#[cfg(unix)]
pub mod exec;
#[cfg(unix)]
pub mod orchestrator;
#[cfg(unix)]
pub mod rootfs;

use std::path::{Path, PathBuf};

pub use orchestrator::{MicrovmSpawnSpec, MicrovmVm};

/// Subdirectory of the daemon data dir holding per-VM state
/// (`<data_dir>/microvm/<agent-id>/{rootfs,exec.sock,console.log}`).
pub const MICROVM_STATE_DIR: &str = "microvm";

/// Guest-side directory (inside the per-VM rootfs) where the daemon stages
/// spawn material: rules file, MCP config + bridge script, provider stderr
/// log. Host-visible as `<vm rootfs>/intent/`.
pub const GUEST_INTENT_DIR: &str = "intent";

/// The vsock port the guest exec agent listens on is taken from the image
/// manifest (`vsockExec.port`); this is the guest mountpoint of the workspace
/// virtio-fs share and its tag.
pub const WORKSPACE_VIRTIOFS_TAG: &str = "work";
/// Guest mountpoint for the workspace share.
pub const GUEST_WORKSPACE_DIR: &str = "/workspace";

/// Per-VM state directory for an agent.
pub fn vm_dir(data_dir: &Path, agent_id: &str) -> PathBuf {
    data_dir.join(MICROVM_STATE_DIR).join(agent_id)
}

/// Errors from the microVM orchestrator, mapped onto [`intent_core::Error`]
/// at the service boundary. Backend failure is a hard spawn error — there is
/// no silent local fallback (spec: settled decisions).
#[derive(Debug, thiserror::Error)]
pub enum MicrovmError {
    #[error("microVM helper binary not found: {0}")]
    HelperMissing(String),
    #[error("rootfs extraction failed: {0}")]
    Extract(String),
    #[error("per-VM rootfs clone failed: {0}")]
    RootfsClone(String),
    #[error("credential staging failed: {0}")]
    AuthStage(String),
    #[error("microVM boot failed: {0}")]
    Boot(String),
    #[error("guest setup failed: {0}")]
    GuestSetup(String),
    #[error("guest exec protocol error: {0}")]
    Exec(String),
    #[error("I/O error: {0}")]
    Io(String),
    /// Constructed only by the non-unix [`orchestrator`] stub.
    #[error("{0}")]
    Unsupported(String),
}

impl From<MicrovmError> for intent_core::Error {
    fn from(e: MicrovmError) -> Self {
        intent_core::Error::Internal(format!("microvm: {e}"))
    }
}

impl From<std::io::Error> for MicrovmError {
    fn from(e: std::io::Error) -> Self {
        MicrovmError::Io(e.to_string())
    }
}

/// Non-unix stub of the unix `orchestrator` module: the same public surface
/// so call sites (agent_manager's microVM spawn path) compile unchanged, but
/// every entry point fails with [`MicrovmError::Unsupported`]. The runtime
/// never reaches this path — `microvm_platform_supported()` is false here,
/// so `workspace.create` rejects `executionEnvironment: "microvm"` long
/// before an agent spawn — and `resolve_helper_exe` failing first maps the
/// error onto the structured `ExecutionEnvironmentUnavailable` at the spawn
/// gate regardless.
#[cfg(not(unix))]
pub mod orchestrator {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use intent_core::{AgentId, WorkspaceId};
    use tokio::io::AsyncRead;
    use tokio::process::Child;

    use super::MicrovmError;
    use crate::sandbox_image::CachedImage;

    /// Guest path of the staged MCP bridge script (mirrors the unix module).
    pub const GUEST_MCP_BRIDGE_PATH: &str = "/intent/mcp-bridge.mjs";

    fn unsupported() -> MicrovmError {
        MicrovmError::Unsupported(
            "microVM sandboxes are not supported on this operating system".to_string(),
        )
    }

    /// Always unsupported on non-unix hosts.
    pub fn resolve_helper_exe() -> Result<PathBuf, MicrovmError> {
        Err(unsupported())
    }

    /// See the unix module; carried only so call sites compile.
    pub struct MicrovmSpawnSpec {
        pub vm_dir: PathBuf,
        pub helper_exe: PathBuf,
        pub image: CachedImage,
        pub workspace_dir: PathBuf,
        pub host_home: PathBuf,
        pub stage_claude_onboarding: bool,
        pub vcpus: u8,
        pub mem_mib: u32,
    }

    /// Stub VM handle: [`MicrovmVm::boot`] always fails, so no instance
    /// exists at runtime on non-unix hosts.
    pub struct MicrovmVm {
        pub child: Option<Child>,
        pub boot_ms: u64,
        pub stop_event: Option<(crate::events::EventBus, WorkspaceId, AgentId)>,
    }

    impl MicrovmVm {
        pub async fn boot(_spec: &MicrovmSpawnSpec) -> Result<Self, MicrovmError> {
            Err(unsupported())
        }

        pub async fn guest_setup(&self) -> Result<(), MicrovmError> {
            Err(unsupported())
        }

        pub async fn stage_mcp_bridge(&self) -> Result<(), MicrovmError> {
            Err(unsupported())
        }

        pub async fn stage_intent_file(
            &self,
            _name: &str,
            _content: &[u8],
        ) -> Result<String, MicrovmError> {
            Err(unsupported())
        }

        pub async fn start_provider(
            &self,
            _argv: &[String],
            _env: &BTreeMap<String, String>,
        ) -> Result<
            (
                tokio::io::DuplexStream,
                tokio::io::DuplexStream,
                Box<dyn AsyncRead + Unpin + Send>,
            ),
            MicrovmError,
        > {
            Err(unsupported())
        }

        pub fn take_child(&mut self) -> Option<Child> {
            self.child.take()
        }
    }
}
