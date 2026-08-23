//! microVM orchestrator: per-agent libkrun VM lifecycle with the agent's ACP
//! stream bridged over vsock (monorepo#1120, EE-5).
//!
//! One VM per agent. Each VM boots the resolved guest image
//! ([`crate::sandbox_image`]) from a per-VM `CoW` clone of the extracted rootfs
//! tree, virtio-fs-mounts the agent's own `CoW` workspace clone
//! ([`crate::sandbox_ops`]) at `/workspace`, and runs the provider through the
//! image's `intent-exec/1` vsock exec agent. The resulting byte stream is
//! handed to the existing [`intent_acp::Connection`] machinery unchanged —
//! provider stdin is a pipe on the guest side, never a TTY.
//!
//! Module layout:
//! - [`exec`] — `intent-exec/1` client over the helper's forwarded unix socket
//! - [`rootfs`] — rootfs archive extraction cache + per-VM `CoW` clone
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
/// (`<data_dir>/microvm/<agent-id>/{rootfs,console.log}`). The exec socket
/// deliberately lives elsewhere — see [`exec_sock_path`].
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
#[must_use]
pub fn vm_dir(data_dir: &Path, agent_id: &str) -> PathBuf {
    data_dir.join(MICROVM_STATE_DIR).join(agent_id)
}

/// Unix socket paths must fit `sockaddr_un.sun_path`: 104 bytes on macOS
/// (`SUN_LEN`), 108 on Linux. Keep the conservative bound.
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

/// Short host rendezvous path for the per-VM exec socket. It deliberately
/// does NOT live in the per-VM state dir: a deep data dir pushes
/// `<vm_dir>/exec.sock` past the `SUN_LEN` limit and both libkrun's bind and
/// the daemon's connect reject it. The socket sits in a private per-user
/// 0700 directory under the system temp dir instead, named by a sha256
/// prefix of the agent id — deterministic (the pre-boot stale scrub finds
/// it again), collision-free across agents, and short regardless of
/// data-dir depth.
///
/// Security: the socket grants arbitrary command execution inside the guest,
/// so the rendezvous path must not be attackable. macOS `$TMPDIR` is already
/// per-user 0700; the extra `intentd-vm-<uid>` parent (created/verified by
/// [`ensure_private_dir`]) covers hosts where the temp dir is shared, and
/// the `/tmp` length fallback never uses bare world-writable `/tmp`.
///
/// # Errors
///
/// Returns `MicrovmError::Boot` when no candidate path fits the socket
/// length limit or the private parent directory cannot be secured.
#[cfg(unix)]
pub fn exec_sock_path(agent_id: &str) -> Result<PathBuf, MicrovmError> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let digest = Sha256::digest(agent_id.as_bytes());
    let hash: String = digest.iter().take(6).fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    });
    let file = format!("{hash}.sock");
    let uid = unsafe { libc::getuid() };
    let parent_name = format!("intentd-vm-{uid}");
    let mut parent = std::env::temp_dir().join(&parent_name);
    if parent.as_os_str().len() + 1 + file.len() > MAX_SOCKET_PATH_BYTES {
        // $TMPDIR itself can be pathological; fall back to a private parent
        // under /tmp — never bare /tmp.
        parent = PathBuf::from("/tmp").join(&parent_name);
    }
    let path = parent.join(&file);
    if path.as_os_str().len() > MAX_SOCKET_PATH_BYTES {
        return Err(MicrovmError::Boot(format!(
            "exec socket path exceeds the {MAX_SOCKET_PATH_BYTES}-byte unix socket limit \
             (SUN_LEN): {}",
            path.display()
        )));
    }
    ensure_private_dir(&parent, uid)?;
    Ok(path)
}

/// Create (or adopt) `dir` as a private 0700 directory owned by `uid`.
/// Refuses symlinks, non-directories, foreign-owned paths, and group/other
/// mode bits — an attacker pre-creating the well-known path must not gain
/// access to the sockets, and a dir that was ever group/other-accessible may
/// already hold planted entries, so it is rejected rather than re-tightened.
#[cfg(unix)]
fn ensure_private_dir(dir: &Path, uid: u32) -> Result<(), MicrovmError> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(MicrovmError::Io(format!(
                "create exec socket dir {}: {e}",
                dir.display()
            )))
        }
    }
    let meta = std::fs::symlink_metadata(dir)
        .map_err(|e| MicrovmError::Io(format!("stat exec socket dir {}: {e}", dir.display())))?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return Err(MicrovmError::Boot(format!(
            "exec socket dir {} exists but is not a directory (possible squat); \
             remove it and retry",
            dir.display()
        )));
    }
    if meta.uid() != uid {
        return Err(MicrovmError::Boot(format!(
            "exec socket dir {} is owned by uid {}, not uid {uid} (possible squat); \
             remove it and retry",
            dir.display(),
            meta.uid()
        )));
    }
    if meta.permissions().mode() & 0o077 != 0 {
        return Err(MicrovmError::Boot(format!(
            "exec socket dir {} has group/other permission bits (mode {:o}); \
             remove it and retry",
            dir.display(),
            meta.permissions().mode() & 0o777
        )));
    }
    Ok(())
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn exec_sock_path_is_short_even_with_pathological_data_dir() {
        // Regression: `<vm_dir>/exec.sock` under a deep data dir exceeded the
        // SUN_LEN limit and both bind and connect rejected it.
        let data_dir = PathBuf::from(format!("/{}", "x".repeat(200)));
        let vm = vm_dir(&data_dir, "agent-a6dc9494-a2cd-4988-b0df-79093cb67028");
        let agent_id = vm.file_name().unwrap().to_str().unwrap();
        let sock = exec_sock_path(agent_id).unwrap();
        assert!(
            sock.as_os_str().len() <= MAX_SOCKET_PATH_BYTES,
            "exec socket path is {} bytes: {}",
            sock.as_os_str().len(),
            sock.display()
        );
        assert!(!sock.starts_with(&data_dir));
    }

    #[test]
    fn exec_sock_path_is_distinct_per_agent_and_deterministic() {
        let a = exec_sock_path("agent-11111111-2222-3333-4444-555555555555").unwrap();
        let b = exec_sock_path("agent-11111111-2222-3333-4444-555555555556").unwrap();
        assert_ne!(a, b);
        assert_eq!(
            a,
            exec_sock_path("agent-11111111-2222-3333-4444-555555555555").unwrap()
        );
    }

    #[test]
    fn exec_sock_parent_dir_is_private_0700() {
        use std::os::unix::fs::PermissionsExt;
        let sock = exec_sock_path("agent-11111111-2222-3333-4444-555555555557").unwrap();
        let mode = std::fs::metadata(sock.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn ensure_private_dir_creates_0700_and_is_idempotent() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let uid = unsafe { libc::getuid() };
        let dir = tmp.path().join("sockdir");
        ensure_private_dir(&dir, uid).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
        // Adopting an existing correct dir succeeds.
        ensure_private_dir(&dir, uid).unwrap();
        // Group/other bits are a hard error, not silently fixed.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = ensure_private_dir(&dir, uid).unwrap_err();
        assert!(err.to_string().contains("group/other"));
    }

    #[test]
    fn ensure_private_dir_rejects_squats() {
        let tmp = tempfile::tempdir().unwrap();
        let uid = unsafe { libc::getuid() };
        // A plain file at the path is refused.
        let file = tmp.path().join("filesquat");
        std::fs::write(&file, b"x").unwrap();
        assert!(ensure_private_dir(&file, uid).is_err());
        // A symlink — even to a directory we own — is refused.
        let real = tmp.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = tmp.path().join("linksquat");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(ensure_private_dir(&link, uid).is_err());
        // A directory owned by a different uid is refused.
        assert!(ensure_private_dir(&real, uid.wrapping_add(1)).is_err());
    }
}
