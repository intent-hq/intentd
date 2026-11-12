//! Local daemon discovery record (protocol §5.58 "Discovery record").
//!
//! Each running daemon publishes one small JSON file in a per-OS-user folder
//! so another Intent run by the same user can list it for local code pairing.
//! The record carries no secret: the port, the TLS fingerprint, a display
//! label and the data folder.
//!
//! Lifecycle:
//! - [`Publisher::start`] sweeps records whose `pid` is no longer running,
//!   then writes this daemon's record (replacing a leftover from a crash).
//! - The publisher re-reads the live fields once a second and rewrites the
//!   record when one changes (listener start/stop, port, fingerprint,
//!   display name) or when the file went missing.
//! - [`Publisher::shutdown`] stops the loop and deletes the record on clean
//!   exit. A crash leaves the record; readers skip it because its `pid` is
//!   not running, and the next daemon start sweeps it.
//!
//! Safety: the folder is created owner-only (`0700`) and each file `0600`.
//! The writer refuses a folder that is a symlink, is not a directory, or is
//! owned by another user, and writes atomically (temporary file in the same
//! folder, then rename), so a pre-planted symlink at the record path is
//! replaced, never followed.

use std::future::Future;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Environment override for the discovery folder (writers and readers).
pub const DIR_ENV: &str = "INTENT_LOCAL_DAEMONS_DIR";

/// Environment override for the install's product name, used as the
/// `displayName` fallback when `sharing.machineName` is unset. The desktop
/// app sets it for its sidecar (for example `Intent Alpha`).
pub const PRODUCT_NAME_ENV: &str = "INTENTD_PRODUCT_NAME";

/// File in the daemon's data folder holding its stable `instanceId`.
pub const INSTANCE_ID_FILE: &str = "local-instance-id";

/// Whether this daemon implements the whole of §5.58 (the five methods, the
/// `/local-pair` endpoint, the credential, the events and this record). The
/// record then carries `capabilities.localCodePairing: 1`, matching the
/// `client.hello` / `system.capabilities` advertisement
/// ([`intent_core::local_pairing::CAPABILITY_VERSION`]).
pub const LOCAL_CODE_PAIRING_SUPPORTED: bool = true;

/// Largest record readers accept (and the sweep considers).
pub const MAX_RECORD_BYTES: u64 = 16 * 1024;

/// How often the publisher re-reads the live fields.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// `capabilities` block of the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// Exactly `1` when local code pairing is implemented; omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_code_pairing: Option<u8>,
}

impl Capabilities {
    /// This build's advertised capabilities.
    #[must_use]
    pub fn current() -> Self {
        Self {
            local_code_pairing: LOCAL_CODE_PAIRING_SUPPORTED
                .then_some(intent_core::local_pairing::CAPABILITY_VERSION),
        }
    }
}

/// The on-disk `LocalDaemonRecord` (§5.58).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalDaemonRecord {
    pub version: u32,
    pub instance_id: String,
    pub pid: u32,
    pub started_at: String,
    pub updated_at: String,
    pub display_name: String,
    pub data_dir: String,
    pub daemon_version: String,
    pub protocol_version: String,
    pub port: Option<u16>,
    pub fingerprint: String,
    pub capabilities: Capabilities,
}

/// The fields that change while the daemon runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFields {
    /// WSS port, `None` while the TCP listener is down.
    pub port: Option<u16>,
    /// Fingerprint of the live listener's certificate, when it is up. The
    /// publisher falls back to the boot certificate's fingerprint.
    pub fingerprint: Option<String>,
    /// `sharing.machineName`, when set.
    pub machine_name: Option<String>,
}

/// Source of [`LiveFields`], implemented by the daemon composition root.
pub trait LiveSource: Send + Sync + 'static {
    fn live(&self) -> Pin<Box<dyn Future<Output = LiveFields> + Send + '_>>;
}

/// The fields fixed for the daemon's lifetime.
#[derive(Debug, Clone)]
pub struct StaticFields {
    pub instance_id: String,
    pub pid: u32,
    pub started_at: String,
    pub data_dir: PathBuf,
    pub daemon_version: String,
    pub protocol_version: String,
    /// Fingerprint of the certificate loaded at boot.
    pub fingerprint: String,
    /// `displayName` when `sharing.machineName` is unset.
    pub product_name: String,
}

impl StaticFields {
    fn record(&self, live: &LiveFields) -> LocalDaemonRecord {
        LocalDaemonRecord {
            version: 1,
            instance_id: self.instance_id.clone(),
            pid: self.pid,
            started_at: self.started_at.clone(),
            updated_at: intent_core::clock::now_iso(),
            display_name: live
                .machine_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or(&self.product_name)
                .to_string(),
            data_dir: self.data_dir.to_string_lossy().into_owned(),
            daemon_version: self.daemon_version.clone(),
            protocol_version: self.protocol_version.clone(),
            port: live.port,
            fingerprint: live
                .fingerprint
                .clone()
                .unwrap_or_else(|| self.fingerprint.clone()),
            capabilities: Capabilities::current(),
        }
    }
}

/// The install's product name: [`PRODUCT_NAME_ENV`] when set, else
/// `Intent (dev build)` for debug builds and `Intent` for release builds.
#[must_use]
pub fn product_name() -> String {
    product_name_from(std::env::var(PRODUCT_NAME_ENV).ok().as_deref())
}

fn product_name_from(env: Option<&str>) -> String {
    match env.map(str::trim) {
        Some(name) if !name.is_empty() && !name.chars().any(char::is_control) => {
            name.chars().take(100).collect()
        }
        _ if cfg!(debug_assertions) => "Intent (dev build)".to_string(),
        _ => "Intent".to_string(),
    }
}

/// Resolve the discovery folder for this OS user, honoring [`DIR_ENV`].
/// `None` when no home folder can be determined.
#[must_use]
pub fn resolve_dir() -> Option<PathBuf> {
    resolve_dir_from(&|name| std::env::var_os(name), home_dir().as_deref())
}

fn home_dir() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|dirs| dirs.home_dir().to_path_buf())
}

fn resolve_dir_from(
    env: &dyn Fn(&str) -> Option<std::ffi::OsString>,
    home: Option<&Path>,
) -> Option<PathBuf> {
    let non_empty = |name: &str| env(name).filter(|value| !value.is_empty());
    if let Some(dir) = non_empty(DIR_ENV) {
        return Some(PathBuf::from(dir));
    }
    if cfg!(target_os = "macos") {
        return home.map(|home| {
            home.join("Library")
                .join("Application Support")
                .join("Intent")
                .join("local-daemons")
        });
    }
    if cfg!(windows) {
        let base = non_empty("LOCALAPPDATA")
            .map(PathBuf::from)
            .or_else(|| home.map(|home| home.join("AppData").join("Local")))?;
        return Some(base.join("Intent").join("local-daemons"));
    }
    if let Some(runtime) = non_empty("XDG_RUNTIME_DIR") {
        return Some(PathBuf::from(runtime).join("intent").join("local-daemons"));
    }
    home.map(|home| {
        home.join(".local")
            .join("state")
            .join("intent")
            .join("local-daemons")
    })
}

/// Read the daemon's stable `instanceId` from its data folder, creating it
/// once. One data folder always maps to one record file.
///
/// # Errors
/// Reading or writing the id file fails.
pub fn load_or_create_instance_id(data_dir: &Path) -> std::io::Result<String> {
    let path = data_dir.join(INSTANCE_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let id = text.trim();
            if uuid::Uuid::parse_str(id).is_ok() {
                return Ok(id.to_string());
            }
            tracing::warn!(path = %path.display(), "replacing malformed local instance id");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let id = uuid::Uuid::new_v4().to_string();
    let tmp = data_dir.join(format!(".{INSTANCE_ID_FILE}.tmp"));
    write_private_file(&tmp, format!("{id}\n").as_bytes())?;
    std::fs::rename(&tmp, &path)?;
    Ok(id)
}

/// Create the discovery folder owner-only, or verify an existing one.
///
/// # Errors
/// The folder cannot be created, or exists but is a symlink, not a
/// directory, or owned by another user.
pub fn ensure_dir(dir: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(dir) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = dir.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                builder.mode(0o700);
            }
            match builder.create(dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    }
    let metadata = std::fs::symlink_metadata(dir)?;
    if !metadata.file_type().is_dir() {
        return Err(std::io::Error::other(format!(
            "{} is not a real directory",
            dir.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != current_uid() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("{} is owned by another user", dir.display()),
            ));
        }
        if metadata.mode() & 0o777 != 0o700 {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn current_uid() -> u32 {
    nix::unistd::geteuid().as_raw()
}

/// Path of the record for `instance_id` in `dir`.
#[must_use]
pub fn record_path(dir: &Path, instance_id: &str) -> PathBuf {
    dir.join(format!("{instance_id}.json"))
}

/// Create `path` exclusively with mode `0600` and write `bytes`, synced.
fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Atomically write `record` into `dir` (which [`ensure_dir`] has checked).
///
/// # Errors
/// Serializing, writing or renaming fails.
pub fn write_record(dir: &Path, record: &LocalDaemonRecord) -> std::io::Result<()> {
    let bytes = serde_json::to_vec_pretty(record).map_err(std::io::Error::other)?;
    let tmp = dir.join(format!(".{}.json.tmp", record.instance_id));
    write_private_file(&tmp, &bytes)?;
    if let Err(error) = std::fs::rename(&tmp, record_path(dir, &record.instance_id)) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    Ok(())
}

/// Delete this daemon's record. A missing file is not an error.
///
/// # Errors
/// Removing the file fails for a reason other than absence.
pub fn remove_record(dir: &Path, instance_id: &str) -> std::io::Result<()> {
    match std::fs::remove_file(record_path(dir, instance_id)) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// Whether a process with `pid` is running (signal-0 probe on Unix).
#[must_use]
pub fn pid_is_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(raw) = i32::try_from(pid) else {
            return false;
        };
        if raw <= 0 {
            return false;
        }
        matches!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None),
            Ok(()) | Err(nix::errno::Errno::EPERM)
        )
    }
    #[cfg(not(unix))]
    {
        use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
        let pid = Pid::from_u32(pid);
        let mut system = System::new();
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing(),
        );
        system.process(pid).is_some()
    }
}

/// Read a record the way readers do: a regular file (not a symlink), at most
/// [`MAX_RECORD_BYTES`], owned by the current user, that parses. `None`
/// otherwise.
#[must_use]
pub fn read_record(path: &Path) -> Option<LocalDaemonRecord> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_RECORD_BYTES {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.uid() != current_uid() {
            return None;
        }
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).ok()?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(
        &mut std::io::Read::take(file, MAX_RECORD_BYTES + 1),
        &mut bytes,
    )
    .ok()?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return None;
    }
    serde_json::from_slice(&bytes).ok()
}

/// Delete other daemons' records whose `pid` is not running. Never deletes
/// a live daemon's record, this daemon's own record, or any file a reader
/// would skip (symlinks, foreign or unparseable files). Returns the number
/// of records removed.
pub fn sweep_stale(dir: &Path, own_instance_id: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Some(record) = read_record(&path) else {
            continue;
        };
        if record.instance_id == own_instance_id
            || path.file_stem().and_then(|stem| stem.to_str()) != Some(&record.instance_id)
            || pid_is_running(record.pid)
        {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                removed += 1;
                tracing::debug!(path = %path.display(), pid = record.pid, "removed stale local daemon record");
            }
            Err(error) => {
                tracing::debug!(path = %path.display(), %error, "could not remove stale local daemon record");
            }
        }
    }
    removed
}

/// Background writer for this daemon's record.
pub struct Publisher {
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    dir: PathBuf,
    instance_id: String,
}

impl Publisher {
    /// Prepare the folder, sweep stale records, write this daemon's record
    /// and start the refresh loop. Failures are logged and the daemon runs
    /// without a record: discovery is best effort and never blocks serving.
    pub async fn start(
        dir: PathBuf,
        fields: StaticFields,
        source: Arc<dyn LiveSource>,
    ) -> Option<Self> {
        if let Err(error) = ensure_dir(&dir) {
            tracing::warn!(dir = %dir.display(), %error, "local daemon discovery disabled: unsafe or unusable folder");
            return None;
        }
        let removed = sweep_stale(&dir, &fields.instance_id);
        if removed > 0 {
            tracing::info!(removed, "removed stale local daemon records");
        }
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let instance_id = fields.instance_id.clone();
        let loop_dir = dir.clone();
        let mut last = source.live().await;
        publish(&loop_dir, &fields, &last);
        let task = intent_core::spawn_daemon(async move {
            let mut ticker = tokio::time::interval(REFRESH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = stopped.changed() => return,
                    _ = ticker.tick() => {}
                }
                let live = source.live().await;
                if *stopped.borrow() {
                    return;
                }
                let missing =
                    std::fs::symlink_metadata(record_path(&loop_dir, &fields.instance_id)).is_err();
                if live != last || missing {
                    publish(&loop_dir, &fields, &live);
                    last = live;
                }
            }
        });
        Some(Self {
            stop,
            task,
            dir,
            instance_id,
        })
    }

    /// Stop the refresh loop and delete the record (clean exit).
    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
        if let Err(error) = remove_record(&self.dir, &self.instance_id) {
            tracing::warn!(%error, "could not remove local daemon record");
        }
    }
}

fn publish(dir: &Path, fields: &StaticFields, live: &LiveFields) {
    let record = fields.record(live);
    // Re-verify the folder each write: it may have been replaced meanwhile.
    let result = ensure_dir(dir).and_then(|()| write_record(dir, &record));
    match result {
        Ok(()) => tracing::debug!(port = ?record.port, "published local daemon record"),
        Err(error) => {
            tracing::warn!(dir = %dir.display(), %error, "could not publish local daemon record");
        }
    }
}

#[cfg(test)]
mod tests;
