//! Explicit, pinned acquisition of Google's official ACP bundle.
//!
//! Discovery never calls this module. Downloads stay in a private staging
//! directory until both executables pass integrity and Apple signature checks.

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use intent_providers::antigravity::{ARCHIVE_SHA256, FILES, HARNESS, SERVER, VERSION};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{watch, Mutex};

pub const ARCHIVE_URL: &str = "https://dl.google.com/agy-extensions/releases/macos/agy-acp-server-agy_acp_server_1.1.1-darwin-arm64.zip";
pub const ARCHIVE_BYTES: u64 = 316_014_828;
const MAX_EXTRACTED_BYTES: u64 = 1_000_000_000;
const INSTALL_TIMEOUT: Duration = Duration::from_secs(600);
const GOOGLE_REQUIREMENT: &str =
    "=anchor apple generic and certificate leaf[subject.OU] = \"EQHXZ8M8AV\"";

/// Shared cancellation also reaches bounded blocking extraction work.
#[derive(Clone)]
pub struct Cancellation(watch::Sender<bool>);

impl Default for Cancellation {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}

impl Cancellation {
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }

    fn check(&self, deadline: Instant) -> Result<(), InstallError> {
        if self.is_cancelled() {
            Err(InstallError::Cancelled)
        } else if Instant::now() >= deadline {
            Err(InstallError::TimedOut)
        } else {
            Ok(())
        }
    }
}

/// Safe wire codes. Underlying network, archive, and subprocess errors never
/// cross the UI boundary or carry credentials/URLs into persisted state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InstallError {
    UnsupportedHost,
    DownloadFailed,
    InvalidArchive,
    IntegrityFailed,
    SignatureFailed,
    DiskError,
    Cancelled,
    TimedOut,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(tag = "phase", rename_all = "camelCase")]
pub enum InstallProgress {
    Downloading { received: u64, total: u64 },
    Verifying,
}

pub type Progress = Arc<dyn Fn(InstallProgress) + Send + Sync>;

fn install_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Acquire a managed runtime after a user requests setup. Concurrent callers
/// share the first download through the lock and validated on-disk result.
///
/// # Errors
/// Returns a safe failure code without changing an existing valid runtime.
pub async fn install(
    root: PathBuf,
    cancellation: Cancellation,
    progress: Progress,
) -> Result<PathBuf, InstallError> {
    if !intent_providers::antigravity::supported_host() {
        return Err(InstallError::UnsupportedHost);
    }
    install_with_timeout(root, cancellation, progress, INSTALL_TIMEOUT).await
}

async fn install_with_timeout(
    root: PathBuf,
    cancellation: Cancellation,
    progress: Progress,
    timeout: Duration,
) -> Result<PathBuf, InstallError> {
    // Stop blocking extraction on our deadline without cancelling the caller's
    // operation: it must still publish the timedOut failure and allow retry.
    let worker_cancel = Cancellation::default();
    let work = async {
        let _lock = install_lock().lock().await;
        worker_cancel.check(Instant::now() + INSTALL_TIMEOUT)?;
        let root = prepare_root(&root)?;
        let destination = root.join(VERSION);
        if destination.exists() && valid_bundle(&destination, &worker_cancel).await.is_ok() {
            worker_cancel.check(Instant::now() + INSTALL_TIMEOUT)?;
            write_ready(&destination)?;
            return Ok(destination.join(SERVER));
        }
        worker_cancel.check(Instant::now() + INSTALL_TIMEOUT)?;
        let staging = tempfile::Builder::new()
            .prefix(".install-")
            .tempdir_in(&root)
            .map_err(|_| InstallError::DiskError)?;
        let archive = staging.path().join("download.zip");
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(20))
            .timeout(INSTALL_TIMEOUT)
            .build()
            .map_err(|_| InstallError::DownloadFailed)?;
        download(
            &client,
            ARCHIVE_URL,
            &archive,
            ARCHIVE_BYTES,
            ARCHIVE_SHA256,
            &worker_cancel,
            &progress,
        )
        .await?;
        progress(InstallProgress::Verifying);
        let cancel = worker_cancel.clone();
        let staging = tokio::task::spawn_blocking(move || {
            let bundle = staging.path().join("bundle");
            fs::create_dir(&bundle).map_err(|_| InstallError::DiskError)?;
            extract_bundle(
                &archive,
                &bundle,
                &cancel,
                Instant::now() + Duration::from_secs(90),
            )?;
            Ok::<_, InstallError>(staging)
        })
        .await
        .map_err(|_| InstallError::InvalidArchive)??;
        let bundle = staging.path().join("bundle");
        valid_bundle(&bundle, &worker_cancel).await?;
        write_ready(&bundle)?;
        worker_cancel.check(Instant::now() + INSTALL_TIMEOUT)?;
        activate(&bundle, &destination, &root)?;
        Ok(destination.join(SERVER))
    };
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            worker_cancel.cancel();
            Err(InstallError::Cancelled)
        },
        result = tokio::time::timeout(timeout, work) => if let Ok(result) = result {
            result
        } else {
            worker_cancel.cancel(); Err(InstallError::TimedOut)
        }
    }
}

fn prepare_root(root: &Path) -> Result<PathBuf, InstallError> {
    if !root.is_absolute() {
        return Err(InstallError::DiskError);
    }
    // Inspect existing ancestors before create_dir_all can follow a symlink.
    // macOS /tmp itself is a symlink, so callers/tests use canonical homes.
    for path in root.ancestors() {
        match fs::symlink_metadata(path) {
            Ok(meta) if !meta.is_dir() => return Err(InstallError::DiskError),
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(InstallError::DiskError)
            }
            _ => {}
        }
    }
    fs::create_dir_all(root).map_err(|_| InstallError::DiskError)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))
            .map_err(|_| InstallError::DiskError)?;
    }
    root.canonicalize().map_err(|_| InstallError::DiskError)
}

async fn download(
    client: &reqwest::Client,
    url: &str,
    destination: &Path,
    max_bytes: u64,
    expected_hash: &str,
    cancellation: &Cancellation,
    progress: &Progress,
) -> Result<(), InstallError> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| InstallError::DownloadFailed)?;
    if !response.status().is_success() || response.content_length().is_some_and(|n| n > max_bytes) {
        return Err(InstallError::DownloadFailed);
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .await
        .map_err(|_| InstallError::DiskError)?;
    let mut hash = Sha256::new();
    let mut received = 0_u64;
    let mut reported = 0_u64;
    progress(InstallProgress::Downloading {
        received,
        total: max_bytes,
    });
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| InstallError::DownloadFailed)?
    {
        cancellation.check(Instant::now() + INSTALL_TIMEOUT)?;
        received = received
            .checked_add(chunk.len() as u64)
            .ok_or(InstallError::InvalidArchive)?;
        if received > max_bytes {
            return Err(InstallError::InvalidArchive);
        }
        file.write_all(&chunk)
            .await
            .map_err(|_| InstallError::DiskError)?;
        hash.update(&chunk);
        if received.saturating_sub(reported) >= 1024 * 1024 {
            progress(InstallProgress::Downloading {
                received,
                total: max_bytes,
            });
            reported = received;
        }
    }
    file.sync_all().await.map_err(|_| InstallError::DiskError)?;
    if hex(&hash.finalize()) != expected_hash {
        return Err(InstallError::IntegrityFailed);
    }
    Ok(())
}

fn extract_bundle(
    archive: &Path,
    target: &Path,
    cancel: &Cancellation,
    deadline: Instant,
) -> Result<(), InstallError> {
    let file = File::open(archive).map_err(|_| InstallError::DiskError)?;
    let mut zip = zip::ZipArchive::new(file).map_err(|_| InstallError::InvalidArchive)?;
    if zip.len() != FILES.len() {
        return Err(InstallError::InvalidArchive);
    }
    let mut seen = std::collections::HashSet::new();
    let mut extracted = 0_u64;
    for index in 0..zip.len() {
        let mut entry = zip
            .by_index(index)
            .map_err(|_| InstallError::InvalidArchive)?;
        if ![SERVER, HARNESS].contains(&entry.name())
            || !seen.insert(entry.name().to_owned())
            || entry
                .unix_mode()
                .is_some_and(|mode| mode & 0o170_000 != 0 && mode & 0o170_000 != 0o100_000)
            || entry.size() > MAX_EXTRACTED_BYTES
        {
            return Err(InstallError::InvalidArchive);
        }
        let path = target.join(entry.name());
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| InstallError::DiskError)?;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            cancel.check(deadline)?;
            let bytes = entry
                .read(&mut buffer)
                .map_err(|_| InstallError::InvalidArchive)?;
            if bytes == 0 {
                break;
            }
            extracted = extracted
                .checked_add(bytes as u64)
                .ok_or(InstallError::InvalidArchive)?;
            if extracted > MAX_EXTRACTED_BYTES {
                return Err(InstallError::InvalidArchive);
            }
            out.write_all(&buffer[..bytes])
                .map_err(|_| InstallError::DiskError)?;
        }
        out.sync_all().map_err(|_| InstallError::DiskError)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .map_err(|_| InstallError::DiskError)?;
        }
    }
    Ok(())
}

async fn valid_bundle(root: &Path, cancellation: &Cancellation) -> Result<(), InstallError> {
    let root = root.to_owned();
    let cancel = cancellation.clone();
    let root = tokio::task::spawn_blocking(move || {
        if !fs::symlink_metadata(&root).is_ok_and(|meta| meta.is_dir()) {
            return Err(InstallError::IntegrityFailed);
        }
        let deadline = Instant::now() + Duration::from_secs(90);
        for (name, length, digest) in FILES {
            let path = root.join(name);
            if !fs::symlink_metadata(&path).is_ok_and(|meta| meta.is_file() && meta.len() == length)
            {
                return Err(InstallError::IntegrityFailed);
            }
            let mut file = File::open(&path).map_err(|_| InstallError::DiskError)?;
            let mut hash = Sha256::new();
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                cancel.check(deadline)?;
                let bytes = file
                    .read(&mut buffer)
                    .map_err(|_| InstallError::DiskError)?;
                if bytes == 0 {
                    break;
                }
                hash.update(&buffer[..bytes]);
            }
            if hex(&hash.finalize()) != digest {
                return Err(InstallError::IntegrityFailed);
            }
            if !intent_core::path_utils::is_executable_file(&path) {
                return Err(InstallError::IntegrityFailed);
            }
        }
        Ok(root)
    })
    .await
    .map_err(|_| InstallError::IntegrityFailed)??;
    for name in [SERVER, HARNESS] {
        let status = tokio::process::Command::new("/usr/bin/codesign")
            .args(["--verify", "--strict", "-R", GOOGLE_REQUIREMENT])
            .arg(root.join(name))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status()
            .await
            .map_err(|_| InstallError::SignatureFailed)?;
        if !status.success() {
            return Err(InstallError::SignatureFailed);
        }
    }
    Ok(())
}

fn write_ready(bundle: &Path) -> Result<(), InstallError> {
    // Atomic replacement does not follow an existing marker symlink.
    let mut marker =
        tempfile::NamedTempFile::new_in(bundle).map_err(|_| InstallError::DiskError)?;
    marker
        .write_all(ARCHIVE_SHA256.as_bytes())
        .map_err(|_| InstallError::DiskError)?;
    marker
        .as_file()
        .sync_all()
        .map_err(|_| InstallError::DiskError)?;
    marker
        .persist(bundle.join("ready"))
        .map_err(|_| InstallError::DiskError)?;
    Ok(())
}

fn activate(staged: &Path, destination: &Path, root: &Path) -> Result<(), InstallError> {
    // Retain an invalid earlier managed copy for rollback. Never replace a
    // custom path or delete another application's files.
    let backup = root.join(format!(".previous-{}", uuid::Uuid::new_v4()));
    let had_previous = fs::symlink_metadata(destination).is_ok();
    if had_previous {
        fs::rename(destination, &backup).map_err(|_| InstallError::DiskError)?;
    }
    if fs::rename(staged, destination).is_err() {
        if had_previous {
            let _ = fs::rename(&backup, destination);
        }
        return Err(InstallError::DiskError);
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

#[cfg(test)]
mod tests;
