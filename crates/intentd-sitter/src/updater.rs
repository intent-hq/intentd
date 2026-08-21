//! Update engine: fetch the channel manifest, compare versions, download +
//! sha256-verify + extract the platform archive, install atomically, prune.
//!
//! Every failure — network, HTTP status, manifest shape, checksum, archive —
//! is a soft failure returned as [`UpdateError`]; nothing here panics. The
//! caller (the supervisor) falls back to the last installed version.
//!
//! Install sequence (crash-safe, state written last):
//!
//! 1. download the archive to `sitter/tmp/…`, hashing while streaming
//! 2. verify sha256 against the manifest, extract next to the download
//! 3. stage `versions/.staging-…/intentd[.exe]` (exec perms, fsync)
//! 4. atomically rename the staging dir to `versions/<version>/`
//! 5. write `state.json` (`current_version`) via temp file + rename
//! 6. prune: keep the new and previous versions, delete the rest best-effort

use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::cli::Channel;
use crate::manifest::{self, ChannelManifest, ManifestError, PlatformEntry, TARGET_TRIPLE};
use crate::paths::{SitterPaths, DAEMON_BIN_NAME};
use crate::state;

/// TCP connect timeout for all requests (fail fast when offline).
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Total timeout for fetching a channel manifest.
pub const MANIFEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Total timeout for downloading a platform archive.
pub const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Result of a successful update check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// A new version was downloaded, verified, and installed.
    Installed {
        version: String,
        /// Version that was current before this install, if any.
        previous: Option<String>,
    },
    /// The installed version is equal to or newer than the manifest's.
    AlreadyCurrent { version: String },
}

/// Result of a dry-run check ([`Updater::check_only`]): installed vs latest,
/// nothing downloaded or installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCheck {
    /// Currently installed version, when its binary actually exists (a
    /// wiped `versions/` dir reads as nothing installed, mirroring
    /// [`Updater::check_and_install`]).
    pub installed: Option<String>,
    /// Version the channel manifest points at.
    pub latest: String,
    /// True when a real check would install `latest`.
    pub update_available: bool,
}

/// Soft failures from an update check. The caller decides how to proceed
/// (typically: log and start the last installed daemon).
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("failed to initialize http client: {0}")]
    Client(#[source] reqwest::Error),
    #[error("network error fetching {url}: {source}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("HTTP {status} fetching {url}")]
    HttpStatus {
        url: String,
        status: reqwest::StatusCode,
    },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("manifest has no platform entry for {triple}")]
    NoPlatformEntry { triple: &'static str },
    #[error("manifest version {version:?} is not valid semver: {source}")]
    InvalidManifestVersion {
        version: String,
        #[source]
        source: semver::Error,
    },
    #[error("sha256 mismatch for {asset}: manifest says {expected}, downloaded {actual}")]
    Sha256Mismatch {
        asset: String,
        expected: String,
        actual: String,
    },
    #[error("bad archive {asset}: {reason}")]
    Archive { asset: String, reason: String },
    #[error("install io error: {0}")]
    Io(#[from] io::Error),
    #[error("no manifest base URLs configured")]
    NoBaseUrls,
}

/// The update engine. Holds the resolved sitter paths and an HTTP client
/// with fail-fast timeouts.
pub struct Updater {
    paths: SitterPaths,
    /// Ordered manifest base URLs; never empty (enforced by the
    /// constructors). The manifest fetch tries each in order.
    base_urls: Vec<String>,
    client: reqwest::blocking::Client,
}

impl Updater {
    /// Updater against the real GitHub release manifests, trying each of
    /// [`manifest::DEFAULT_MANIFEST_BASE_URLS`] in order.
    ///
    /// # Errors
    ///
    /// Returns [`UpdateError::Client`] if the HTTP client cannot be built.
    pub fn new(paths: SitterPaths) -> Result<Self, UpdateError> {
        Self::with_base_urls(paths, manifest::DEFAULT_MANIFEST_BASE_URLS.iter().copied())
    }

    /// Updater against exactly one manifest base URL — no fallback (tests
    /// and the `INTENTD_SITTER_MANIFEST_BASE_URL` override use a local
    /// fixture server).
    ///
    /// # Errors
    ///
    /// Returns [`UpdateError::Client`] if the HTTP client cannot be built.
    pub fn with_base_url(
        paths: SitterPaths,
        base_url: impl Into<String>,
    ) -> Result<Self, UpdateError> {
        Self::with_base_urls(paths, [base_url.into()])
    }

    /// Updater against an ordered list of manifest base URLs; the manifest
    /// fetch tries each in order and the first fetchable + parseable
    /// manifest wins. An empty list is rejected with
    /// [`UpdateError::NoBaseUrls`].
    ///
    /// # Errors
    ///
    /// Returns [`UpdateError::NoBaseUrls`] for an empty list; [`UpdateError::Client`] if the HTTP client cannot be built.
    pub fn with_base_urls<I, S>(paths: SitterPaths, base_urls: I) -> Result<Self, UpdateError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let base_urls: Vec<String> = base_urls.into_iter().map(Into::into).collect();
        if base_urls.is_empty() {
            return Err(UpdateError::NoBaseUrls);
        }
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(UpdateError::Client)?;
        Ok(Self {
            paths,
            base_urls,
            client,
        })
    }

    /// Run one update check for `channel`: fetch the manifest and, when it
    /// points at a newer version than `state.current_version`, download,
    /// verify, and install it. Equal or older manifests are a no-op.
    ///
    /// # Errors
    ///
    /// Returns an [`UpdateError`] when the manifest cannot be fetched or parsed, or the download/verify/install sequence fails.
    pub fn check_and_install(&self, channel: Channel) -> Result<UpdateOutcome, UpdateError> {
        self.install_from_manifest(channel, false)
    }

    /// Fetch the channel manifest and install its version unconditionally,
    /// bypassing the newer-only comparison — the explicit
    /// `sitter channel <value> --redownload` path, and thus the only
    /// downgrade path (e.g. beta → stable). Reuses the same
    /// download/verify/install/prune sequence; never touches a running
    /// daemon (the new binary takes effect on the next spawn).
    ///
    /// # Errors
    ///
    /// Returns an [`UpdateError`] when the manifest cannot be fetched or parsed, or the download/verify/install sequence fails.
    pub fn force_install(&self, channel: Channel) -> Result<UpdateOutcome, UpdateError> {
        self.install_from_manifest(channel, true)
    }

    /// Dry-run: fetch the channel manifest and report installed vs latest
    /// without downloading or installing anything. Applies the same
    /// newer-only comparison (and the same "installed only counts when the
    /// binary exists" rule) as [`Updater::check_and_install`].
    ///
    /// # Errors
    ///
    /// Returns an [`UpdateError`] when the manifest cannot be fetched or parsed, or its version string is invalid.
    pub fn check_only(&self, channel: Channel) -> Result<UpdateCheck, UpdateError> {
        let manifest = self.fetch_manifest(channel)?;
        semver::Version::parse(&manifest.version).map_err(|source| {
            UpdateError::InvalidManifestVersion {
                version: manifest.version.clone(),
                source,
            }
        })?;

        let state = state::load(&self.paths.state_path);
        let installed = state
            .current_version
            .filter(|current| self.paths.daemon_binary(current).exists());
        let update_available = match installed.as_deref() {
            Some(current) => manifest_is_newer(&manifest.version, current)?,
            None => true,
        };
        Ok(UpdateCheck {
            installed,
            latest: manifest.version,
            update_available,
        })
    }

    fn install_from_manifest(
        &self,
        channel: Channel,
        force: bool,
    ) -> Result<UpdateOutcome, UpdateError> {
        let manifest = self.fetch_manifest(channel)?;
        // The manifest version becomes a directory name under `versions/` and
        // the persisted `current_version`; reject anything that is not valid
        // semver before it touches the filesystem (also covers the fresh
        // install path, where `manifest_is_newer` below never runs).
        semver::Version::parse(&manifest.version).map_err(|source| {
            UpdateError::InvalidManifestVersion {
                version: manifest.version.clone(),
                source,
            }
        })?;

        let state = state::load(&self.paths.state_path);
        if !force {
            if let Some(current) = state.current_version.as_deref() {
                // Only trust "already current" when the binary actually
                // exists; a wiped versions dir must trigger a reinstall.
                if self.paths.daemon_binary(current).exists()
                    && !manifest_is_newer(&manifest.version, current)?
                {
                    return Ok(UpdateOutcome::AlreadyCurrent {
                        version: current.to_string(),
                    });
                }
            }
        }

        let entry = manifest
            .platforms
            .get(TARGET_TRIPLE)
            .ok_or(UpdateError::NoPlatformEntry {
                triple: TARGET_TRIPLE,
            })?;

        let tmp_dir = self.paths.tmp_dir.join(format!(
            "update-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        fs::create_dir_all(&tmp_dir)?;
        let result = self.download_and_install(&manifest.version, channel, entry, &tmp_dir, force);
        let _ = fs::remove_dir_all(&tmp_dir);
        result
    }

    fn download_and_install(
        &self,
        version: &str,
        channel: Channel,
        entry: &PlatformEntry,
        tmp_dir: &Path,
        force: bool,
    ) -> Result<UpdateOutcome, UpdateError> {
        let archive_path = tmp_dir.join(&entry.asset);
        self.download_verified(entry, &archive_path)?;

        let extract_dir = tmp_dir.join("extracted");
        fs::create_dir_all(&extract_dir)?;
        extract_archive(&archive_path, &entry.asset, &extract_dir)?;
        let extracted_bin =
            find_daemon_binary(&extract_dir).ok_or_else(|| UpdateError::Archive {
                asset: entry.asset.clone(),
                reason: format!("archive does not contain a {DAEMON_BIN_NAME} binary"),
            })?;

        self.install_version(version, &extracted_bin)?;

        // state.json is written only after the binary is fully installed.
        // Reload it here instead of trusting the pre-download snapshot:
        // another updater (e.g. a serve-mode sitter's periodic check running
        // next to a CLI `intentd update`) may have installed an equal or
        // newer version while we were downloading, and overwriting its state
        // entry would activate a downgrade on the next (re)spawn. `force`
        // skips the guard — it is the explicit downgrade path.
        let mut new_state = state::load(&self.paths.state_path);
        if !force {
            if let Some(current) = new_state.current_version.as_deref() {
                // Strictly newer only: an equal version is our own reinstall
                // (or an identical concurrent install) and must still commit;
                // `install_version` above already made `version`'s binary
                // exist, so an "is current installed?" check can't be used
                // here. Unparseable `current` never wins.
                if self.paths.daemon_binary(current).exists()
                    && manifest_is_newer(current, version).unwrap_or(false)
                {
                    // Lost the race: keep the winner's state. Our orphaned
                    // `versions/<version>/` dir is swept by a later prune.
                    return Ok(UpdateOutcome::AlreadyCurrent {
                        version: current.to_string(),
                    });
                }
            }
        }
        let previous = new_state.current_version.take();
        new_state.channel = channel;
        new_state.current_version = Some(version.to_string());
        state::save(&self.paths.state_path, &new_state)?;

        self.prune(version, previous.as_deref());
        Ok(UpdateOutcome::Installed {
            version: version.to_string(),
            previous,
        })
    }

    /// Fetch and parse the channel manifest, trying each configured base
    /// URL in order. Any failure — network error, HTTP status, unparseable
    /// body — advances to the next base; when every base fails, the last
    /// error is returned.
    fn fetch_manifest(&self, channel: Channel) -> Result<ChannelManifest, UpdateError> {
        let mut last_err = None;
        for (i, base_url) in self.base_urls.iter().enumerate() {
            let url = manifest::manifest_url(base_url, channel);
            let attempt = self
                .fetch(&url, MANIFEST_TIMEOUT)
                .and_then(|bytes| manifest::parse(&bytes).map_err(UpdateError::from));
            match attempt {
                Ok(manifest) => return Ok(manifest),
                Err(e) => {
                    // Surface a degraded primary even when a later base
                    // succeeds (the overall check would otherwise hide it).
                    if i + 1 < self.base_urls.len() {
                        eprintln!(
                            "intentd-sitter: manifest fetch from {url} failed ({e}); \
                             trying next base"
                        );
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or(UpdateError::NoBaseUrls))
    }

    fn fetch(&self, url: &str, timeout: Duration) -> Result<Vec<u8>, UpdateError> {
        let network = |source| UpdateError::Network {
            url: url.to_string(),
            source,
        };
        let resp = self
            .client
            .get(url)
            .timeout(timeout)
            .send()
            .map_err(network)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(UpdateError::HttpStatus {
                url: url.to_string(),
                status,
            });
        }
        Ok(resp.bytes().map_err(network)?.to_vec())
    }

    /// Stream the archive to `dest`, hashing as it downloads, and reject it
    /// (removing the partial file) when the digest disagrees with the
    /// manifest.
    fn download_verified(&self, entry: &PlatformEntry, dest: &Path) -> Result<(), UpdateError> {
        let network = |source| UpdateError::Network {
            url: entry.url.clone(),
            source,
        };
        let mut resp = self
            .client
            .get(&entry.url)
            .timeout(DOWNLOAD_TIMEOUT)
            .send()
            .map_err(network)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(UpdateError::HttpStatus {
                url: entry.url.clone(),
                status,
            });
        }

        let mut tee = TeeWriter {
            file: fs::File::create(dest)?,
            hasher: Sha256::new(),
        };
        resp.copy_to(&mut tee).map_err(network)?;
        tee.file.flush()?;

        let actual: String = tee
            .hasher
            .finalize()
            .iter()
            .fold(String::new(), |mut s, b| {
                let _ = write!(s, "{b:02x}");
                s
            });
        let expected = entry.sha256.to_ascii_lowercase();
        if actual != expected {
            let _ = fs::remove_file(dest);
            return Err(UpdateError::Sha256Mismatch {
                asset: entry.asset.clone(),
                expected,
                actual,
            });
        }
        Ok(())
    }

    /// Stage the binary under `versions/` with exec permissions, fsync it,
    /// then atomically rename the staging dir to `versions/<version>/`.
    fn install_version(&self, version: &str, src_bin: &Path) -> Result<(), UpdateError> {
        fs::create_dir_all(&self.paths.versions_dir)?;
        let staging =
            self.paths
                .versions_dir
                .join(format!(".staging-{}-{}", version, std::process::id()));
        let _ = fs::remove_dir_all(&staging);
        fs::create_dir_all(&staging)?;

        let staged_bin = staging.join(DAEMON_BIN_NAME);
        fs::copy(src_bin, &staged_bin)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&staged_bin, fs::Permissions::from_mode(0o755))?;
        }
        fs::File::open(&staged_bin)?.sync_all()?;

        let final_dir = self.paths.versions_dir.join(version);
        if final_dir.exists() {
            // Leftover from an interrupted install of this same version.
            fs::remove_dir_all(&final_dir)?;
        }
        fs::rename(&staging, &final_dir)?;
        if let Ok(dir) = fs::File::open(&self.paths.versions_dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Delete everything under `versions/` except the current and previous
    /// versions (also sweeps stale staging dirs). Best-effort by design.
    fn prune(&self, current: &str, previous: Option<&str>) {
        let Ok(entries) = fs::read_dir(&self.paths.versions_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let keep = name
                .to_str()
                .is_some_and(|n| n == current || previous == Some(n));
            if !keep {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    }
}

/// True when the manifest's version is strictly newer than `current`.
/// A `current` that no longer parses as semver cannot be compared and is
/// treated as out of date (reinstall); a manifest version that does not
/// parse is a soft manifest failure.
fn manifest_is_newer(manifest_version: &str, current: &str) -> Result<bool, UpdateError> {
    if manifest_version == current {
        return Ok(false);
    }
    let manifest = semver::Version::parse(manifest_version).map_err(|source| {
        UpdateError::InvalidManifestVersion {
            version: manifest_version.to_string(),
            source,
        }
    })?;
    match semver::Version::parse(current) {
        Ok(current) => Ok(manifest > current),
        Err(_) => Ok(true),
    }
}

/// Hashes bytes as they stream to the download file.
struct TeeWriter {
    file: fs::File,
    hasher: Sha256,
}

impl Write for TeeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.update(buf);
        self.file.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[allow(clippy::case_sensitive_file_extension_comparisons)] // extensions generated by our own code with fixed case
fn extract_archive(archive: &Path, asset: &str, dest: &Path) -> Result<(), UpdateError> {
    let archive_err = |reason: String| UpdateError::Archive {
        asset: asset.to_string(),
        reason,
    };
    if asset.ends_with(".tar.xz") {
        let file = fs::File::open(archive)?;
        tar::Archive::new(liblzma::read::XzDecoder::new(file))
            .unpack(dest)
            .map_err(|e| archive_err(e.to_string()))
    } else if asset.ends_with(".tar.gz") {
        let file = fs::File::open(archive)?;
        tar::Archive::new(flate2::read::GzDecoder::new(file))
            .unpack(dest)
            .map_err(|e| archive_err(e.to_string()))
    } else if asset.ends_with(".zip") {
        extract_zip(archive, dest).map_err(archive_err)
    } else {
        Err(archive_err("unsupported archive type".to_string()))
    }
}

#[cfg(windows)]
fn extract_zip(archive: &Path, dest: &Path) -> Result<(), String> {
    let file = fs::File::open(archive).map_err(|e| e.to_string())?;
    let mut zip = zip::ZipArchive::new(file).map_err(|e| e.to_string())?;
    zip.extract(dest).map_err(|e| e.to_string())
}

#[cfg(not(windows))]
fn extract_zip(_archive: &Path, _dest: &Path) -> Result<(), String> {
    Err("zip archives are only supported on windows".to_string())
}

/// Depth-first search for the daemon binary in the extracted archive
/// (cargo-dist archives nest it under `intentd-<triple>/`).
fn find_daemon_binary(root: &Path) -> Option<PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if entry.file_name() == DAEMON_BIN_NAME {
                return Some(path);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newer_older_equal_versions() {
        assert!(manifest_is_newer("0.2.0", "0.1.0").unwrap());
        assert!(!manifest_is_newer("0.1.0", "0.1.0").unwrap());
        assert!(!manifest_is_newer("0.1.0", "0.2.0").unwrap());
        assert!(manifest_is_newer("0.2.0", "0.2.0-rc.1").unwrap());
    }

    #[test]
    fn unparseable_current_means_reinstall() {
        assert!(manifest_is_newer("0.2.0", "not-a-version").unwrap());
    }

    #[test]
    fn equal_strings_never_error_even_if_not_semver() {
        assert!(!manifest_is_newer("weird", "weird").unwrap());
    }

    #[test]
    fn unparseable_manifest_version_is_soft_error() {
        assert!(matches!(
            manifest_is_newer("not-a-version", "0.1.0"),
            Err(UpdateError::InvalidManifestVersion { .. })
        ));
    }

    #[test]
    fn extracts_tar_gz_and_finds_nested_binary() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir.path().join("intentd-x.tar.gz");
        let mut header = tar::Header::new_gnu();
        header.set_size(9);
        header.set_mode(0o755);
        header.set_cksum();
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut builder = tar::Builder::new(encoder);
        builder
            .append_data(
                &mut header,
                format!("intentd-x/{DAEMON_BIN_NAME}"),
                &b"gz daemon"[..],
            )
            .unwrap();
        fs::write(
            &archive_path,
            builder.into_inner().unwrap().finish().unwrap(),
        )
        .unwrap();

        let dest = dir.path().join("out");
        fs::create_dir_all(&dest).unwrap();
        extract_archive(&archive_path, "intentd-x.tar.gz", &dest).unwrap();
        let found = find_daemon_binary(&dest).unwrap();
        assert_eq!(fs::read(found).unwrap(), b"gz daemon");
    }

    #[cfg(not(windows))]
    #[test]
    fn zip_is_unsupported_off_windows() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("intentd-x.zip");
        fs::write(&archive, b"PK").unwrap();
        assert!(matches!(
            extract_archive(&archive, "intentd-x.zip", dir.path()),
            Err(UpdateError::Archive { .. })
        ));
    }

    #[test]
    fn unknown_archive_type_is_soft_error() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("intentd-x.tar.bz2");
        fs::write(&archive, b"x").unwrap();
        assert!(matches!(
            extract_archive(&archive, "intentd-x.tar.bz2", dir.path()),
            Err(UpdateError::Archive { .. })
        ));
    }
}
