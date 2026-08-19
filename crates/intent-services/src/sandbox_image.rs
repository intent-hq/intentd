//! Guest-image contract + download/verify/cache pipeline for microVM
//! sandboxing (monorepo#1120, EE-3).
//!
//! A *guest image* is a versioned aarch64 Linux rootfs (tar, xz-compressed)
//! plus a `manifest.json` describing it: architecture, the vsock exec agent
//! contract, which ACP providers are preinstalled, and the tool inventory.
//! Any image whose manifest conforms to schema v1 works — the base image
//! published from `guest-image/` in this repo is just the default. Images are
//! fetched on first use from a manifest URL, sha256-verified, and cached
//! content-addressed under `<data_dir>/guest-images/`.
//!
//! Resolution order for which image a spawn uses (consumed by the EE-5
//! orchestrator): repo `.intent/config.json` (`executionEnvironment.image`)
//! → sandbox-profile default → built-in pin ([`builtin_image_ref`]).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use intent_core::events::{SANDBOX_IMAGE_DOWNLOADED, SANDBOX_IMAGE_ERROR, SANDBOX_IMAGE_PULLING};
use intent_core::{now_iso, GuestImageRef, RepoConfig, WorkspaceId};
use intent_store::NewEvent;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::events::EventBus;
use crate::system_actor;

/// Guest-image manifest schema version this daemon understands.
pub const GUEST_IMAGE_MANIFEST_SCHEMA_VERSION: u64 = 1;

/// Guest architecture v1 supports (Apple Silicon hosts boot aarch64 guests).
pub const SUPPORTED_ARCH: &str = "aarch64";

/// Rootfs archive format v1 supports.
pub const SUPPORTED_ROOTFS_FORMAT: &str = "tar.xz";

/// vsock exec-agent protocol the daemon speaks (EE-5 consumes this contract).
pub const SUPPORTED_EXEC_PROTOCOL: &str = "intent-exec/1";

/// Version of the built-in pinned base image (the `guest-image-v<VERSION>`
/// release tag on intent-hq/intentd, mirrored to intent-hq/intentd-releases).
/// Bumping the pin is a deliberate one-line change here.
pub const BUILTIN_IMAGE_VERSION: &str = "0.1.0";

/// Manifest URL of the built-in pinned base image (public mirror).
pub fn builtin_manifest_url() -> String {
    format!(
        "https://github.com/intent-hq/intentd-releases/releases/download/guest-image-v{BUILTIN_IMAGE_VERSION}/manifest.json"
    )
}

/// Subdirectory of the daemon data dir holding cached guest images.
pub const GUEST_IMAGE_CACHE_DIR: &str = "guest-images";

/// Cache root for guest images under the daemon `data_dir`.
pub fn guest_image_cache_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(GUEST_IMAGE_CACHE_DIR)
}

/// Where a resolved [`GuestImageRef`] came from — named in structured errors
/// and `sandbox:image:*` events so users know which config to fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageSource {
    /// Repo `.intent/config.json` → `executionEnvironment.image`.
    RepoConfig,
    /// The sandbox profile's default-image override.
    ProfileDefault,
    /// The built-in pin compiled into the daemon.
    BuiltinPin,
}

impl ImageSource {
    /// Stable machine-readable identifier carried in events/errors.
    pub fn as_str(&self) -> &'static str {
        match self {
            ImageSource::RepoConfig => "repo-config",
            ImageSource::ProfileDefault => "profile-default",
            ImageSource::BuiltinPin => "builtin-pin",
        }
    }

    /// Human-readable name of the config surface for error prose.
    pub fn describe(&self) -> &'static str {
        match self {
            ImageSource::RepoConfig => ".intent/config.json (executionEnvironment.image)",
            ImageSource::ProfileDefault => "sandbox profile default image",
            ImageSource::BuiltinPin => "built-in image pin",
        }
    }
}

impl std::fmt::Display for ImageSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The built-in pinned image reference (manifest fetched unpinned; the rootfs
/// inside is always sha256-verified).
pub fn builtin_image_ref() -> GuestImageRef {
    GuestImageRef {
        manifest_url: builtin_manifest_url(),
        sha256: None,
    }
}

/// Resolve which image reference a spawn should use: repo config →
/// profile default → built-in pin. The EE-5 orchestrator passes the repo's
/// parsed [`RepoConfig`] and the active profile's optional override.
pub fn resolve_image_ref(
    repo_config: &RepoConfig,
    profile_default: Option<&GuestImageRef>,
) -> (GuestImageRef, ImageSource) {
    if let Some(image) = repo_config
        .execution_environment
        .as_ref()
        .and_then(|ee| ee.image.as_ref())
    {
        return (image.clone(), ImageSource::RepoConfig);
    }
    if let Some(image) = profile_default {
        return (image.clone(), ImageSource::ProfileDefault);
    }
    (builtin_image_ref(), ImageSource::BuiltinPin)
}

// ---------------------------------------------------------------------------
// Manifest schema (v1)
// ---------------------------------------------------------------------------

/// The vsock exec-agent contract advertised by an image: how the daemon
/// executes commands inside the booted guest. `init` is the guest entrypoint
/// (PID 1 workload) that mounts pseudo-filesystems and starts the exec agent;
/// `port` is the vsock port the agent listens on; `protocol` names the wire
/// protocol version ([`SUPPORTED_EXEC_PROTOCOL`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VsockExecSpec {
    /// Absolute guest path of the init entrypoint (e.g. `/usr/local/bin/intent-init`).
    pub init: String,
    /// vsock port the exec agent accepts connections on.
    pub port: u32,
    /// Exec protocol identifier (e.g. `intent-exec/1`).
    pub protocol: String,
}

/// The rootfs archive of an image: where to fetch it, its format, and digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootfsSpec {
    /// Absolute download URL of the rootfs archive.
    pub url: String,
    /// Archive format ([`SUPPORTED_ROOTFS_FORMAT`]).
    pub format: String,
    /// Hex sha256 of the archive as downloaded.
    pub sha256: String,
    /// Uncompressed size in bytes (informational; used for cache sizing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Guest-image manifest (schema v1). Parsing is lenient about extra fields
/// (forward compat); validation is strict about the contract fields the
/// daemon needs ([`ImageManifest::validate`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageManifest {
    /// Manifest schema version ([`GUEST_IMAGE_MANIFEST_SCHEMA_VERSION`]).
    pub schema: u64,
    /// Stable image identifier (e.g. `intent-guest-base`).
    pub id: String,
    /// Image version (semver-ish string; informational).
    pub version: String,
    /// Guest CPU architecture ([`SUPPORTED_ARCH`]).
    pub arch: String,
    /// The rootfs archive.
    pub rootfs: RootfsSpec,
    /// The vsock exec-agent contract.
    pub vsock_exec: VsockExecSpec,
    /// Provider-inclusion map: provider id → whether its CLI/adapter is
    /// preinstalled in the image (e.g. `{"auggie": true, "opencode": true}`).
    /// Providers absent from the map are treated as not included.
    #[serde(default)]
    pub providers: BTreeMap<String, bool>,
    /// Tool inventory: tool name → version string (e.g. `{"node": "24.13.0"}`).
    /// Informational; surfaced in diagnostics.
    #[serde(default)]
    pub tools: BTreeMap<String, String>,
    /// Unknown/extra keys tolerated for forward compatibility.
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// A structured guest-image pipeline failure. Every variant names the
/// `config_source` the failing image reference came from so the user knows
/// which config surface to fix (Definition of Done: "non-conforming images
/// produce structured errors naming the config source").
#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    #[error("failed to fetch image manifest from {url} (configured via {}): {detail}", config_source.describe())]
    ManifestFetch {
        url: String,
        config_source: ImageSource,
        detail: String,
    },
    #[error("image manifest at {url} (configured via {}) is invalid: {detail}", config_source.describe())]
    ManifestInvalid {
        url: String,
        config_source: ImageSource,
        detail: String,
    },
    #[error("image manifest sha256 mismatch for {url} (configured via {}): expected {expected}, got {actual}", config_source.describe())]
    ManifestSha256Mismatch {
        url: String,
        config_source: ImageSource,
        expected: String,
        actual: String,
    },
    #[error("image manifest at {url} (configured via {}) does not conform to the guest-image contract: {detail}", config_source.describe())]
    NonConforming {
        url: String,
        config_source: ImageSource,
        detail: String,
    },
    #[error("failed to download rootfs {rootfs_url} for image manifest {url} (configured via {}): {detail}", config_source.describe())]
    RootfsDownload {
        url: String,
        rootfs_url: String,
        config_source: ImageSource,
        detail: String,
    },
    #[error("rootfs sha256 mismatch for image manifest {url} (configured via {}): expected {expected}, got {actual}", config_source.describe())]
    RootfsSha256Mismatch {
        url: String,
        config_source: ImageSource,
        expected: String,
        actual: String,
    },
    #[error("guest-image cache I/O failure at {path}: {detail}")]
    CacheIo { path: String, detail: String },
}

impl ImageError {
    /// The config surface the failing image reference came from. Cache I/O
    /// failures are environmental, not config-attributable.
    pub fn config_source(&self) -> Option<&ImageSource> {
        match self {
            ImageError::ManifestFetch { config_source, .. }
            | ImageError::ManifestInvalid { config_source, .. }
            | ImageError::ManifestSha256Mismatch { config_source, .. }
            | ImageError::NonConforming { config_source, .. }
            | ImageError::RootfsDownload { config_source, .. }
            | ImageError::RootfsSha256Mismatch { config_source, .. } => Some(config_source),
            ImageError::CacheIo { .. } => None,
        }
    }
}

impl From<ImageError> for intent_core::Error {
    fn from(e: ImageError) -> Self {
        match &e {
            ImageError::ManifestInvalid { .. } | ImageError::NonConforming { .. } => {
                intent_core::Error::InvalidInput(e.to_string())
            }
            _ => intent_core::Error::Internal(e.to_string()),
        }
    }
}

impl ImageManifest {
    /// Parse and contract-check a manifest document. `url` and `source` label
    /// the structured error on failure.
    pub fn parse_and_validate(
        content: &str,
        url: &str,
        source: &ImageSource,
    ) -> std::result::Result<Self, ImageError> {
        let manifest: ImageManifest =
            serde_json::from_str(content).map_err(|e| ImageError::ManifestInvalid {
                url: url.to_string(),
                config_source: source.clone(),
                detail: e.to_string(),
            })?;
        manifest.validate(url, source)?;
        Ok(manifest)
    }

    /// Contract conformance: schema version, architecture, rootfs format,
    /// digest shape, and the vsock exec agent. Violations produce
    /// [`ImageError::NonConforming`] naming the config source.
    pub fn validate(&self, url: &str, source: &ImageSource) -> std::result::Result<(), ImageError> {
        let fail = |detail: String| ImageError::NonConforming {
            url: url.to_string(),
            config_source: source.clone(),
            detail,
        };
        if self.schema != GUEST_IMAGE_MANIFEST_SCHEMA_VERSION {
            return Err(fail(format!(
                "unsupported schema {} (this daemon understands schema {GUEST_IMAGE_MANIFEST_SCHEMA_VERSION})",
                self.schema
            )));
        }
        if self.arch != SUPPORTED_ARCH {
            return Err(fail(format!(
                "unsupported arch {:?} (expected {SUPPORTED_ARCH:?})",
                self.arch
            )));
        }
        if self.id.trim().is_empty() {
            return Err(fail("image id is empty".to_string()));
        }
        if self.version.trim().is_empty() {
            return Err(fail("image version is empty".to_string()));
        }
        if self.rootfs.format != SUPPORTED_ROOTFS_FORMAT {
            return Err(fail(format!(
                "unsupported rootfs format {:?} (expected {SUPPORTED_ROOTFS_FORMAT:?})",
                self.rootfs.format
            )));
        }
        if !is_hex_sha256(&self.rootfs.sha256) {
            return Err(fail(format!(
                "rootfs sha256 {:?} is not a 64-char hex digest",
                self.rootfs.sha256
            )));
        }
        if self.rootfs.url.trim().is_empty() {
            return Err(fail("rootfs url is empty".to_string()));
        }
        if self.vsock_exec.protocol != SUPPORTED_EXEC_PROTOCOL {
            return Err(fail(format!(
                "unsupported vsock exec protocol {:?} (expected {SUPPORTED_EXEC_PROTOCOL:?})",
                self.vsock_exec.protocol
            )));
        }
        if self.vsock_exec.init.trim().is_empty() || !self.vsock_exec.init.starts_with('/') {
            return Err(fail(format!(
                "vsock exec init {:?} must be an absolute guest path",
                self.vsock_exec.init
            )));
        }
        if self.vsock_exec.port == 0 {
            return Err(fail("vsock exec port must be non-zero".to_string()));
        }
        Ok(())
    }
}

/// Whether `s` looks like a lowercase-insensitive 64-char hex sha256 digest.
fn is_hex_sha256(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// Download / verify / cache
// ---------------------------------------------------------------------------

/// A guest image resolved into the local cache: the validated manifest plus
/// the on-disk paths EE-5 hands to the microVM helper.
#[derive(Debug, Clone)]
pub struct CachedImage {
    /// The validated manifest.
    pub manifest: ImageManifest,
    /// Verified rootfs archive in the cache.
    pub rootfs_path: PathBuf,
    /// Manifest document cached next to the rootfs.
    pub manifest_path: PathBuf,
}

/// Total timeout for fetching a manifest document.
const MANIFEST_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Total timeout for downloading a rootfs archive.
const ROOTFS_DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Ensure the image referenced by `image_ref` is present and verified in the
/// cache under `data_dir`, downloading on first use. Emits
/// `sandbox:image:pulling` / `sandbox:image:downloaded` /
/// `sandbox:image:error` on `bus` when provided (`workspace_id` scopes the
/// events; `None` publishes them workspace-unscoped like other global events).
///
/// The manifest is always re-fetched (it is small and may move between
/// versions); the rootfs is content-addressed by its sha256, so a cache hit
/// skips the download entirely.
pub async fn ensure_image(
    data_dir: &Path,
    image_ref: &GuestImageRef,
    source: &ImageSource,
    bus: Option<&EventBus>,
    workspace_id: Option<&WorkspaceId>,
) -> std::result::Result<CachedImage, ImageError> {
    match ensure_image_inner(data_dir, image_ref, source, bus, workspace_id).await {
        Ok(cached) => Ok(cached),
        Err(e) => {
            publish_event(
                bus,
                workspace_id,
                SANDBOX_IMAGE_ERROR,
                json!({
                    "manifestUrl": image_ref.manifest_url,
                    "configSource": source.as_str(),
                    "error": e.to_string(),
                }),
            )
            .await;
            Err(e)
        }
    }
}

/// Fetch the manifest referenced by `image_ref`, verify the optional outer
/// pin, and contract-check it — the network/validation half of
/// [`ensure_image`], without touching the cache or downloading the rootfs.
/// Also the engine behind the `sandbox.image.check` dry-run RPC (§5.5b).
pub async fn fetch_and_validate_manifest(
    image_ref: &GuestImageRef,
    source: &ImageSource,
) -> std::result::Result<(ImageManifest, Vec<u8>), ImageError> {
    let url = &image_ref.manifest_url;
    let client = reqwest::Client::new();

    // 1. Fetch the manifest document.
    let manifest_bytes = fetch_bytes(&client, url, MANIFEST_FETCH_TIMEOUT)
        .await
        .map_err(|detail| ImageError::ManifestFetch {
            url: url.clone(),
            config_source: source.clone(),
            detail,
        })?;

    // 2. Verify the optional outer manifest pin.
    if let Some(expected) = image_ref.sha256.as_deref() {
        let actual = hex_sha256(&manifest_bytes);
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(ImageError::ManifestSha256Mismatch {
                url: url.clone(),
                config_source: source.clone(),
                expected: expected.to_ascii_lowercase(),
                actual,
            });
        }
    }

    // 3. Parse + contract-check.
    let manifest_text =
        String::from_utf8(manifest_bytes.clone()).map_err(|e| ImageError::ManifestInvalid {
            url: url.clone(),
            config_source: source.clone(),
            detail: format!("manifest is not UTF-8: {e}"),
        })?;
    let manifest = ImageManifest::parse_and_validate(&manifest_text, url, source)?;
    Ok((manifest, manifest_bytes))
}

async fn ensure_image_inner(
    data_dir: &Path,
    image_ref: &GuestImageRef,
    source: &ImageSource,
    bus: Option<&EventBus>,
    workspace_id: Option<&WorkspaceId>,
) -> std::result::Result<CachedImage, ImageError> {
    let url = &image_ref.manifest_url;
    let (manifest, manifest_bytes) = fetch_and_validate_manifest(image_ref, source).await?;

    // 4. Cache check — content-addressed by the rootfs digest, so identical
    // rootfs bytes referenced from different manifests share one entry.
    let rootfs_sha = manifest.rootfs.sha256.to_ascii_lowercase();
    let entry_dir = guest_image_cache_dir(data_dir).join(&rootfs_sha);
    let rootfs_path = entry_dir.join(format!("rootfs.{}", manifest.rootfs.format));
    let manifest_path = entry_dir.join("manifest.json");
    let cache_io = |path: &Path, e: std::io::Error| ImageError::CacheIo {
        path: path.display().to_string(),
        detail: e.to_string(),
    };
    tokio::fs::create_dir_all(&entry_dir)
        .await
        .map_err(|e| cache_io(&entry_dir, e))?;
    if tokio::fs::try_exists(&rootfs_path).await.unwrap_or(false) {
        // Refresh the cached manifest copy (the rootfs was verified when it
        // landed; the manifest may carry updated metadata).
        tokio::fs::write(&manifest_path, &manifest_bytes)
            .await
            .map_err(|e| cache_io(&manifest_path, e))?;
        return Ok(CachedImage {
            manifest,
            rootfs_path,
            manifest_path,
        });
    }

    // 5. Cache miss — download.
    publish_event(
        bus,
        workspace_id,
        SANDBOX_IMAGE_PULLING,
        json!({
            "manifestUrl": url,
            "imageId": manifest.id,
            "version": manifest.version,
        }),
    )
    .await;

    let tmp_path = entry_dir.join(format!("rootfs.{}.partial", manifest.rootfs.format));
    let client = reqwest::Client::new();
    let actual = download_hashed(&client, &manifest.rootfs.url, &tmp_path)
        .await
        .map_err(|detail| ImageError::RootfsDownload {
            url: url.clone(),
            rootfs_url: manifest.rootfs.url.clone(),
            config_source: source.clone(),
            detail,
        })?;

    // 6. Verify + atomically land in the cache.
    if actual != rootfs_sha {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(ImageError::RootfsSha256Mismatch {
            url: url.clone(),
            config_source: source.clone(),
            expected: rootfs_sha,
            actual,
        });
    }
    tokio::fs::rename(&tmp_path, &rootfs_path)
        .await
        .map_err(|e| cache_io(&rootfs_path, e))?;
    tokio::fs::write(&manifest_path, &manifest_bytes)
        .await
        .map_err(|e| cache_io(&manifest_path, e))?;

    publish_event(
        bus,
        workspace_id,
        SANDBOX_IMAGE_DOWNLOADED,
        json!({
            "manifestUrl": url,
            "imageId": manifest.id,
            "version": manifest.version,
            "sha256": rootfs_sha,
            "cachePath": rootfs_path.display().to_string(),
        }),
    )
    .await;

    Ok(CachedImage {
        manifest,
        rootfs_path,
        manifest_path,
    })
}

/// GET `url` fully into memory (manifests are small).
async fn fetch_bytes(
    client: &reqwest::Client,
    url: &str,
    timeout: std::time::Duration,
) -> std::result::Result<Vec<u8>, String> {
    let resp = client
        .get(url)
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    resp.bytes()
        .await
        .map(|b| b.to_vec())
        .map_err(|e| e.to_string())
}

/// Stream `url` to `dest`, hashing as it downloads; returns the hex sha256.
async fn download_hashed(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
) -> std::result::Result<String, String> {
    let mut resp = client
        .get(url)
        .timeout(ROOTFS_DOWNLOAD_TIMEOUT)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| format!("create {}: {e}", dest.display()))?;
    let mut hasher = Sha256::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| e.to_string())? {
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    file.flush()
        .await
        .map_err(|e| format!("flush {}: {e}", dest.display()))?;
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Hex sha256 of a fetched manifest document — the pin value clients save
/// alongside a `sandbox.microvm.image` override (`sandbox.image.check`, §5.5b).
pub fn manifest_sha256(bytes: &[u8]) -> String {
    hex_sha256(bytes)
}

/// Hex sha256 of a byte slice.
fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Publish a `sandbox:image:*` event when a bus is wired; failures are logged
/// and never fail the pipeline.
async fn publish_event(
    bus: Option<&EventBus>,
    workspace_id: Option<&WorkspaceId>,
    event_type: &str,
    mut data: serde_json::Value,
) {
    let Some(bus) = bus else { return };
    if let (Some(ws), Some(obj)) = (workspace_id, data.as_object_mut()) {
        obj.insert("workspaceId".to_string(), json!(ws.to_string()));
    }
    let ev = NewEvent {
        workspace_id: workspace_id
            .cloned()
            .unwrap_or_else(|| WorkspaceId::from_string(String::new())),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    };
    if let Err(e) = bus.publish(&ev).await {
        tracing::warn!(error = %e, event_type, "failed to publish sandbox:image event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Minimal fixture HTTP server. `build_routes` receives the base URL
    /// (`http://127.0.0.1:<port>`) so manifests can embed absolute URLs back
    /// into the same server; unknown paths 404. Returns the base URL plus a
    /// per-path hit counter (download-count assertions for the cache test).
    fn serve_fixtures(
        build_routes: impl FnOnce(&str) -> HashMap<String, Vec<u8>>,
    ) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let routes = build_routes(&base);
        let rootfs_hits = Arc::new(AtomicUsize::new(0));
        let hits = rootfs_hits.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                if path.contains("rootfs") {
                    hits.fetch_add(1, Ordering::SeqCst);
                }
                match routes.get(&path) {
                    Some(body) => {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(body);
                    }
                    None => {
                        let _ = stream.write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        );
                    }
                }
            }
        });
        (base, rootfs_hits)
    }

    fn manifest_json(rootfs_url: &str, rootfs_sha: &str) -> serde_json::Value {
        json!({
            "schema": 1,
            "id": "intent-guest-base",
            "version": "0.1.0",
            "arch": "aarch64",
            "rootfs": {
                "url": rootfs_url,
                "format": "tar.xz",
                "sha256": rootfs_sha,
            },
            "vsockExec": {
                "init": "/usr/local/bin/intent-init",
                "port": 4088,
                "protocol": "intent-exec/1",
            },
            "providers": { "auggie": true, "opencode": true, "unsloth": false },
            "tools": { "node": "24.13.0", "git": "2.39.5" },
        })
    }

    fn image_ref(base: &str) -> GuestImageRef {
        GuestImageRef {
            manifest_url: format!("{base}/manifest.json"),
            sha256: None,
        }
    }

    /// Fixture server for the standard happy layout: `/manifest.json`
    /// pointing at `/rootfs.tar.xz`.
    fn serve_image(rootfs: Vec<u8>) -> (String, Arc<AtomicUsize>) {
        let sha = hex_sha256(&rootfs);
        serve_fixtures(move |base| {
            HashMap::from([
                (
                    "/manifest.json".to_string(),
                    serde_json::to_vec(&manifest_json(&format!("{base}/rootfs.tar.xz"), &sha))
                        .unwrap(),
                ),
                ("/rootfs.tar.xz".to_string(), rootfs),
            ])
        })
    }

    #[tokio::test]
    async fn download_verify_cache_happy_path() {
        let rootfs: Vec<u8> = b"fake-rootfs-bytes".to_vec();
        let (base, _hits) = serve_image(rootfs.clone());
        let tmp = tempfile::tempdir().unwrap();

        let cached = ensure_image(
            tmp.path(),
            &image_ref(&base),
            &ImageSource::BuiltinPin,
            None,
            None,
        )
        .await
        .expect("happy path");
        assert_eq!(cached.manifest.id, "intent-guest-base");
        assert_eq!(cached.manifest.providers.get("auggie"), Some(&true));
        assert_eq!(cached.manifest.providers.get("unsloth"), Some(&false));
        assert_eq!(cached.manifest.vsock_exec.port, 4088);
        assert!(cached.rootfs_path.exists());
        assert!(cached.manifest_path.exists());
        assert_eq!(std::fs::read(&cached.rootfs_path).unwrap(), rootfs);
        // Cache path is content-addressed by the rootfs digest.
        assert!(cached
            .rootfs_path
            .display()
            .to_string()
            .contains(&hex_sha256(&rootfs)));
    }

    #[tokio::test]
    async fn second_resolve_hits_cache_not_network() {
        let rootfs: Vec<u8> = b"cache-me".to_vec();
        let (base, hits) = serve_image(rootfs.clone());
        let tmp = tempfile::tempdir().unwrap();
        let r = image_ref(&base);

        ensure_image(tmp.path(), &r, &ImageSource::BuiltinPin, None, None)
            .await
            .expect("first fetch");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let cached = ensure_image(tmp.path(), &r, &ImageSource::BuiltinPin, None, None)
            .await
            .expect("second fetch");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "rootfs must not re-download"
        );
        assert!(cached.rootfs_path.exists());
    }

    #[tokio::test]
    async fn rootfs_sha_mismatch_is_structured_and_leaves_no_cache_entry() {
        let rootfs: Vec<u8> = b"actual-bytes".to_vec();
        let wrong_sha = hex_sha256(b"different-bytes");
        let (base, _hits) = serve_fixtures(|base| {
            HashMap::from([
                (
                    "/manifest.json".to_string(),
                    serde_json::to_vec(&manifest_json(
                        &format!("{base}/rootfs.tar.xz"),
                        &wrong_sha,
                    ))
                    .unwrap(),
                ),
                ("/rootfs.tar.xz".to_string(), rootfs),
            ])
        });
        let tmp = tempfile::tempdir().unwrap();

        let err = ensure_image(
            tmp.path(),
            &image_ref(&base),
            &ImageSource::RepoConfig,
            None,
            None,
        )
        .await
        .expect_err("sha mismatch must fail");
        assert!(matches!(err, ImageError::RootfsSha256Mismatch { .. }));
        assert!(err.to_string().contains(".intent/config.json"));
        let entry = guest_image_cache_dir(tmp.path()).join(&wrong_sha);
        assert!(!entry.join("rootfs.tar.xz").exists());
        assert!(!entry.join("rootfs.tar.xz.partial").exists());
    }

    #[tokio::test]
    async fn manifest_pin_mismatch_fails() {
        let rootfs: Vec<u8> = b"pinned".to_vec();
        let (base, _hits) = serve_image(rootfs);
        let tmp = tempfile::tempdir().unwrap();
        let r = GuestImageRef {
            manifest_url: format!("{base}/manifest.json"),
            sha256: Some(hex_sha256(b"not-the-manifest")),
        };

        let err = ensure_image(tmp.path(), &r, &ImageSource::ProfileDefault, None, None)
            .await
            .expect_err("manifest pin mismatch must fail");
        assert!(matches!(err, ImageError::ManifestSha256Mismatch { .. }));
        assert!(err.to_string().contains("sandbox profile default image"));
    }

    /// The dry-run half (`sandbox.image.check` engine): validates the
    /// manifest without downloading the rootfs or touching any cache.
    #[tokio::test]
    async fn fetch_and_validate_manifest_is_a_dry_run() {
        let rootfs: Vec<u8> = b"never-downloaded".to_vec();
        let (base, rootfs_hits) = serve_image(rootfs);

        let (manifest, bytes) =
            fetch_and_validate_manifest(&image_ref(&base), &ImageSource::ProfileDefault)
                .await
                .expect("valid manifest");
        assert_eq!(manifest.id, "intent-guest-base");
        assert_eq!(manifest.version, "0.1.0");
        assert_eq!(manifest.arch, "aarch64");
        assert!(!bytes.is_empty());
        // Dry run: the rootfs was never fetched.
        assert_eq!(rootfs_hits.load(Ordering::SeqCst), 0);

        // Pin mismatch surfaces as the structured error.
        let pinned = GuestImageRef {
            manifest_url: format!("{base}/manifest.json"),
            sha256: Some(hex_sha256(b"wrong")),
        };
        let err = fetch_and_validate_manifest(&pinned, &ImageSource::ProfileDefault)
            .await
            .expect_err("pin mismatch must fail");
        assert!(matches!(err, ImageError::ManifestSha256Mismatch { .. }));

        // Unreachable host surfaces as ManifestFetch.
        let unreachable = GuestImageRef {
            manifest_url: "http://127.0.0.1:1/manifest.json".to_string(),
            sha256: None,
        };
        let err = fetch_and_validate_manifest(&unreachable, &ImageSource::ProfileDefault)
            .await
            .expect_err("unreachable must fail");
        assert!(matches!(err, ImageError::ManifestFetch { .. }));
    }

    #[tokio::test]
    async fn non_conforming_manifest_names_config_source() {
        let mut bad = manifest_json("http://example.invalid/rootfs.tar.xz", &"0".repeat(64));
        bad["arch"] = json!("x86_64");
        let (base, _hits) = serve_fixtures(|_| {
            HashMap::from([(
                "/manifest.json".to_string(),
                serde_json::to_vec(&bad).unwrap(),
            )])
        });
        let tmp = tempfile::tempdir().unwrap();

        let err = ensure_image(
            tmp.path(),
            &image_ref(&base),
            &ImageSource::RepoConfig,
            None,
            None,
        )
        .await
        .expect_err("wrong arch must fail");
        assert!(matches!(err, ImageError::NonConforming { .. }));
        let msg = err.to_string();
        assert!(msg.contains("x86_64"));
        assert!(msg.contains(".intent/config.json (executionEnvironment.image)"));
        assert_eq!(err.config_source().map(|s| s.as_str()), Some("repo-config"));
    }

    #[tokio::test]
    async fn manifest_fetch_failure_is_structured() {
        let (base, _hits) = serve_fixtures(|_| HashMap::new());
        let tmp = tempfile::tempdir().unwrap();

        let err = ensure_image(
            tmp.path(),
            &image_ref(&base),
            &ImageSource::BuiltinPin,
            None,
            None,
        )
        .await
        .expect_err("404 manifest must fail");
        assert!(matches!(err, ImageError::ManifestFetch { .. }));
        assert!(err.to_string().contains("built-in image pin"));
    }

    #[test]
    fn validate_rejects_each_contract_violation() {
        let sha = "a".repeat(64);
        let good: ImageManifest =
            serde_json::from_value(manifest_json("http://example.invalid/rootfs.tar.xz", &sha))
                .unwrap();
        let src = ImageSource::BuiltinPin;
        good.validate("u", &src).expect("baseline manifest valid");

        type Mutator = Box<dyn Fn(&mut ImageManifest)>;
        let cases: Vec<(&str, Mutator)> = vec![
            ("schema", Box::new(|m| m.schema = 2)),
            ("arch", Box::new(|m| m.arch = "riscv64".into())),
            ("id", Box::new(|m| m.id = " ".into())),
            ("version", Box::new(|m| m.version = String::new())),
            ("format", Box::new(|m| m.rootfs.format = "ext4".into())),
            ("sha256", Box::new(|m| m.rootfs.sha256 = "zzz".into())),
            ("rootfs url", Box::new(|m| m.rootfs.url = String::new())),
            (
                "protocol",
                Box::new(|m| m.vsock_exec.protocol = "intent-exec/9".into()),
            ),
            (
                "init",
                Box::new(|m| m.vsock_exec.init = "relative/path".into()),
            ),
            ("port", Box::new(|m| m.vsock_exec.port = 0)),
        ];
        for (name, mutate) in cases {
            let mut m = good.clone();
            mutate(&mut m);
            let err = m.validate("u", &src).expect_err(name);
            assert!(matches!(err, ImageError::NonConforming { .. }), "{name}");
        }
    }

    #[test]
    fn manifest_tolerates_unknown_fields() {
        let mut v = manifest_json("http://example.invalid/rootfs.tar.xz", &"b".repeat(64));
        v["futureField"] = json!({"nested": true});
        let m: ImageManifest = serde_json::from_value(v).unwrap();
        assert!(m.extra.contains_key("futureField"));
        m.validate("u", &ImageSource::BuiltinPin).unwrap();
    }

    #[test]
    fn resolution_order_repo_then_profile_then_builtin() {
        let repo_image = GuestImageRef {
            manifest_url: "https://repo.example/m.json".into(),
            sha256: None,
        };
        let profile_image = GuestImageRef {
            manifest_url: "https://profile.example/m.json".into(),
            sha256: None,
        };
        let repo_cfg_with = RepoConfig {
            execution_environment: Some(intent_core::ExecutionEnvironmentRepoConfig {
                image: Some(repo_image.clone()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let repo_cfg_empty = RepoConfig::default();

        let (r, s) = resolve_image_ref(&repo_cfg_with, Some(&profile_image));
        assert_eq!(r, repo_image);
        assert_eq!(s, ImageSource::RepoConfig);

        let (r, s) = resolve_image_ref(&repo_cfg_empty, Some(&profile_image));
        assert_eq!(r, profile_image);
        assert_eq!(s, ImageSource::ProfileDefault);

        let (r, s) = resolve_image_ref(&repo_cfg_empty, None);
        assert_eq!(r.manifest_url, builtin_manifest_url());
        assert_eq!(s, ImageSource::BuiltinPin);
    }

    #[tokio::test]
    async fn events_emitted_on_pull_and_error() {
        use crate::events::filter::SubscriptionFilter;
        use intent_store::Store;

        let db = std::env::temp_dir().join(format!("intentd-sbimg-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&db).await.expect("open store");
        let bus = EventBus::new(store);
        let mut sub = bus.subscribe(SubscriptionFilter {
            event_types: vec!["sandbox:image:*".to_string()],
            ..Default::default()
        });

        let rootfs: Vec<u8> = b"event-rootfs".to_vec();
        let (base, _hits) = serve_image(rootfs);
        let tmp = tempfile::tempdir().unwrap();
        let ws = WorkspaceId::from("ws-img");

        ensure_image(
            tmp.path(),
            &image_ref(&base),
            &ImageSource::BuiltinPin,
            Some(&bus),
            Some(&ws),
        )
        .await
        .expect("download succeeds");

        let mut seen = Vec::new();
        while seen.len() < 2 {
            let batch = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
                .await
                .expect("events within 5s")
                .expect("bus alive");
            seen.extend(batch);
        }
        assert_eq!(seen[0].event_type, SANDBOX_IMAGE_PULLING);
        assert_eq!(
            seen[0].data["manifestUrl"],
            json!(format!("{base}/manifest.json"))
        );
        assert_eq!(seen[0].data["workspaceId"], json!("ws-img"));
        assert_eq!(seen[1].event_type, SANDBOX_IMAGE_DOWNLOADED);
        assert_eq!(seen[1].data["imageId"], json!("intent-guest-base"));
        assert!(seen[1].data["cachePath"]
            .as_str()
            .unwrap()
            .contains("guest-images"));

        // Error path: 404 manifest emits sandbox:image:error naming the source.
        let (bad_base, _h) = serve_fixtures(|_| HashMap::new());
        let bad_ref = image_ref(&bad_base);
        ensure_image(
            tmp.path(),
            &bad_ref,
            &ImageSource::RepoConfig,
            Some(&bus),
            Some(&ws),
        )
        .await
        .expect_err("404 must fail");
        let batch = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("error event within 5s")
            .expect("bus alive");
        let err_ev = &batch[0];
        assert_eq!(err_ev.event_type, SANDBOX_IMAGE_ERROR);
        assert_eq!(err_ev.data["configSource"], json!("repo-config"));
        assert!(err_ev.data["error"].as_str().unwrap().contains("HTTP 404"));

        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
        }
    }

    #[test]
    fn repo_config_execution_environment_round_trips() {
        let json_text = r#"{
            "branchPrefix": "feature/",
            "executionEnvironment": {
                "image": { "manifestUrl": "https://x.example/m.json", "sha256": null },
                "futureKnob": 7
            }
        }"#;
        let cfg: RepoConfig = serde_json::from_str(json_text).unwrap();
        let image = cfg
            .execution_environment
            .as_ref()
            .and_then(|ee| ee.image.as_ref())
            .expect("image parsed");
        assert_eq!(image.manifest_url, "https://x.example/m.json");
        assert!(cfg
            .execution_environment
            .as_ref()
            .unwrap()
            .extra
            .contains_key("futureKnob"));
        let back = serde_json::to_value(&cfg).unwrap();
        assert_eq!(back["executionEnvironment"]["futureKnob"], json!(7));
    }
}
