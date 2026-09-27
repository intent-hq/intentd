//! Focused macOS diagnostics: inspect local files, never execute a provider.
//!
//! macOS kqueue rejects `NOTE_TRACK`/`NOTE_CHILD`, and launchd only cleans the
//! original process group. Neither owns detached descendants after leader exit:
//! <https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_event.c>
//! <https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5>
//! Until a stronger execution capability exists, version/catalog probes are
//! rejected before authentication or temporary process state is prepared. This
//! module validates only the selected adapter's declared package/bin identity;
//! it does not approximate Node resolution or attribute a PATH Codex runtime.

use std::io;
use std::path::{Path, PathBuf};

use tokio::io::AsyncReadExt;

use super::super::{
    parse_version, safe_text, CodexInspection, CodexLaunch, UnknownReason, VersionKind,
    LOCAL_TIMEOUT,
};

const MANIFEST_LIMIT: usize = 64 * 1024;

/// Read-only inspection is portable so its validation can also run on Linux
/// and Windows. Only macOS routes ordinary inspection through this capability.
pub(in crate::codex_diagnostics) async fn inspect_metadata(
    launch: &CodexLaunch,
    adapter: &Path,
    pin: Option<&str>,
) -> CodexInspection {
    let mut result = launch.unknown(UnknownReason::UnsupportedPlatform);
    result.report.adapter_path = Some(safe_text(&adapter.to_string_lossy()));
    if let Ok(Ok((path, version))) =
        tokio::time::timeout(LOCAL_TIMEOUT, package_metadata(adapter, pin)).await
    {
        result.report.adapter_path = Some(safe_text(&path.to_string_lossy()));
        result.report.adapter_package_version = version;
    }
    result
}

async fn package_metadata(
    adapter: &Path,
    pin: Option<&str>,
) -> io::Result<(PathBuf, Option<String>)> {
    let adapter = tokio::fs::canonicalize(adapter).await?;
    if !matches!(
        adapter.extension().and_then(|s| s.to_str()),
        Some("js" | "cjs" | "mjs")
    ) {
        return Err(invalid_metadata());
    }
    let mut header = Vec::new();
    open_regular(&adapter)
        .await?
        .take(128)
        .read_to_end(&mut header)
        .await?;
    let first_line = header
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    if first_line.strip_suffix(b"\r").unwrap_or(first_line) != b"#!/usr/bin/env node" {
        return Err(invalid_metadata());
    }
    let mut root = adapter.parent().ok_or_else(invalid_metadata)?;
    for _ in 0..4 {
        match open_regular(&root.join("package.json")).await {
            Ok(file) => {
                if file.metadata().await?.len() > MANIFEST_LIMIT as u64 {
                    return Err(invalid_metadata());
                }
                let mut bytes = Vec::new();
                file.take((MANIFEST_LIMIT + 1) as u64)
                    .read_to_end(&mut bytes)
                    .await?;
                if bytes.len() > MANIFEST_LIMIT {
                    return Err(invalid_metadata());
                }
                let manifest: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|_| invalid_metadata())?;
                let bin = manifest["bin"]
                    .as_str()
                    .or_else(|| manifest["bin"]["codex-acp"].as_str())
                    .ok_or_else(invalid_metadata)?;
                let version = manifest["version"].as_str();
                if manifest["name"] != "@agentclientprotocol/codex-acp"
                    || pin.is_some_and(|pin| version != Some(pin))
                    || tokio::fs::canonicalize(root.join(bin)).await? != adapter
                {
                    return Err(invalid_metadata());
                }
                // The same strict version allowlist protects measured output,
                // but this field always remains explicitly package metadata.
                let version = version.and_then(|v| {
                    parse_version(v.as_bytes(), VersionKind::Adapter).filter(|safe| safe == v)
                });
                return Ok((adapter, version));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        root = root.parent().ok_or_else(invalid_metadata)?;
    }
    Err(invalid_metadata())
}

async fn open_regular(path: &Path) -> io::Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    // Opening a FIFO must not block before we can reject its file type. Check
    // the opened descriptor rather than a racy path metadata preflight.
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_NONBLOCK);
    let file = options.open(path).await?;
    if !file.metadata().await?.is_file() {
        return Err(invalid_metadata());
    }
    Ok(file)
}

fn invalid_metadata() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "unverified adapter metadata")
}

/// No macOS command is currently eligible for process ownership. An
/// uninhabited owner makes it impossible to report successful cleanup of a
/// command we could not contain. The shared guard rejects before acquiring a
/// resource lease; this lower boundary also refuses accidental direct callers.
#[cfg(target_os = "macos")]
pub(super) enum Ownership {}

#[cfg(target_os = "macos")]
#[expect(
    clippy::unused_async,
    reason = "matches the cfg-selected platform startup interface"
)]
pub(super) async fn spawn(command: tokio::process::Command) -> io::Result<super::Started> {
    drop(command);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        UnknownReason::UnsupportedPlatform.message(),
    ))
}

#[cfg(target_os = "macos")]
impl Ownership {
    #[expect(
        clippy::unused_async,
        reason = "matches the cfg-selected platform wait interface"
    )]
    pub(super) async fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        match *self {}
    }

    #[expect(
        clippy::unused_async,
        reason = "matches the cfg-selected platform cleanup interface"
    )]
    pub(super) async fn cleanup(self) -> io::Result<()> {
        match self {}
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{LaunchSource, ProviderLaunch, RuntimeSource, VersionMeasurement};
    use super::*;
    use intent_providers::discover::{ProviderBinary, ProviderBinarySource};
    use serde_json::json;

    struct Fixture {
        root: tempfile::TempDir,
        adapter: PathBuf,
        manifest: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let root = crate::test_support::test_tempdir("codex-macos-metadata");
            let adapter = root.path().join("package/dist/adapter.js");
            let manifest = root.path().join("package/package.json");
            std::fs::create_dir_all(adapter.parent().unwrap()).unwrap();
            // Large entrypoints still need only a bounded header read. Nothing
            // here or on PATH may be executed, even with a plausible version.
            std::fs::write(
                &adapter,
                format!("#!/usr/bin/env node\n{}", "x".repeat(MANIFEST_LIMIT + 1)),
            )
            .unwrap();
            std::fs::write(&manifest, json!({"name":"@agentclientprotocol/codex-acp", "version":"2.4.6", "bin":{"codex-acp":"dist/adapter.js"}}).to_string()).unwrap();
            Self {
                root,
                adapter,
                manifest,
            }
        }

        fn launch(&self) -> CodexLaunch {
            CodexLaunch {
                selection: ProviderLaunch::Local(ProviderBinary {
                    path: self.adapter.clone(),
                    source: ProviderBinarySource::SettingsOverride,
                }),
                path: self.root.path().as_os_str().to_owned(),
                codex_path: Some(self.root.path().join("misleading-codex").into_os_string()),
            }
        }
    }

    #[tokio::test]
    async fn macos_metadata_is_declared_not_measured_and_never_attributes_path_runtime() {
        let fixture = Fixture::new();
        let report = inspect_metadata(&fixture.launch(), &fixture.adapter, None).await;
        assert_eq!(report.report.launch_source, LaunchSource::SettingsOverride);
        assert_eq!(
            report.report.adapter_package_version.as_deref(),
            Some("2.4.6")
        );
        assert_eq!(
            report.report.adapter_version,
            VersionMeasurement::Unknown(UnknownReason::UnsupportedPlatform)
        );
        assert_eq!(
            report.report.runtime_version,
            VersionMeasurement::Unknown(UnknownReason::UnsupportedPlatform)
        );
        assert_eq!(report.report.runtime_source, RuntimeSource::Unknown);
        assert!(report.report.runtime_path.is_none());
        assert!(report.runtime.is_none());
        assert!(report.report.removes_codex_overrides);
    }

    #[tokio::test]
    async fn macos_metadata_requires_matching_package_bin_and_pin() {
        let fixture = Fixture::new();
        assert!(package_metadata(&fixture.adapter, Some("2.4.6"))
            .await
            .is_ok());
        assert!(package_metadata(&fixture.adapter, Some("1.0.0"))
            .await
            .is_err());
        for manifest in [
            json!({"name":"unrelated", "version":"2.4.6", "bin":"dist/adapter.js"}),
            json!({"name":"@agentclientprotocol/codex-acp", "version":"2.4.6", "bin":"dist/other.js"}),
        ] {
            std::fs::write(&fixture.manifest, manifest.to_string()).unwrap();
            assert!(package_metadata(&fixture.adapter, None).await.is_err());
        }
    }

    #[tokio::test]
    async fn macos_metadata_rejects_opaque_or_unreadable_packages_and_sensitive_versions() {
        let fixture = Fixture::new();
        for text in ["not-json".into(), "x".repeat(MANIFEST_LIMIT + 1), json!({"name":"@agentclientprotocol/codex-acp","version":"credential-canary user@example.invalid", "bin":"dist/adapter.js"}).to_string()] {
            std::fs::write(&fixture.manifest, text).unwrap();
            let report = inspect_metadata(&fixture.launch(), &fixture.adapter, None).await;
            assert!(report.report.adapter_package_version.is_none());
            let serialized = serde_json::to_string(&report.report).unwrap();
            assert!(!serialized.contains("credential-canary"));
            assert!(!serialized.contains("example.invalid"));
        }
        std::fs::write(&fixture.adapter, "#!/bin/sh\necho credential-canary\n").unwrap();
        assert!(package_metadata(&fixture.adapter, None).await.is_err());
    }

    #[cfg(unix)]
    fn fifo(path: &Path) {
        use std::os::unix::ffi::OsStrExt;

        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: path is NUL-terminated and remains live for the syscall.
        assert_eq!(unsafe { nix::libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn macos_metadata_follows_selected_symlink_and_rejects_fifos_without_waiting() {
        let fixture = Fixture::new();
        let link = fixture.root.path().join("codex-acp");
        std::os::unix::fs::symlink(&fixture.adapter, &link).unwrap();
        assert_eq!(
            package_metadata(&link, None).await.unwrap().0,
            std::fs::canonicalize(&fixture.adapter).unwrap()
        );
        std::fs::remove_file(&fixture.manifest).unwrap();
        fifo(&fixture.manifest);
        assert!(
            tokio::time::timeout(LOCAL_TIMEOUT, package_metadata(&link, None))
                .await
                .unwrap()
                .is_err()
        );
        std::fs::remove_file(&fixture.adapter).unwrap();
        fifo(&fixture.adapter);
        assert!(
            tokio::time::timeout(LOCAL_TIMEOUT, package_metadata(&link, None))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_rejects_process_start_before_retaining_home_or_dependency() {
        let fixture = Fixture::new();
        let marker = fixture.root.path().join("must-not-exist");
        let mut command = tokio::process::Command::new("/usr/bin/touch");
        command.arg(&marker);
        let home = crate::test_support::test_tempdir("codex-macos-rejected");
        let home_path = home.path().to_owned();
        let owner_home = crate::test_support::test_tempdir("codex-macos-owner");
        let owner_path = owner_home.path().to_owned();
        let owner = super::super::resources::ProbeHome::new(owner_home);
        let result = super::super::ProbeProcess::spawn_with_dependency(
            command,
            home,
            Some(owner.dependency()),
        )
        .await;
        assert!(matches!(result, Err(UnknownReason::UnsupportedPlatform)));
        owner.remove().unwrap();
        assert!(!home_path.exists());
        assert!(!owner_path.exists());
        assert!(!marker.exists());
        let mut command = tokio::process::Command::new("/usr/bin/touch");
        command.arg(&marker);
        assert!(
            matches!(spawn(command).await, Err(error) if error.kind() == io::ErrorKind::Unsupported)
        );
        assert!(!marker.exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_catalogs_are_explicitly_unsupported_and_inconclusive() {
        use super::super::super::{CatalogFailure, CatalogOutcome, ModelObservation};
        let fixture = Fixture::new();
        let report = fixture.launch().fresh_catalogs().await;
        assert_eq!(
            report.runtime.adapter_package_version.as_deref(),
            Some("2.4.6")
        );
        assert_eq!(
            report.acp,
            CatalogOutcome::Failed(CatalogFailure::UnsupportedPlatform)
        );
        assert_eq!(
            report.raw,
            CatalogOutcome::Failed(CatalogFailure::UnsupportedPlatform)
        );
        assert_eq!(report.observe("any-model"), ModelObservation::Inconclusive);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_rejects_authentication_capture_and_local_output_at_entry() {
        use super::super::super::{catalog_io::Authentication, local_output, CatalogFailure};
        let fixture = Fixture::new();
        assert!(matches!(
            Authentication::capture(&fixture.launch()).await,
            Err(CatalogFailure::UnsupportedPlatform)
        ));
        let marker = fixture.root.path().join("must-not-exist");
        let mut command = tokio::process::Command::new("/usr/bin/touch");
        command.arg(&marker);
        assert_eq!(
            local_output(command, fixture.root.path().as_os_str(), LOCAL_TIMEOUT).await,
            Err(UnknownReason::UnsupportedPlatform)
        );
        assert!(!marker.exists());
    }
}
