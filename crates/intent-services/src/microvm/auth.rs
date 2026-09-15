//! Credential staging for microVM guests (monorepo#1120, EE-5).
//!
//! Settled auth design (issue #1120, 2026-07-30 comment): the HOST is the
//! only refresher; guests are read-only consumers. Plain-file auth state is
//! COPIED into the guest home (`<vm rootfs>/root/…`) at spawn — never shared
//! via virtio-fs, never synced guest→host. A host-side watcher pushes
//! rotations into running VMs (atomic temp+rename). Teardown scrubs
//! everything by deleting the per-VM rootfs clone.
//!
//! claude-code is the exception (macOS Keychain can't be file-staged): the
//! `providers.claudeCodeOauthToken` sensitive setting is injected per-spawn
//! as `CLAUDE_CODE_OAUTH_TOKEN`, plus a minimal guest `~/.claude.json` with
//! `hasCompletedOnboarding: true`. Git identity is copied WITHOUT the
//! osxkeychain credential helper; `gh auth setup-git` runs in guest setup.

use std::path::{Path, PathBuf};

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::task::JoinHandle;

use super::MicrovmError;

/// One host auth file staged into the guest home. `guest_rel` is relative to
/// the guest home directory (`/root`).
#[derive(Debug, Clone)]
pub struct StagedAuthFile {
    pub host: PathBuf,
    pub guest_rel: &'static str,
    /// Absent host files are skipped silently (provider simply unauthenticated
    /// in-guest) unless another file of the same provider was staged.
    pub provider: &'static str,
}

/// The plain-file staging catalog (validated by FC-Spike 5): host path →
/// guest-home-relative destination. `home` is the host home directory.
#[must_use]
pub fn staging_catalog(home: &Path) -> Vec<StagedAuthFile> {
    let f = |host: PathBuf, guest_rel: &'static str, provider: &'static str| StagedAuthFile {
        host,
        guest_rel,
        provider,
    };
    vec![
        // auggie — session file consumed via --augment-session-json.
        f(
            home.join(".augment/session.json"),
            ".augment/session.json",
            "auggie",
        ),
        // codex — auth.json only (config.toml carries host paths; not copied).
        f(home.join(".codex/auth.json"), ".codex/auth.json", "codex"),
        // pi
        f(
            home.join(".pi/agent/auth.json"),
            ".pi/agent/auth.json",
            "pi",
        ),
        // droid — split key/file pair.
        f(
            home.join(".factory/auth.v2.file"),
            ".factory/auth.v2.file",
            "droid",
        ),
        f(
            home.join(".factory/auth.v2.key"),
            ".factory/auth.v2.key",
            "droid",
        ),
        // grok — auth + optional agent id.
        f(home.join(".grok/auth.json"), ".grok/auth.json", "grok"),
        f(home.join(".grok/agent_id"), ".grok/agent_id", "grok"),
        // gh — hosts.yml staged like provider auth; `gh auth setup-git` runs
        // in guest setup so git picks up the gh credential helper in-guest.
        f(
            home.join(".config/gh/hosts.yml"),
            ".config/gh/hosts.yml",
            "gh",
        ),
    ]
}

/// Copy one staged file into the guest home with a 0600 atomic temp+rename
/// (the rotation watcher reuses this for pushes into running VMs). Returns
/// `false` without writing when the host file is absent OR the destination
/// already holds identical bytes — refreshers rewrite credential files on a
/// timer without changing them, and re-pushing those would spam the rotation
/// log and guest I/O for nothing.
///
/// # Errors
///
/// Returns `MicrovmError::AuthStage` when a filesystem step fails.
pub fn stage_file(entry: &StagedAuthFile, guest_home: &Path) -> Result<bool, MicrovmError> {
    if !entry.host.is_file() {
        return Ok(false);
    }
    let dst = guest_home.join(entry.guest_rel);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| MicrovmError::AuthStage(format!("mkdir {}: {e}", parent.display())))?;
    }
    let bytes = std::fs::read(&entry.host)
        .map_err(|e| MicrovmError::AuthStage(format!("read {}: {e}", entry.host.display())))?;
    if matches!(std::fs::read(&dst), Ok(existing) if existing == bytes) {
        return Ok(false);
    }
    let tmp = dst.with_extension("intentd-staging");
    std::fs::write(&tmp, &bytes)
        .map_err(|e| MicrovmError::AuthStage(format!("write {}: {e}", tmp.display())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, &dst)
        .map_err(|e| MicrovmError::AuthStage(format!("rename to {}: {e}", dst.display())))?;
    Ok(true)
}

/// Stage the full catalog into `guest_home`, returning the entries that were
/// actually staged (host file present). Presence — not `stage_file`'s
/// "wrote bytes" bool — decides membership, so an entry whose destination
/// already matched still gets rotation-watched.
///
/// # Errors
///
/// Returns `MicrovmError::AuthStage` when staging an entry fails.
pub fn stage_all(home: &Path, guest_home: &Path) -> Result<Vec<StagedAuthFile>, MicrovmError> {
    let mut staged = Vec::new();
    for entry in staging_catalog(home) {
        let host_present = entry.host.is_file();
        stage_file(&entry, guest_home)?;
        if host_present {
            staged.push(entry);
        }
    }
    Ok(staged)
}

/// Host `.gitconfig` filtered for guest use: any `helper = …osxkeychain…`
/// line is dropped (the macOS keychain does not exist in-guest; gh's helper
/// is configured by `gh auth setup-git` during guest setup instead).
#[must_use]
pub fn filtered_gitconfig(host_gitconfig: &str) -> String {
    host_gitconfig
        .lines()
        .filter(|line| {
            let t = line.trim();
            !(t.starts_with("helper") && t.contains("osxkeychain"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Stage the host git identity: `~/.gitconfig` copied through
/// [`filtered_gitconfig`]. Missing host file ⇒ no-op.
///
/// # Errors
///
/// Returns `MicrovmError::AuthStage` when writing the filtered file fails.
pub fn stage_gitconfig(home: &Path, guest_home: &Path) -> Result<(), MicrovmError> {
    let src = home.join(".gitconfig");
    let Ok(content) = std::fs::read_to_string(&src) else {
        return Ok(());
    };
    let dst = guest_home.join(".gitconfig");
    std::fs::write(&dst, filtered_gitconfig(&content))
        .map_err(|e| MicrovmError::AuthStage(format!("write {}: {e}", dst.display())))?;
    Ok(())
}

/// Debounce window for rotation pushes (editors/refreshers write in bursts).
const ROTATION_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(500);

/// Host-side rotation watcher: watches the parent directories of the staged
/// host files and re-copies a changed file into the guest home (atomic
/// temp+rename via [`stage_file`]). Directory-level watches survive the
/// rename/atomic-save pattern token refreshers use (same rationale as
/// `crate::config_watcher`). Dropping the handle stops the watcher.
pub struct RotationWatcher {
    /// Held for its Drop (stops the notify backend).
    _watcher: RecommendedWatcher,
    task: JoinHandle<()>,
}

impl Drop for RotationWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start a rotation watcher pushing `staged` entries from the host into
/// `guest_home`. Only entries that were staged at spawn are watched — a file
/// that did not exist then cannot rotate into the VM (next spawn picks it up).
///
/// # Errors
///
/// Returns `MicrovmError::AuthStage` when the filesystem watcher cannot start.
pub fn watch_rotations(
    staged: Vec<StagedAuthFile>,
    guest_home: PathBuf,
) -> Result<RotationWatcher, MicrovmError> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            for path in event.paths {
                let _ = tx.send(path);
            }
        }
    })
    .map_err(|e| MicrovmError::AuthStage(format!("create rotation watcher: {e}")))?;

    let mut dirs: Vec<PathBuf> = staged
        .iter()
        .filter_map(|e| e.host.parent().map(Path::to_path_buf))
        .collect();
    dirs.sort();
    dirs.dedup();
    for dir in &dirs {
        watcher
            .watch(dir, RecursiveMode::NonRecursive)
            .map_err(|e| MicrovmError::AuthStage(format!("watch {}: {e}", dir.display())))?;
    }

    let task = tokio::spawn(async move {
        loop {
            let Some(first) = rx.recv().await else { break };
            // Debounce: coalesce the burst, remembering every touched path.
            let mut touched = vec![first];
            loop {
                match tokio::time::timeout(ROTATION_DEBOUNCE, rx.recv()).await {
                    Ok(Some(p)) => touched.push(p),
                    Ok(None) => return,
                    Err(_) => break,
                }
            }
            for entry in &staged {
                if touched.contains(&entry.host) {
                    match stage_file(entry, &guest_home) {
                        Ok(true) => tracing::info!(
                            provider = entry.provider,
                            guest_rel = entry.guest_rel,
                            "pushed rotated credential into microVM"
                        ),
                        Ok(false) => {}
                        Err(e) => tracing::warn!(
                            provider = entry.provider,
                            error = %e,
                            "failed to push rotated credential into microVM"
                        ),
                    }
                }
            }
        }
    });

    Ok(RotationWatcher {
        _watcher: watcher,
        task,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gitconfig_filter_drops_osxkeychain_helper() {
        let host = "[user]\n\tname = Test\n\temail = t@example.com\n[credential]\n\thelper = osxkeychain\n[core]\n\teditor = vim\n";
        let filtered = filtered_gitconfig(host);
        assert!(!filtered.contains("osxkeychain"));
        assert!(filtered.contains("name = Test"));
        assert!(filtered.contains("editor = vim"));
    }

    #[test]
    fn staging_catalog_paths_match_settled_design() {
        let home = Path::new("/h");
        let catalog = staging_catalog(home);
        let rels: Vec<&str> = catalog.iter().map(|e| e.guest_rel).collect();
        for expected in [
            ".augment/session.json",
            ".codex/auth.json",
            ".pi/agent/auth.json",
            ".factory/auth.v2.file",
            ".factory/auth.v2.key",
            ".grok/auth.json",
            ".config/gh/hosts.yml",
        ] {
            assert!(rels.contains(&expected), "missing {expected}");
        }
        // opencode/unsloth are gated unavailable-in-microVM (libkrunfw#137)
        // and claude-code rides CLAUDE_CODE_OAUTH_TOKEN — none are staged.
        assert!(!rels.iter().any(|r| r.contains("opencode")));
        assert!(!rels.iter().any(|r| r.contains("claude")));
    }

    #[test]
    fn stage_file_skips_unchanged_content_and_pushes_changed() {
        let host_home = tempfile::tempdir().unwrap();
        let guest_home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(host_home.path().join(".augment")).unwrap();
        let host_file = host_home.path().join(".augment/session.json");
        std::fs::write(&host_file, b"{\"t\":1}").unwrap();
        let entry = StagedAuthFile {
            host: host_file.clone(),
            guest_rel: ".augment/session.json",
            provider: "auggie",
        };

        // First push writes.
        assert!(stage_file(&entry, guest_home.path()).unwrap());
        // Identical-content rewrite (refresher touch) is skipped.
        std::fs::write(&host_file, b"{\"t\":1}").unwrap();
        assert!(!stage_file(&entry, guest_home.path()).unwrap());
        // Real rotation pushes again.
        std::fs::write(&host_file, b"{\"t\":2}").unwrap();
        assert!(stage_file(&entry, guest_home.path()).unwrap());
        assert_eq!(
            std::fs::read(guest_home.path().join(".augment/session.json")).unwrap(),
            b"{\"t\":2}"
        );
    }

    #[test]
    fn stage_all_includes_already_matching_entries() {
        // A destination that already matches must still be returned (so the
        // rotation watcher covers it) even though no bytes were written.
        let host_home = tempfile::tempdir().unwrap();
        let guest_home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(host_home.path().join(".augment")).unwrap();
        std::fs::write(host_home.path().join(".augment/session.json"), b"{\"t\":1}").unwrap();
        std::fs::create_dir_all(guest_home.path().join(".augment")).unwrap();
        std::fs::write(
            guest_home.path().join(".augment/session.json"),
            b"{\"t\":1}",
        )
        .unwrap();

        let staged = stage_all(host_home.path(), guest_home.path()).unwrap();
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].guest_rel, ".augment/session.json");
    }

    #[test]
    fn stage_all_copies_present_files_with_0600() {
        let host_home = tempfile::tempdir().unwrap();
        let guest_home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(host_home.path().join(".augment")).unwrap();
        std::fs::write(host_home.path().join(".augment/session.json"), b"{\"t\":1}").unwrap();

        let staged = stage_all(host_home.path(), guest_home.path()).unwrap();
        assert_eq!(staged.len(), 1);
        let dst = guest_home.path().join(".augment/session.json");
        assert_eq!(std::fs::read(&dst).unwrap(), b"{\"t\":1}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&dst).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
