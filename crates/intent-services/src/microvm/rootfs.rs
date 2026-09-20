//! Rootfs materialization for microVM boots (monorepo#1120, EE-5).
//!
//! The image cache ([`crate::sandbox_image`]) holds a verified `rootfs.tar.xz`
//! per rootfs digest. Booting needs a *directory tree* the helper can expose
//! over virtio-fs, and each VM needs its own writable copy (staged
//! credentials, provider caches). Extraction is expensive, so it runs once
//! per digest into `<cache entry>/tree/`; each VM then takes a cheap `CoW`
//! reflink clone of that tree ([`intent_git::cow_clone`] — microVM workspaces
//! require a CoW-capable filesystem by design, so no byte-copy fallback).

use std::path::{Path, PathBuf};

use tokio::sync::Mutex;

use super::MicrovmError;

/// Name of the extracted-tree directory next to the cached archive.
const TREE_DIR: &str = "tree";
/// Marker file written after a successful extraction (atomicity guard: an
/// interrupted extraction leaves no marker and is redone).
const TREE_OK_MARKER: &str = ".tree-ok";

/// Serializes extraction per daemon (concurrent spawns of the same image must
/// not race the extract). Cheap: held only while checking/extracting.
static EXTRACT_LOCK: Mutex<()> = Mutex::const_new(());

/// Ensure `<entry_dir>/tree/` holds the extracted rootfs for the cached
/// archive at `rootfs_path`, extracting on first use. Returns the tree path.
///
/// # Errors
///
/// Returns `MicrovmError::Extract` when the extraction fails.
pub async fn ensure_extracted_tree(rootfs_path: &Path) -> Result<PathBuf, MicrovmError> {
    let entry_dir = rootfs_path
        .parent()
        .ok_or_else(|| MicrovmError::Extract("rootfs path has no parent".to_string()))?;
    let tree = entry_dir.join(TREE_DIR);
    let marker = entry_dir.join(TREE_OK_MARKER);

    let _guard = EXTRACT_LOCK.lock().await;
    if tokio::fs::try_exists(&marker).await.unwrap_or(false)
        && tokio::fs::try_exists(&tree).await.unwrap_or(false)
    {
        return Ok(tree);
    }
    // Containment is checked explicitly rather than trusted to tar's
    // defaults: the archive is sha-verified only against a manifest from the
    // same (repo/admin supplied) source.
    reject_escaping_members(rootfs_path).await?;
    // Stale partial tree from an interrupted extraction: remove and redo.
    if tokio::fs::try_exists(&tree).await.unwrap_or(false) {
        tokio::fs::remove_dir_all(&tree)
            .await
            .map_err(|e| MicrovmError::Extract(format!("remove partial tree: {e}")))?;
    }
    tokio::fs::create_dir_all(&tree)
        .await
        .map_err(|e| MicrovmError::Extract(format!("create tree dir: {e}")))?;

    // bsdtar (macOS) and GNU tar both auto-detect xz via -xf. Ownership lands
    // as the daemon user (libkrun maps the host uid to guest root). Device
    // entries under /dev are defensively excluded: older images shipped
    // mknod'd /dev/{null,zero,...} in the tarball, and a non-root process
    // cannot mknod on macOS (bsdtar exits non-zero, aborting the spawn). The
    // guest never needs them — intent-init mounts devtmpfs over /dev at boot.
    // Archive entries are `./dev/...`-style, so match both path forms; both
    // tars accept --exclude on extraction in this position.
    let archive = rootfs_path.to_path_buf();
    let tree_clone = tree.clone();
    let output = tokio::process::Command::new("tar")
        .arg("-xf")
        .arg(&archive)
        .arg("-C")
        .arg(&tree_clone)
        .arg("--exclude")
        .arg("./dev/*")
        .arg("--exclude")
        .arg("dev/*")
        .output()
        .await
        .map_err(|e| MicrovmError::Extract(format!("spawn tar: {e}")))?;
    if !output.status.success() {
        let _ = tokio::fs::remove_dir_all(&tree).await;
        return Err(MicrovmError::Extract(format!(
            "tar -xf {} failed ({}): {}",
            archive.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    // bsdtar drops the `./dev/` directory entry itself when its children are
    // excluded; intent-init needs /dev to exist as the devtmpfs mountpoint.
    tokio::fs::create_dir_all(tree.join("dev"))
        .await
        .map_err(|e| MicrovmError::Extract(format!("create dev mountpoint: {e}")))?;
    tokio::fs::write(&marker, b"ok")
        .await
        .map_err(|e| MicrovmError::Extract(format!("write marker: {e}")))?;
    Ok(tree)
}

/// Why an archive member name must not be extracted: an absolute path or a
/// `..` component would land outside the tree.
fn escaping_member_reason(name: &str) -> Option<&'static str> {
    if name.starts_with('/') {
        return Some("absolute path");
    }
    if name.split('/').any(|component| component == "..") {
        return Some("`..` component");
    }
    None
}

/// List the archive (`-P` so GNU tar reports member names as stored instead
/// of silently stripping the very prefixes being checked) and refuse it on
/// the first member that would escape the extraction tree.
async fn reject_escaping_members(archive: &Path) -> Result<(), MicrovmError> {
    let output = tokio::process::Command::new("tar")
        .arg("-tPf")
        .arg(archive)
        .output()
        .await
        .map_err(|e| MicrovmError::Extract(format!("spawn tar -t: {e}")))?;
    if !output.status.success() {
        return Err(MicrovmError::Extract(format!(
            "tar -tf {} failed ({}): {}",
            archive.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    for name in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(reason) = escaping_member_reason(name) {
            return Err(MicrovmError::Extract(format!(
                "refusing to extract {}: member `{name}` has an {reason}",
                archive.display()
            )));
        }
    }
    Ok(())
}

/// CoW-clone the extracted tree into the per-VM rootfs directory. `dst` must
/// not exist. No byte-copy fallback: a clone failure is a hard spawn error
/// (microVM requires `CoW` support by design).
///
/// # Errors
///
/// Returns `MicrovmError::RootfsClone` when the `CoW` clone fails.
pub async fn clone_vm_rootfs(tree: &Path, dst: &Path) -> Result<(), MicrovmError> {
    let tree = tree.to_path_buf();
    let dst_owned = dst.to_path_buf();
    tokio::task::spawn_blocking(move || intent_git::cow_clone(&tree, &dst_owned))
        .await
        .map_err(|e| MicrovmError::RootfsClone(format!("clone task panicked: {e}")))?
        .map_err(|e| MicrovmError::RootfsClone(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Extraction lands the tree + marker; a second call is a cache hit that
    /// does not re-extract (proven by mutating the tree between calls).
    #[tokio::test]
    async fn extract_is_cached_and_marker_gated() {
        let dir = tempfile::tempdir().unwrap();
        let entry = dir.path().join("digest");
        std::fs::create_dir_all(&entry).unwrap();

        // Build a tiny tar.xz fixture: one file `hello`.
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("hello"), b"hi").unwrap();
        let archive = entry.join("rootfs.tar.xz");
        let status = std::process::Command::new("tar")
            .arg("-cJf")
            .arg(&archive)
            .arg("-C")
            .arg(&src)
            .arg(".")
            .status()
            .expect("tar available");
        assert!(status.success());

        let tree = ensure_extracted_tree(&archive).await.expect("extract");
        assert!(tree.join("hello").exists());

        // Cache hit: a sentinel dropped into the tree survives the second call.
        std::fs::write(tree.join("sentinel"), b"x").unwrap();
        let tree2 = ensure_extracted_tree(&archive).await.expect("cache hit");
        assert_eq!(tree, tree2);
        assert!(tree2.join("sentinel").exists());

        // Missing marker forces a re-extract that drops the sentinel.
        std::fs::remove_file(entry.join(TREE_OK_MARKER)).unwrap();
        let tree3 = ensure_extracted_tree(&archive).await.expect("re-extract");
        assert!(!tree3.join("sentinel").exists());
        assert!(tree3.join("hello").exists());
    }

    /// Regression (device nodes in the archive): older guest images shipped
    /// mknod'd entries under /dev, which a non-root `tar -xf` cannot recreate
    /// on macOS. Tests cannot mknod either, so a plain file under `dev/`
    /// stands in — asserting it is excluded proves the `--exclude` patterns
    /// are applied and match the `./dev/...`-style archive paths.
    #[tokio::test]
    async fn dev_entries_are_excluded_from_extraction() {
        let dir = tempfile::tempdir().unwrap();
        let entry = dir.path().join("digest");
        std::fs::create_dir_all(&entry).unwrap();

        // Fixture mirrors the build script's `tar -C tree -cf … .` layout,
        // which yields `./dev/null` entry names.
        let src = dir.path().join("src");
        std::fs::create_dir_all(src.join("dev")).unwrap();
        std::fs::create_dir_all(src.join("etc")).unwrap();
        std::fs::write(src.join("dev/null"), b"not a device").unwrap();
        std::fs::write(src.join("etc/hostname"), b"guest").unwrap();
        let archive = entry.join("rootfs.tar.xz");
        let status = std::process::Command::new("tar")
            .arg("-cJf")
            .arg(&archive)
            .arg("-C")
            .arg(&src)
            .arg(".")
            .status()
            .expect("tar available");
        assert!(status.success());

        let tree = ensure_extracted_tree(&archive).await.expect("extract");
        assert!(!tree.join("dev/null").exists());
        assert!(tree.join("etc/hostname").exists());
        // /dev survives as an empty directory: intent-init's devtmpfs
        // mountpoint.
        assert!(tree.join("dev").is_dir());
        assert_eq!(std::fs::read_dir(tree.join("dev")).unwrap().count(), 0);
    }

    /// Hand-built ustar archive with one regular-file member named `name`
    /// (tar's own create modes sanitize names, so the escaping ones are
    /// written byte-for-byte here).
    fn crafted_tar(name: &str) -> Vec<u8> {
        let data = b"escaped";
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100..108].copy_from_slice(b"0000644\0");
        header[108..116].copy_from_slice(b"0000000\0");
        header[116..124].copy_from_slice(b"0000000\0");
        header[124..136].copy_from_slice(format!("{:011o}\0", data.len()).as_bytes());
        header[136..148].copy_from_slice(b"00000000000\0");
        header[148..156].copy_from_slice(b"        ");
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let sum: u32 = header.iter().map(|b| u32::from(*b)).sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        let mut out = header.to_vec();
        out.extend_from_slice(data);
        out.resize(out.len().div_ceil(512) * 512, 0);
        out.extend_from_slice(&[0u8; 1024]);
        out
    }

    /// #873 review nit: archive members that would land outside the tree —
    /// absolute paths and `..` components — are refused explicitly, before
    /// anything is extracted, instead of relying on tar's default stripping.
    #[tokio::test]
    async fn escaping_archive_members_are_refused_before_extraction() {
        for (name, reason) in [
            ("../escape", "`..` component"),
            ("etc/../../escape", "`..` component"),
            ("/abs/escape", "absolute path"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let entry = dir.path().join("digest");
            std::fs::create_dir_all(&entry).unwrap();
            let archive = entry.join("rootfs.tar");
            std::fs::write(&archive, crafted_tar(name)).unwrap();

            let err = ensure_extracted_tree(&archive)
                .await
                .expect_err("escaping member must be refused");
            let msg = err.to_string();
            assert!(matches!(err, MicrovmError::Extract(_)), "{name}: {msg}");
            assert!(msg.contains(reason), "{name}: {msg}");
            assert!(msg.contains(name), "{name}: {msg}");
            assert!(
                !entry.join(TREE_DIR).exists() && !entry.join(TREE_OK_MARKER).exists(),
                "{name}: nothing extracted"
            );
            assert!(
                !dir.path().join("escape").exists(),
                "{name}: escaped the tree"
            );
        }

        // Control: the same crafted archive with a contained name extracts.
        let dir = tempfile::tempdir().unwrap();
        let entry = dir.path().join("digest");
        std::fs::create_dir_all(&entry).unwrap();
        let archive = entry.join("rootfs.tar");
        std::fs::write(&archive, crafted_tar("./etc/contained")).unwrap();
        let tree = ensure_extracted_tree(&archive).await.expect("contained");
        assert_eq!(
            std::fs::read(tree.join("etc/contained")).unwrap(),
            b"escaped"
        );
    }

    #[test]
    fn escaping_member_reason_flags_only_real_escapes() {
        assert_eq!(escaping_member_reason("./usr/bin/sh"), None);
        assert_eq!(escaping_member_reason("etc/..hidden"), None);
        assert_eq!(escaping_member_reason("a/../b"), Some("`..` component"));
        assert_eq!(escaping_member_reason(".."), Some("`..` component"));
        assert_eq!(escaping_member_reason("/etc/passwd"), Some("absolute path"));
    }
}
