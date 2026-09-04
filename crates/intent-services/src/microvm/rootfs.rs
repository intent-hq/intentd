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
}
