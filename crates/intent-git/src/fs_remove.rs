//! Bounded-concurrency recursive directory removal.
//!
//! [`remove_dir_all_parallel`] is the shared removal helper behind every
//! background workspace-file delete: detached-trash removal
//! ([`crate::worktree::remove_detached_worktree`]), the `workspace.delete`
//! background sweep of workspace-dir candidates, and the startup orphaned
//! trash sweep in `intent-services`. A plain `std::fs::remove_dir_all` is
//! single-threaded, so reclaiming a `node_modules`-heavy multi-GB checkout
//! takes minutes of serial unlinking; fanning the walk out across a small
//! pool of threads keeps external volumes unsaturated while cutting the
//! wall-clock time substantially.
//!
//! Pure `std` by design — callers include synchronous code and closures
//! already running on tokio's blocking pool, so the helper must not require
//! a runtime context (and must not nest `spawn_blocking`, which would tie up
//! extra blocking-pool slots for the duration of the removal).

use std::io;
use std::path::Path;
use std::sync::Mutex;

/// Number of worker threads a single removal fans out across. Deliberately
/// small (task guidance: ~4–8): enough to overlap unlink syscalls, not
/// enough to saturate an external volume with parallel metadata writes.
const MAX_REMOVAL_THREADS: usize = 6;

/// Recursively remove `path` and everything beneath it, fanning the removal
/// of first-level subdirectories out across up to [`MAX_REMOVAL_THREADS`]
/// threads. A missing `path` is success (`NotFound` ⇒ `Ok`), matching
/// `remove_dir_all` call sites that treat "already gone" as done.
///
/// Two phases:
/// 1. **Parallel subtree removal** (best-effort): first-level entries whose
///    `read_dir` file type is a directory (`d_type` — symlinks are never
///    classified as directories) are pulled from a shared queue by a bounded
///    pool of workers, each running `std::fs::remove_dir_all` on its
///    subtree. Every actual unlink therefore happens inside std's
///    TOCTOU-hardened `remove_dir_all`, which never follows symlinks — even
///    a directory swapped for a symlink after classification makes the call
///    error out rather than descend through the link. Fan-out is
///    deliberately first-level only: queueing deeper paths would compose
///    multi-component paths whose intermediate components can be retargeted
///    after classification, and plain path resolution *would* follow those.
///    Per-subtree failures are ignored here.
/// 2. **Authoritative serial pass**: a final `std::fs::remove_dir_all(path)`
///    removes the root's files, the emptied root itself, and anything phase
///    1 could not remove, and is the sole source of the returned error, so
///    error semantics match the serial original.
///
/// # Errors
///
/// Returns the first I/O error from listing or removing entries (a missing `path` is a no-op success).
pub fn remove_dir_all_parallel(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Ok(meta) if meta.file_type().is_dir() => remove_subtrees_parallel(path),
        // Non-directory or unreadable metadata: let the serial pass decide.
        _ => {}
    }
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        #[cfg(unix)]
        Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
            // A read-only directory (a Go module cache, a `chmod -R a-w`
            // build tree) cannot have its entries unlinked: make every
            // directory owner-writable, then retry once.
            make_dirs_writable(path);
            match std::fs::remove_dir_all(path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(_) => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

/// Best-effort: add owner `rwx` to `root` and every directory beneath it.
///
/// The walk is anchored on directory descriptors: every directory below
/// `root` is opened with `openat(parent_fd, name, O_DIRECTORY | O_NOFOLLOW)`
/// relative to its already-open parent, listed through its own descriptor,
/// and changed with `fchmod`. No accumulated path is ever resolved again, so
/// a directory (or an ancestor) swapped for a symlink mid-walk is never
/// followed and nothing outside `root` changes mode. A directory that cannot
/// be opened for reading (no `r` bit) is skipped; the caller's removal then
/// reports the failure. Open descriptors stay bounded by the tree depth: a
/// pending entry holds only its parent.
#[cfg(unix)]
fn make_dirs_writable(root: &Path) {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::rc::Rc;

    const FLAGS: libc::c_int =
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    let Ok(path) = CString::new(root.as_os_str().as_bytes()) else {
        return;
    };
    // SAFETY: `path` is a valid NUL-terminated string; `open` returns a new
    // descriptor or -1.
    let fd = unsafe { libc::open(path.as_ptr(), FLAGS) };
    if fd < 0 {
        return;
    }
    // SAFETY: `fd` was just returned by `open` and is owned by nothing else.
    let root_fd = Rc::new(unsafe { OwnedFd::from_raw_fd(fd) });
    let mut pending: Vec<(Rc<OwnedFd>, CString)> = Vec::new();
    let visit = |dir: &Rc<OwnedFd>, pending: &mut Vec<(Rc<OwnedFd>, CString)>| {
        add_owner_rwx(dir.as_raw_fd());
        for name in entry_names(dir.as_raw_fd()) {
            pending.push((Rc::clone(dir), name));
        }
    };
    visit(&root_fd, &mut pending);
    drop(root_fd);
    while let Some((parent, name)) = pending.pop() {
        // SAFETY: `parent` is an open directory descriptor and `name` a
        // NUL-terminated single path component; `openat` returns a new
        // descriptor or -1 (ENOTDIR for files, ELOOP for symlinks).
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), FLAGS) };
        drop(parent);
        if fd < 0 {
            continue;
        }
        // SAFETY: `fd` was just returned by `openat` and is owned by nothing
        // else.
        let dir = Rc::new(unsafe { OwnedFd::from_raw_fd(fd) });
        visit(&dir, &mut pending);
    }
}

/// `fchmod` the directory open at `fd` to add owner `rwx`, if missing.
#[cfg(unix)]
fn add_owner_rwx(fd: std::os::fd::RawFd) {
    let rwx: libc::mode_t = 0o700;
    // SAFETY: `libc::stat` is plain old data; all-zero is a valid value.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is an open descriptor and `stat` a valid out-pointer.
    let status = unsafe { libc::fstat(fd, &raw mut stat) };
    if status != 0 || stat.st_mode & rwx == rwx {
        return;
    }
    // SAFETY: `fd` is an open descriptor; a failure leaves the mode as is.
    let _ = unsafe { libc::fchmod(fd, (stat.st_mode | rwx) & 0o7777) };
}

/// The entry names of the directory open at `fd` (without `.` and `..`),
/// read through a duplicate descriptor so `fd` stays open.
#[cfg(unix)]
fn entry_names(fd: std::os::fd::RawFd) -> Vec<std::ffi::CString> {
    // SAFETY: `fd` is an open descriptor; `dup` returns a new one or -1.
    let dup = unsafe { libc::dup(fd) };
    if dup < 0 {
        return Vec::new();
    }
    // SAFETY: `dup` is an open directory descriptor we own; on success
    // `fdopendir` takes ownership of it (released by `closedir`).
    let stream = unsafe { libc::fdopendir(dup) };
    if stream.is_null() {
        // SAFETY: `fdopendir` failed, so `dup` is still ours to close.
        unsafe { libc::close(dup) };
        return Vec::new();
    }
    let mut names = Vec::new();
    loop {
        // SAFETY: `stream` is a valid open `DIR*`.
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            break;
        }
        // SAFETY: `readdir` returned a valid entry whose `d_name` is
        // NUL-terminated; it is copied before the next `readdir` call.
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            names.push(name.to_owned());
        }
    }
    // SAFETY: `stream` is a valid open `DIR*`, closed exactly once.
    unsafe { libc::closedir(stream) };
    names
}

/// Phase 1 of [`remove_dir_all_parallel`]: best-effort parallel removal of
/// `root`'s first-level subdirectories. The root's non-directory entries and
/// the root itself are left for the authoritative serial pass.
fn remove_subtrees_parallel(root: &Path) {
    let mut subdirs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                subdirs.push(entry.path());
            }
        }
    }
    if subdirs.len() < 2 {
        // Nothing to overlap — the serial pass handles it alone.
        return;
    }
    let workers = MAX_REMOVAL_THREADS.min(subdirs.len());
    let queue = Mutex::new(subdirs);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            // Builder::spawn_scoped (unlike Scope::spawn) reports OS
            // thread-spawn failure as an Err instead of panicking; under
            // resource exhaustion we degrade gracefully — whatever workers
            // did spawn (possibly none) drain the queue, and the serial
            // pass removes the rest.
            let spawned = std::thread::Builder::new().spawn_scoped(scope, || loop {
                let Some(dir) = queue.lock().expect("removal queue lock poisoned").pop() else {
                    return;
                };
                // Failures (permissions, races) are re-encountered and
                // reported by the authoritative serial pass.
                let _ = std::fs::remove_dir_all(&dir);
            });
            if spawned.is_err() {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::remove_dir_all_parallel;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "intent-git-fs-remove-{tag}-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn removes_deep_and_wide_tree_completely() {
        let root = temp_dir("tree");
        // Wide: more first-level dirs than worker threads. Deep: nested
        // chains so workers keep feeding the queue mid-walk.
        for i in 0..16 {
            let mut dir = root.join(format!("wide-{i}"));
            for depth in 0..8 {
                dir = dir.join(format!("d{depth}"));
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("f.txt"), "x").unwrap();
            }
        }
        std::fs::write(root.join("top.txt"), "x").unwrap();
        std::fs::create_dir(root.join("empty")).unwrap();

        remove_dir_all_parallel(&root).unwrap();
        assert!(!root.exists(), "entire tree removed, root included");
    }

    #[test]
    fn missing_path_is_ok() {
        let missing = std::env::temp_dir().join(format!(
            "intent-git-fs-remove-missing-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        remove_dir_all_parallel(&missing).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unlinks_symlinks_without_following_them() {
        let target = temp_dir("symlink-target");
        std::fs::write(target.join("keep.txt"), "keep").unwrap();
        let root = temp_dir("symlink-root");
        std::os::unix::fs::symlink(&target, root.join("link-to-dir")).unwrap();
        std::os::unix::fs::symlink(target.join("keep.txt"), root.join("nested-file-link")).unwrap();

        remove_dir_all_parallel(&root).unwrap();
        assert!(!root.exists());
        assert!(
            target.join("keep.txt").exists(),
            "symlink target untouched — links unlinked, never followed"
        );
        std::fs::remove_dir_all(&target).unwrap();
    }

    /// Read-only directories (a Go module cache, a `chmod -R a-w` build
    /// tree) are made writable and removed; a symlinked directory's target
    /// keeps its mode.
    #[cfg(unix)]
    #[test]
    fn removes_read_only_directories() {
        use std::os::unix::fs::PermissionsExt;
        let target = temp_dir("read-only-target");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o555)).unwrap();
        let root = temp_dir("read-only");
        for top in ["a", "b"] {
            let deep = root.join(top).join("pkg").join("mod");
            std::fs::create_dir_all(&deep).unwrap();
            std::fs::write(deep.join("f.go"), "x").unwrap();
            for dir in [deep.clone(), deep.parent().unwrap().to_path_buf()] {
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
            }
        }
        std::fs::write(root.join("top.txt"), "x").unwrap();
        std::os::unix::fs::symlink(&target, root.join("link")).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555)).unwrap();

        remove_dir_all_parallel(&root).unwrap();
        assert!(!root.exists(), "read-only tree removed");
        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o555, "symlink target untouched");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(&target).unwrap();
    }

    #[test]
    fn plain_file_at_path_is_an_error() {
        let root = temp_dir("file-at-path");
        let file = root.join("not-a-dir.txt");
        std::fs::write(&file, "x").unwrap();
        remove_dir_all_parallel(&file).unwrap_err();
        assert!(file.exists(), "non-directory path is refused, not deleted");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
