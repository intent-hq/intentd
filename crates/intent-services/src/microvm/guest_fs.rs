//! Rootfs-contained host writes into a guest tree (#873 review).
//!
//! A running guest owns every path inside its per-VM rootfs, so a plain
//! `std::fs::write(rootfs.join(rel))` from the host follows whatever the
//! guest planted there: swap `/root/.codex/auth.json` (or its parent, or the
//! staging temp name) for a symlink to an absolute host path and the next
//! credential rotation truncates a daemon-writable host file. Every host
//! write into the guest tree therefore goes through [`RootfsWriter`]: the
//! rootfs root is opened once as a directory fd, each relative component is
//! walked with `openat(O_DIRECTORY | O_NOFOLLOW)` (missing directories are
//! created with `mkdirat` and re-opened the same way), the payload lands in a
//! temp file created with `openat(O_CREAT | O_EXCL | O_NOFOLLOW)` in the final
//! directory fd, and `renameat` moves it into place within that same fd. A
//! component that turns out to be a symlink (or anything but a real directory
//! / regular file) is refused, never followed; absolute or `..` paths are
//! refused up front. Reads of the destination (the "already identical" check)
//! are no-follow too — a planted symlink must not be read either.
//!
//! Only the rootfs root itself is opened by path (following symlinks): it is
//! the daemon-owned `<vm_dir>/rootfs` clone target, outside anything the
//! guest can rename.

use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

/// Mode of directories the walk creates on the way to a destination.
const DIR_MODE: libc::mode_t = 0o755;

#[derive(Debug, thiserror::Error)]
pub enum GuestFsError {
    /// `guest_rel` is absolute, empty, or has a `..` / `.` / NUL component.
    #[error("guest path {0:?} is not a plain relative path inside the rootfs")]
    Escape(String),
    /// A component exists but is not what the walk requires — a symlink
    /// where a directory or regular file is expected, typically.
    #[error("guest path {path:?} refused: {what}")]
    NotContained { path: String, what: &'static str },
    #[error("{op} {path:?}: {source}")]
    Io {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Name of the temp file a [`RootfsWriter::write_file`] of `name` stages
/// through, in the destination's directory. Deterministic (per daemon pid) so
/// a crashed prior write leaves at most one stale regular file, which the
/// next write reclaims; anything else at that name is refused.
#[must_use]
pub fn temp_name(name: &str) -> String {
    format!(".{name}.{}.intentd-staging", std::process::id())
}

/// A directory fd on a per-VM rootfs root; every operation resolves
/// `guest_rel` component by component below it without following symlinks.
pub struct RootfsWriter {
    root: OwnedFd,
}

impl RootfsWriter {
    /// Open `rootfs` (the daemon-owned clone root) as a directory fd.
    ///
    /// # Errors
    ///
    /// Returns [`GuestFsError::Io`] when the root cannot be opened as a
    /// directory.
    pub fn open(rootfs: &Path) -> Result<Self, GuestFsError> {
        let display = rootfs.display().to_string();
        let c = cstr(rootfs.as_os_str()).ok_or_else(|| GuestFsError::Escape(display.clone()))?;
        let fd = unsafe {
            libc::open(
                c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(GuestFsError::Io {
                op: "open rootfs",
                path: display,
                source: std::io::Error::last_os_error(),
            });
        }
        Ok(Self {
            root: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }

    /// Ensure the directory `guest_rel` exists (every component a real
    /// directory), creating missing ones.
    ///
    /// # Errors
    ///
    /// [`GuestFsError::Escape`] for a non-contained path,
    /// [`GuestFsError::NotContained`] when a component is a symlink or a
    /// non-directory, [`GuestFsError::Io`] otherwise.
    pub fn ensure_dir(&self, guest_rel: &str) -> Result<(), GuestFsError> {
        let (mut dirs, name) = split(guest_rel)?;
        dirs.push(name);
        self.walk_dirs(&dirs, guest_rel)?;
        Ok(())
    }

    /// Atomically write `bytes` to the regular file `guest_rel` with `mode`
    /// (temp file + `renameat` inside the destination directory fd), creating
    /// parent directories as needed. An existing destination must be a
    /// regular file; the temp name must be absent or a stale regular file.
    ///
    /// # Errors
    ///
    /// [`GuestFsError::Escape`] for a non-contained path,
    /// [`GuestFsError::NotContained`] when any component, the destination, or
    /// the temp name is a symlink / wrong file type, [`GuestFsError::Io`]
    /// when a filesystem step fails.
    pub fn write_file(&self, guest_rel: &str, bytes: &[u8], mode: u32) -> Result<(), GuestFsError> {
        let (dirs, name) = split(guest_rel)?;
        let dir = self.walk_dirs(&dirs, guest_rel)?;
        let name_str = name.to_string_lossy();
        match stat_nofollow(&dir, name) {
            Ok(st) if !is_reg(&st) => {
                return Err(not_contained(
                    guest_rel,
                    "destination is not a regular file",
                ));
            }
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
            Err(e) => return Err(io_err("stat destination", guest_rel, e)),
        }
        let tmp = temp_name(&name_str);
        let tmp_os = OsStr::new(&tmp);
        // Reclaim only a stale regular temp file from a crashed prior write;
        // a symlink (or anything else) planted at the temp name is refused.
        match stat_nofollow(&dir, tmp_os) {
            Ok(st) if is_reg(&st) => {
                unlinkat(&dir, tmp_os).map_err(|e| io_err("unlink stale temp", guest_rel, e))?;
            }
            Ok(_) => return Err(not_contained(guest_rel, "temp name is not a regular file")),
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
            Err(e) => return Err(io_err("stat temp", guest_rel, e)),
        }
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW;
        let tmp_fd = openat(&dir, tmp_os, flags, mode).map_err(|e| match e.raw_os_error() {
            Some(libc::EEXIST | libc::ELOOP) => {
                not_contained(guest_rel, "temp name was replaced during staging")
            }
            _ => io_err("create temp", guest_rel, e),
        })?;
        let mut file = File::from(tmp_fd);
        let written = file
            .write_all(bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| renameat(&dir, tmp_os, name));
        if let Err(e) = written {
            let _ = unlinkat(&dir, tmp_os);
            return Err(io_err("write", guest_rel, e));
        }
        let _ = unsafe { libc::fsync(dir.as_raw_fd()) };
        Ok(())
    }

    /// Read the regular file `guest_rel` without following symlinks anywhere
    /// on the path. `Ok(None)` when the file or a parent is absent.
    ///
    /// # Errors
    ///
    /// [`GuestFsError::Escape`] for a non-contained path,
    /// [`GuestFsError::NotContained`] when a component or the destination is
    /// a symlink / not a regular file, [`GuestFsError::Io`] otherwise.
    pub fn read_file(&self, guest_rel: &str) -> Result<Option<Vec<u8>>, GuestFsError> {
        let (dirs, name) = split(guest_rel)?;
        let mut dir = self
            .root
            .try_clone()
            .map_err(|e| io_err("dup rootfs fd", guest_rel, e))?;
        for comp in &dirs {
            dir = match openat(
                &dir,
                comp,
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW,
                0,
            ) {
                Ok(fd) => fd,
                Err(e) if e.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
                Err(e) => return Err(classify_dir_open(guest_rel, e)),
            };
        }
        // O_NONBLOCK: a planted FIFO must not park the caller on open; the
        // fstat below then rejects it (regular-file reads ignore the flag).
        let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK;
        let fd = match openat(&dir, name, flags, 0) {
            Ok(fd) => fd,
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
            Err(e) if e.raw_os_error() == Some(libc::ELOOP) => {
                return Err(not_contained(guest_rel, "destination is a symlink"));
            }
            Err(e) => return Err(io_err("open", guest_rel, e)),
        };
        let mut file = File::from(fd);
        let meta = file.metadata().map_err(|e| io_err("fstat", guest_rel, e))?;
        if !meta.file_type().is_file() {
            return Err(not_contained(
                guest_rel,
                "destination is not a regular file",
            ));
        }
        let mut out = Vec::new();
        file.read_to_end(&mut out)
            .map_err(|e| io_err("read", guest_rel, e))?;
        Ok(Some(out))
    }

    /// Walk `dirs` below the root, creating missing directories, and return
    /// the final directory fd. Each hop is `O_DIRECTORY | O_NOFOLLOW`; a
    /// `mkdirat` is always followed by the same no-follow re-open, so a
    /// symlink racing into the freshly named slot is still refused.
    fn walk_dirs(&self, dirs: &[&OsStr], guest_rel: &str) -> Result<OwnedFd, GuestFsError> {
        let mut dir = self
            .root
            .try_clone()
            .map_err(|e| io_err("dup rootfs fd", guest_rel, e))?;
        for comp in dirs {
            let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW;
            dir = match openat(&dir, comp, flags, 0) {
                Ok(fd) => fd,
                Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {
                    match mkdirat(&dir, comp) {
                        Ok(()) => {}
                        Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {}
                        Err(e) => return Err(io_err("mkdir", guest_rel, e)),
                    }
                    openat(&dir, comp, flags, 0).map_err(|e| classify_dir_open(guest_rel, e))?
                }
                Err(e) => return Err(classify_dir_open(guest_rel, e)),
            };
        }
        Ok(dir)
    }
}

/// Split `guest_rel` into its parent components and final name, refusing
/// anything that is not a plain relative path made of normal components.
fn split(guest_rel: &str) -> Result<(Vec<&OsStr>, &OsStr), GuestFsError> {
    let escape = || GuestFsError::Escape(guest_rel.to_string());
    if guest_rel.is_empty() || guest_rel.contains('\0') {
        return Err(escape());
    }
    let mut comps = Path::new(guest_rel)
        .components()
        .map(|c| match c {
            Component::Normal(name) => Ok(name),
            _ => Err(escape()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let name = comps.pop().ok_or_else(escape)?;
    Ok((comps, name))
}

fn cstr(name: &OsStr) -> Option<CString> {
    CString::new(name.as_bytes()).ok()
}

fn name_cstr(name: &OsStr) -> std::io::Result<CString> {
    cstr(name).ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))
}

fn openat(dir: &OwnedFd, name: &OsStr, flags: libc::c_int, mode: u32) -> std::io::Result<OwnedFd> {
    let c = name_cstr(name)?;
    let fd = unsafe { libc::openat(dir.as_raw_fd(), c.as_ptr(), flags | libc::O_CLOEXEC, mode) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn mkdirat(dir: &OwnedFd, name: &OsStr) -> std::io::Result<()> {
    let c = name_cstr(name)?;
    check(unsafe { libc::mkdirat(dir.as_raw_fd(), c.as_ptr(), DIR_MODE) })
}

fn renameat(dir: &OwnedFd, from: &OsStr, to: &OsStr) -> std::io::Result<()> {
    let (f, t) = (name_cstr(from)?, name_cstr(to)?);
    check(unsafe { libc::renameat(dir.as_raw_fd(), f.as_ptr(), dir.as_raw_fd(), t.as_ptr()) })
}

fn unlinkat(dir: &OwnedFd, name: &OsStr) -> std::io::Result<()> {
    let c = name_cstr(name)?;
    check(unsafe { libc::unlinkat(dir.as_raw_fd(), c.as_ptr(), 0) })
}

fn stat_nofollow(dir: &OwnedFd, name: &OsStr) -> std::io::Result<libc::stat> {
    let c = name_cstr(name)?;
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    check(unsafe {
        libc::fstatat(
            dir.as_raw_fd(),
            c.as_ptr(),
            &raw mut st,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    Ok(st)
}

fn is_reg(st: &libc::stat) -> bool {
    (st.st_mode & libc::S_IFMT) == libc::S_IFREG
}

fn check(rc: libc::c_int) -> std::io::Result<()> {
    if rc < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn classify_dir_open(guest_rel: &str, e: std::io::Error) -> GuestFsError {
    match e.raw_os_error() {
        Some(libc::ELOOP) => not_contained(guest_rel, "a path component is a symlink"),
        Some(libc::ENOTDIR) => not_contained(guest_rel, "a path component is not a directory"),
        _ => io_err("open dir", guest_rel, e),
    }
}

fn not_contained(guest_rel: &str, what: &'static str) -> GuestFsError {
    GuestFsError::NotContained {
        path: guest_rel.to_string(),
        what,
    }
}

fn io_err(op: &'static str, guest_rel: &str, source: std::io::Error) -> GuestFsError {
    GuestFsError::Io {
        op,
        path: guest_rel.to_string(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::time::SystemTime;

    const HOST_BYTES: &[u8] = b"host-secret-must-survive";

    /// A rootfs under `tmp` plus a "host target" file outside it whose bytes
    /// and mtime every refusal test asserts untouched.
    struct Fixture {
        _tmp: tempfile::TempDir,
        rootfs: std::path::PathBuf,
        host_target: std::path::PathBuf,
        host_mtime: SystemTime,
    }

    fn fixture() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let rootfs = tmp.path().join("rootfs");
        std::fs::create_dir_all(rootfs.join("root")).unwrap();
        let host_target = tmp.path().join("host-target");
        std::fs::write(&host_target, HOST_BYTES).unwrap();
        let host_mtime = std::fs::metadata(&host_target).unwrap().modified().unwrap();
        Fixture {
            _tmp: tmp,
            rootfs,
            host_target,
            host_mtime,
        }
    }

    impl Fixture {
        fn writer(&self) -> RootfsWriter {
            RootfsWriter::open(&self.rootfs).unwrap()
        }

        fn assert_host_untouched(&self) {
            assert_eq!(std::fs::read(&self.host_target).unwrap(), HOST_BYTES);
            let meta = std::fs::metadata(&self.host_target).unwrap();
            assert_eq!(meta.modified().unwrap(), self.host_mtime);
        }
    }

    #[test]
    fn happy_path_creates_dirs_0600_and_replaces_atomically() {
        let fx = fixture();
        let w = fx.writer();
        w.write_file("root/.codex/auth.json", b"{\"t\":1}", 0o600)
            .unwrap();
        let dst = fx.rootfs.join("root/.codex/auth.json");
        assert_eq!(std::fs::read(&dst).unwrap(), b"{\"t\":1}");
        assert_eq!(
            std::fs::metadata(&dst).unwrap().permissions().mode() & 0o777,
            0o600
        );
        w.write_file("root/.codex/auth.json", b"{\"t\":2}", 0o600)
            .unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), b"{\"t\":2}");
        assert_eq!(
            w.read_file("root/.codex/auth.json").unwrap().unwrap(),
            b"{\"t\":2}"
        );
        // No temp file left behind.
        let leftovers: Vec<_> = std::fs::read_dir(fx.rootfs.join("root/.codex"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers, vec![std::ffi::OsString::from("auth.json")]);
        assert!(w.read_file("root/.codex/absent").unwrap().is_none());
        assert!(w.read_file("root/nope/absent").unwrap().is_none());
    }

    #[test]
    fn symlinked_destination_is_refused_for_write_and_read() {
        let fx = fixture();
        std::fs::create_dir_all(fx.rootfs.join("root/.codex")).unwrap();
        symlink(&fx.host_target, fx.rootfs.join("root/.codex/auth.json")).unwrap();
        let w = fx.writer();
        let err = w
            .write_file("root/.codex/auth.json", b"pwned", 0o600)
            .unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        let err = w.read_file("root/.codex/auth.json").unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        fx.assert_host_untouched();
        assert!(fx.rootfs.join("root/.codex/auth.json").is_symlink());
    }

    #[test]
    fn symlinked_parent_dir_is_refused() {
        let fx = fixture();
        let host_dir = fx.host_target.parent().unwrap().join("host-dir");
        std::fs::create_dir_all(&host_dir).unwrap();
        symlink(&host_dir, fx.rootfs.join("root/.codex")).unwrap();
        let w = fx.writer();
        let err = w
            .write_file("root/.codex/auth.json", b"pwned", 0o600)
            .unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        assert!(std::fs::read_dir(&host_dir).unwrap().next().is_none());
        let err = w.ensure_dir("root/.codex/sub").unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        assert!(std::fs::read_dir(&host_dir).unwrap().next().is_none());
        // A symlinked `/root` itself is refused the same way.
        let fx2 = fixture();
        std::fs::remove_dir(fx2.rootfs.join("root")).unwrap();
        symlink(fx2.host_target.parent().unwrap(), fx2.rootfs.join("root")).unwrap();
        let err = fx2
            .writer()
            .write_file("root/host-target", b"pwned", 0o600)
            .unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        fx2.assert_host_untouched();
    }

    #[test]
    fn symlinked_temp_name_is_refused_but_stale_regular_temp_is_reclaimed() {
        let fx = fixture();
        std::fs::create_dir_all(fx.rootfs.join("root/.codex")).unwrap();
        let tmp = fx.rootfs.join("root/.codex").join(temp_name("auth.json"));
        symlink(&fx.host_target, &tmp).unwrap();
        let w = fx.writer();
        let err = w
            .write_file("root/.codex/auth.json", b"pwned", 0o600)
            .unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        fx.assert_host_untouched();
        assert!(tmp.is_symlink());
        assert!(!fx.rootfs.join("root/.codex/auth.json").exists());

        std::fs::remove_file(&tmp).unwrap();
        std::fs::write(&tmp, b"stale").unwrap();
        w.write_file("root/.codex/auth.json", b"fresh", 0o600)
            .unwrap();
        assert_eq!(
            std::fs::read(fx.rootfs.join("root/.codex/auth.json")).unwrap(),
            b"fresh"
        );
        assert!(!tmp.exists());
    }

    #[test]
    fn absolute_and_dotdot_paths_are_refused() {
        let fx = fixture();
        let w = fx.writer();
        let abs = fx.host_target.to_string_lossy().to_string();
        for rel in [
            abs.as_str(),
            "/etc/passwd",
            "../host-target",
            "root/../../host-target",
            "",
            ".",
            "root/x\0",
        ] {
            let err = w.write_file(rel, b"pwned", 0o600).unwrap_err();
            assert!(matches!(err, GuestFsError::Escape(_)), "{rel:?}: {err}");
            let err = w.read_file(rel).unwrap_err();
            assert!(matches!(err, GuestFsError::Escape(_)), "{rel:?}: {err}");
        }
        fx.assert_host_untouched();
    }

    #[test]
    fn non_regular_destination_is_refused() {
        let fx = fixture();
        std::fs::create_dir_all(fx.rootfs.join("root/.codex/auth.json")).unwrap();
        let err = fx
            .writer()
            .write_file("root/.codex/auth.json", b"x", 0o600)
            .unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
        let err = fx.writer().read_file("root/.codex/auth.json").unwrap_err();
        assert!(matches!(err, GuestFsError::NotContained { .. }), "{err}");
    }
}
