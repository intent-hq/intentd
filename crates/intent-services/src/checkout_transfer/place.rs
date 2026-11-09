//! Writes under a caller's checkout that never follow a link. On unix each
//! folder is opened from the checkout's root one component at a time with
//! `O_NOFOLLOW | O_DIRECTORY`, folders are made with `mkdirat` and entries
//! moved with `renameat` relative to those handles, so a folder swapped for
//! a link while the pieces arrive fails instead of being written through.
//! Elsewhere each folder is checked again by path right before the write.

use std::path::{Path, PathBuf};

use intent_core::{Error, Result};

/// What is at a name in a [`Folder`], without following a link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Entry {
    Missing,
    Folder,
    Link,
    Other,
}

/// One folder of the checkout, opened without following links.
pub(super) struct Folder {
    #[cfg(unix)]
    fd: std::os::fd::OwnedFd,
    path: PathBuf,
}

fn io(context: &str, error: &std::io::Error) -> Error {
    super::io(context, error)
}

impl Folder {
    /// The checkout's root (its canonical path), opened without following
    /// a link there; kept for the whole pull, so the checkout swapped for a
    /// link later is never followed.
    #[cfg_attr(
        not(unix),
        expect(clippy::unnecessary_wraps, reason = "fails only on unix")
    )]
    pub(super) fn root(root: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use nix::fcntl::OFlag;
            let fd = nix::fcntl::open(
                root,
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                nix::sys::stat::Mode::empty(),
            )
            .map_err(|e| io("open the checkout", &e.into()))?;
            Ok(Self {
                fd,
                path: root.to_path_buf(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                path: root.to_path_buf(),
            })
        }
    }

    /// What is at `name` here.
    #[cfg_attr(
        not(unix),
        expect(clippy::unnecessary_wraps, reason = "fails only on unix")
    )]
    pub(super) fn entry(&self, name: &str) -> Result<Entry> {
        #[cfg(unix)]
        {
            use nix::sys::stat::{fstatat, SFlag};
            match fstatat(&self.fd, name, nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW) {
                Ok(stat) => {
                    let kind = SFlag::from_bits_truncate(stat.st_mode) & SFlag::S_IFMT;
                    Ok(if kind == SFlag::S_IFDIR {
                        Entry::Folder
                    } else if kind == SFlag::S_IFLNK {
                        Entry::Link
                    } else {
                        Entry::Other
                    })
                }
                Err(nix::errno::Errno::ENOENT) => Ok(Entry::Missing),
                Err(e) => Err(io("inspect the destination", &e.into())),
            }
        }
        #[cfg(not(unix))]
        {
            Ok(match std::fs::symlink_metadata(self.path.join(name)) {
                Err(_) => Entry::Missing,
                Ok(meta) if meta.file_type().is_symlink() => Entry::Link,
                Ok(meta) if meta.is_dir() => Entry::Folder,
                Ok(_) => Entry::Other,
            })
        }
    }

    /// The folder `name` here, made when missing and `create` is set (its path
    /// is pushed to `made`). A link there is outside the checkout; a file
    /// there is in the way.
    pub(super) fn child(
        &self,
        name: &str,
        create: bool,
        made: &mut Vec<PathBuf>,
        refuse: &dyn Fn(Entry) -> Error,
    ) -> Result<Self> {
        let path = self.path.join(name);
        match self.entry(name)? {
            Entry::Folder => {}
            Entry::Missing if create => {
                #[cfg(unix)]
                nix::sys::stat::mkdirat(
                    &self.fd,
                    name,
                    nix::sys::stat::Mode::from_bits_truncate(0o755),
                )
                .map_err(|e| io("make the destination folder", &e.into()))?;
                #[cfg(not(unix))]
                std::fs::create_dir(&path).map_err(|e| io("make the destination folder", &e))?;
                made.push(path.clone());
            }
            other => return Err(refuse(other)),
        }
        #[cfg(unix)]
        {
            use nix::fcntl::OFlag;
            match nix::fcntl::openat(
                &self.fd,
                name,
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                nix::sys::stat::Mode::empty(),
            ) {
                Ok(fd) => Ok(Self { fd, path }),
                // Swapped for a link (or a file) since it was inspected.
                Err(nix::errno::Errno::ELOOP | nix::errno::Errno::ENOTDIR) => {
                    Err(refuse(Entry::Link))
                }
                Err(e) => Err(io("open the destination folder", &e.into())),
            }
        }
        #[cfg(not(unix))]
        {
            Ok(Self { path })
        }
    }

    /// The folder at `parts` below this one, each opened as [`Self::child`].
    pub(super) fn walk(
        &self,
        parts: &[String],
        create: bool,
        made: &mut Vec<PathBuf>,
        refuse: &dyn Fn(Entry) -> Error,
    ) -> Result<Self> {
        let mut folder = self.reopen()?;
        for part in parts {
            folder = folder.child(part, create, made, refuse)?;
        }
        Ok(folder)
    }

    #[cfg_attr(
        not(unix),
        expect(clippy::unnecessary_wraps, reason = "fails only on unix")
    )]
    fn reopen(&self) -> Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            fd: self
                .fd
                .try_clone()
                .map_err(|e| io("open the destination folder", &e))?,
            path: self.path.clone(),
        })
    }

    /// A new file `name` here, open to write and read back, never through
    /// a link.
    pub(super) fn create_file(&self, name: &str) -> Result<std::fs::File> {
        #[cfg(unix)]
        {
            use nix::fcntl::OFlag;
            let fd = nix::fcntl::openat(
                &self.fd,
                name,
                OFlag::O_RDWR
                    | OFlag::O_CREAT
                    | OFlag::O_EXCL
                    | OFlag::O_NOFOLLOW
                    | OFlag::O_CLOEXEC,
                nix::sys::stat::Mode::from_bits_truncate(0o644),
            )
            .map_err(|e| io("write the copy", &e.into()))?;
            Ok(std::fs::File::from(fd))
        }
        #[cfg(not(unix))]
        {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(self.path.join(name))
                .map_err(|e| io("write the copy", &e))
        }
    }

    /// Move `name` here to `to_name` in `to`.
    pub(super) fn rename(&self, name: &str, to: &Self, to_name: &str) -> Result<()> {
        #[cfg(unix)]
        {
            nix::fcntl::renameat(&self.fd, name, &to.fd, to_name)
                .map_err(|e| io("place the copy", &e.into()))
        }
        #[cfg(not(unix))]
        {
            std::fs::rename(self.path.join(name), to.path.join(to_name))
                .map_err(|e| io("place the copy", &e))
        }
    }

    /// The names here, each with whether it is a folder (a link is not).
    pub(super) fn list(&self) -> Result<Vec<(String, bool)>> {
        #[cfg(unix)]
        {
            use nix::dir::{Dir, Type};
            let fd = self.fd.try_clone().map_err(|e| io("list the folder", &e))?;
            let dir = Dir::from_fd(fd).map_err(|e| io("list the folder", &e.into()))?;
            let mut names = Vec::new();
            for entry in dir {
                let entry = entry.map_err(|e| io("list the folder", &e.into()))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name == "." || name == ".." {
                    continue;
                }
                let is_dir = match entry.file_type() {
                    Some(Type::Directory) => true,
                    Some(_) => false,
                    None => self.entry(&name)? == Entry::Folder,
                };
                names.push((name, is_dir));
            }
            names.sort();
            Ok(names)
        }
        #[cfg(not(unix))]
        {
            let mut names = Vec::new();
            for entry in std::fs::read_dir(&self.path).map_err(|e| io("list the folder", &e))? {
                let entry = entry.map_err(|e| io("list the folder", &e))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                let is_dir = self.entry(&name)? == Entry::Folder;
                names.push((name, is_dir));
            }
            names.sort();
            Ok(names)
        }
    }

    /// Remove the file `name` here (never what a link points to).
    pub(super) fn remove_file(&self, name: &str) -> Result<()> {
        #[cfg(unix)]
        {
            nix::unistd::unlinkat(&self.fd, name, nix::unistd::UnlinkatFlags::NoRemoveDir)
                .map_err(|e| io("remove the copy", &e.into()))
        }
        #[cfg(not(unix))]
        {
            std::fs::remove_file(self.path.join(name)).map_err(|e| io("remove the copy", &e))
        }
    }

    /// Remove the folder `name` here and everything in it, through handles,
    /// removing links as links.
    pub(super) fn remove_tree(&self, name: &str) -> Result<()> {
        if self.entry(name)? == Entry::Folder {
            let inner = self.child(name, false, &mut Vec::new(), &|_| {
                io(
                    "remove the copy",
                    &std::io::Error::other("changed while removed"),
                )
            })?;
            for (entry, is_dir) in inner.list()? {
                if is_dir {
                    inner.remove_tree(&entry)?;
                } else {
                    inner.remove_file(&entry)?;
                }
            }
            #[cfg(unix)]
            {
                nix::unistd::unlinkat(&self.fd, name, nix::unistd::UnlinkatFlags::RemoveDir)
                    .map_err(|e| io("remove the copy", &e.into()))?;
            }
            #[cfg(not(unix))]
            {
                std::fs::remove_dir(self.path.join(name)).map_err(|e| io("remove the copy", &e))?;
            }
            Ok(())
        } else {
            self.remove_file(name)
        }
    }
}
