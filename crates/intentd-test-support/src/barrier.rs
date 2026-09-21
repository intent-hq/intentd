use std::fs;
use std::path::{Path, PathBuf};

/// A release-file barrier between a test and its fake daemon: the daemon's
/// shell script blocks at [`Barrier::sh_wait`] until the test calls
/// [`Barrier::release`], so a held state stays stable for as long as the
/// test's assertions take instead of racing a fixed `sleep`. A script can
/// also [`Barrier::sh_arrive`] just before waiting so the test can prove,
/// via [`Barrier::entered`], that the daemon actually reached the hold.
///
/// A suite that hands the release path to its fake through an environment
/// variable instead of an inlined snippet reads it from [`Barrier::path`].
pub struct Barrier {
    path: PathBuf,
}

impl Barrier {
    /// A barrier whose release file is `<data_dir>/barrier-<name>`.
    #[must_use]
    pub fn new(data_dir: &Path, name: &str) -> Self {
        Self {
            path: data_dir.join(format!("barrier-{name}")),
        }
    }

    /// The release file a script waits on (see [`Barrier::sh_wait`]).
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Let every script blocked in [`Barrier::sh_wait`] proceed.
    ///
    /// # Panics
    ///
    /// Panics when the release file cannot be created (the test's data
    /// directory is gone).
    pub fn release(&self) {
        fs::write(&self.path, b"").unwrap();
    }

    /// Shell snippet that blocks until the barrier is released.
    #[must_use]
    pub fn sh_wait(&self) -> String {
        let path = self.path.display();
        // timing-guard: poll interval
        format!("while [ ! -e \"{path}\" ]; do sleep 0.05; done")
    }

    /// Shell snippet that marks the barrier as reached (see [`Barrier::entered`]).
    #[must_use]
    pub fn sh_arrive(&self) -> String {
        format!(": > \"{}\"", self.entered_path().display())
    }

    /// Whether a script has run [`Barrier::sh_arrive`].
    #[must_use]
    pub fn entered(&self) -> bool {
        self.entered_path().exists()
    }

    fn entered_path(&self) -> PathBuf {
        let mut path = self.path.clone().into_os_string();
        path.push(".entered");
        path.into()
    }
}
