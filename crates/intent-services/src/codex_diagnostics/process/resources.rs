//! Shared temporary directories used by more than one diagnostic process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A planning handle. Transfer a clone to each dependent guard before startup.
/// Drop planning handles before the owner's explicit cleanup so final removal
/// errors can be returned by that cleanup.
#[derive(Clone)]
pub(crate) struct ProbeDependency(Arc<Directory>);

/// A process lease. Dropping without confirmed cleanup makes retention sticky
/// for every owner, including cleanup tasks detached by cancellation.
pub(super) struct ProbeHome(Option<Arc<Directory>>);

impl ProbeHome {
    pub fn new(home: tempfile::TempDir) -> Self {
        Self(Some(Arc::new(Directory {
            home: Some(home),
            retained: AtomicBool::new(false),
        })))
    }

    pub fn dependency(&self) -> ProbeDependency {
        ProbeDependency(self.0.as_ref().expect("live directory lease").clone())
    }

    pub fn remove(mut self) -> std::io::Result<()> {
        if let Some(directory) = self.0.take().and_then(Arc::into_inner) {
            directory.remove()?;
        }
        Ok(())
    }
}

impl From<ProbeDependency> for ProbeHome {
    fn from(dependency: ProbeDependency) -> Self {
        Self(Some(dependency.0))
    }
}

impl Drop for ProbeHome {
    fn drop(&mut self) {
        if let Some(directory) = self.0.take() {
            directory.retained.store(true, Ordering::Release);
        }
    }
}

struct Directory {
    home: Option<tempfile::TempDir>,
    retained: AtomicBool,
}

impl Directory {
    fn remove(mut self) -> std::io::Result<()> {
        if !self.retained.load(Ordering::Acquire) {
            if let Some(home) = self.home.take() {
                return home.close();
            }
        }
        Ok(())
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if self.retained.load(Ordering::Acquire) {
            if let Some(home) = self.home.take() {
                let _ = home.keep();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unconfirmed_lease_retention_survives_other_confirmed_releases() {
        let directory = crate::test_support::test_tempdir("codex-shared-home");
        let path = directory.path().to_owned();
        let owner = ProbeHome::new(directory);
        let pending = ProbeHome::from(owner.dependency());
        owner.remove().unwrap();
        assert!(path.is_dir());
        drop(pending);
        assert!(path.is_dir());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn planning_handle_does_not_require_process_cleanup() {
        let directory = crate::test_support::test_tempdir("codex-unused-dependency");
        let path = directory.path().to_owned();
        let owner = ProbeHome::new(directory);
        let unused = owner.dependency();
        drop(unused);
        owner.remove().unwrap();
        assert!(!path.exists());
    }
}
