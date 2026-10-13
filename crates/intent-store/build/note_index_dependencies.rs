//! Dependency input for the note index profile revision.
//!
//! Shared by `build.rs` and the crate's unit tests so the tested code is the
//! code that produces the fingerprint input.

use std::path::{Path, PathBuf};

/// Files read from disk for the dependency profile.
pub struct Inputs {
    pub lock: String,
    pub manifests: Vec<String>,
    pub watched: Vec<PathBuf>,
}

/// Locate the lockfile governing `manifest_dir`.
///
/// # Errors
/// Returns an actionable message when a required file is missing or unreadable.
pub fn collect(manifest_dir: &Path) -> Result<Inputs, String> {
    // Workspace builds use the root lock; cargo package verification has its own
    // lock next to the extracted manifest. Never depend on the checkout layout.
    let lock_path = manifest_dir
        .ancestors()
        .map(|directory| directory.join("Cargo.lock"))
        .find(|path| path.is_file())
        .ok_or("Cargo.lock is required for the note index profile revision")?;
    let lock = read(&lock_path)?;
    Ok(Inputs {
        lock,
        manifests: Vec::new(),
        watched: vec![lock_path],
    })
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|error| {
        format!(
            "note index profile: cannot read {}: {error}",
            path.display()
        )
    })
}

/// Canonical dependency profile text hashed into the note index profile.
///
/// # Errors
/// Returns an actionable message for malformed or ambiguous inputs.
pub fn dependency_profile(lock: &str, _manifests: &[&str]) -> Result<String, String> {
    Ok(lock.to_owned())
}
