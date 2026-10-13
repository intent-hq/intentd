//! Runtime paths for relocated native test archives. Ordinary Cargo tests keep
//! their compiled fallback. An explicit nextest/archive path is authoritative:
//! never hide an incomplete archive by using a file in the producer checkout.
use std::path::PathBuf;

/// Resolve a companion executable, honoring nextest's relocated path.
///
/// # Panics
/// Panics if the selected path is not an absolute path to a file.
pub fn binary_path(name: &str, compiled: &str) -> PathBuf {
    let variable = format!("NEXTEST_BIN_EXE_{name}");
    let path = std::env::var_os(&variable).map_or_else(|| PathBuf::from(compiled), PathBuf::from);
    assert!(
        path.is_absolute() && path.is_file(),
        "{variable}: missing or invalid test binary {}",
        path.display()
    );
    path
}

/// Resolve the package's runtime source/fixture directory.
///
/// # Panics
/// Panics if the selected path is not an absolute path to a directory.
pub fn manifest_dir(compiled: &str) -> PathBuf {
    let path = std::env::var_os("CARGO_MANIFEST_DIR")
        .map_or_else(|| PathBuf::from(compiled), PathBuf::from);
    assert!(
        path.is_absolute() && path.is_dir(),
        "CARGO_MANIFEST_DIR: missing or invalid test source {}",
        path.display()
    );
    path
}
