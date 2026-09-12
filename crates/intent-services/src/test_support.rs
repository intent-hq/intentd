//! Crate-wide test-only scratch-directory helpers (mirrors
//! `intentd/tests/common::test_tempdir` / `test_tempdir_in`).
//!
//! Every test that needs a scratch path goes through these so the directory is
//! swept on drop (including on panic) and `INTENTD_TEST_KEEP_TMP` opts out of
//! the sweep for debugging. Put `SQLite` files *inside* the returned dir
//! (`dir.path().join("x.db")`) so the `-wal` / `-shm` sidecars are swept too.

fn keep_tmp() -> bool {
    std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty())
}

/// Create a temp dir with a recognizable `prefix` under the system temp root.
/// The returned guard removes the dir on drop (including on panic); set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
pub(crate) fn test_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create test tempdir");
    if keep_tmp() {
        dir.disable_cleanup(true);
    }
    dir
}

/// Like [`test_tempdir`], but rooted at `base` instead of the system temp
/// root. Use with `"/tmp"` when the dir must stay short enough for a UDS
/// socket path (macOS caps them at ~104 bytes; `temp_dir()` resolves to a
/// long `/var/folders/...` path).
// Mirrors `intentd/tests/common::test_tempdir_in`; no in-crate caller needs a short root yet.
#[expect(dead_code)]
pub(crate) fn test_tempdir_in(base: &str, prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(base)
        .expect("create test tempdir");
    if keep_tmp() {
        dir.disable_cleanup(true);
    }
    dir
}
