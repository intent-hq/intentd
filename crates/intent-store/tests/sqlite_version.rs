//! The daemon must run on `SQLite` 3.51.3 or later.
//!
//! `SQLite` 3.7.0 through 3.51.2 can corrupt a WAL database when two
//! connections write or checkpoint at the same instant (the "WAL-reset" bug,
//! <https://sqlite.org/wal.html>; intent-hq/intent#8592). The fix ships in the
//! `SQLite` that libsqlite3-sys 0.37 bundles, so this fails if a dependency
//! change ever resolves an older bundled copy.

use intent_store::Store;

/// 3.51.3 encoded as `SQLITE_VERSION_NUMBER` (X*1000000 + Y*1000 + Z).
const MIN_VERSION_NUMBER: i32 = 3_051_003;

/// Parse "X.Y.Z" into the `SQLITE_VERSION_NUMBER` encoding.
fn version_number(version: &str) -> i32 {
    let mut parts = version.split('.').map(|part| {
        part.parse::<i32>()
            .unwrap_or_else(|e| panic!("bad sqlite_version {version:?}: {e}"))
    });
    let mut next = || parts.next().unwrap_or(0);
    next() * 1_000_000 + next() * 1_000 + next()
}

#[test]
fn version_number_parses_sqlite_versions() {
    assert_eq!(version_number("3.46.0"), 3_046_000);
    assert_eq!(version_number("3.51.3"), MIN_VERSION_NUMBER);
    assert_eq!(version_number("3.52"), 3_052_000);
}

#[test]
fn linked_sqlite_library_has_the_wal_reset_fix() {
    // SAFETY: `sqlite3_libversion_number` only reads a compiled-in constant.
    let linked = unsafe { libsqlite3_sys::sqlite3_libversion_number() };
    assert!(
        linked >= MIN_VERSION_NUMBER,
        "linked SQLite {linked} predates 3.51.3"
    );
}

// The bundled headers too: an older copy fails this test binary's build.
const _: () = assert!(
    libsqlite3_sys::SQLITE_VERSION_NUMBER >= MIN_VERSION_NUMBER,
    "bundled SQLite headers predate 3.51.3"
);

#[tokio::test]
async fn store_pools_run_sqlite_with_the_wal_reset_fix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    for (name, pool) in [("read", store.read_pool()), ("write", store.write_pool())] {
        let version: String = sqlx::query_scalar("SELECT sqlite_version()")
            .fetch_one(pool)
            .await
            .unwrap();
        assert!(
            version_number(&version) >= MIN_VERSION_NUMBER,
            "{name} pool runs SQLite {version}, which predates 3.51.3"
        );
    }
    store.close().await;
}
