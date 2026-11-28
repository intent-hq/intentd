//! The embedded migration set must match the `.sql` files on disk.
//!
//! `sqlx::migrate!` embeds the migrations at compile time and takes each
//! version from the file name. A build that predates a rename, renumber, add
//! or edit still embeds the old set: cargo misses an added file unless the
//! directory is a tracked input, and sccache keys on file contents but not
//! file names, so a pure renumber was served the old build (intentd#3861 got
//! a duplicate 0203 back). `build.rs` closes both gaps; this test fails on
//! such a stale build.
//!
//! It is an integration test on purpose: it links the `intent-store` rlib
//! the way dependent crates do. sccache caches rlibs but never test
//! binaries, so a unit test is always compiled fresh and cannot see a stale
//! cached build.

use std::path::{Path, PathBuf};

use sqlx::migrate::Migrator;

/// `(version, description, sql)` for each `*.sql` file, sorted by version,
/// parsed the way `sqlx::migrate!` names them (`<version>_<description>.sql`,
/// underscores in the description become spaces).
fn on_disk(dir: &Path) -> Vec<(i64, String, String)> {
    let mut files: Vec<(i64, String, String)> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("migration dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .map(|path| {
            let stem = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("UTF-8 migration file name");
            let (version, description) = stem
                .split_once('_')
                .unwrap_or_else(|| panic!("{stem}: expected <version>_<description>.sql"));
            let version = version
                .parse()
                .unwrap_or_else(|e| panic!("{stem}: version {version:?}: {e}"));
            let sql = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
            (version, description.replace('_', " "), sql)
        })
        .collect();
    files.sort();
    files
}

fn embedded(migrator: &Migrator) -> Vec<(i64, String, String)> {
    let mut migrations: Vec<(i64, String, String)> = migrator
        .iter()
        .map(|m| {
            (
                m.version,
                m.description.to_string(),
                m.sql.as_str().to_owned(),
            )
        })
        .collect();
    migrations.sort();
    migrations
}

fn names(migrations: &[(i64, String, String)]) -> Vec<String> {
    migrations
        .iter()
        .map(|(version, description, _)| format!("{version:04} {description}"))
        .collect()
}

#[track_caller]
fn assert_matches_disk(migrator: &Migrator, relative_dir: &str) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative_dir);
    let embedded = embedded(migrator);
    let on_disk = on_disk(&dir);
    let (embedded_names, disk_names) = (names(&embedded), names(&on_disk));
    let only_embedded: Vec<_> = embedded_names
        .iter()
        .filter(|n| !disk_names.contains(n))
        .collect();
    let only_on_disk: Vec<_> = disk_names
        .iter()
        .filter(|n| !embedded_names.contains(n))
        .collect();
    assert!(
        embedded_names == disk_names,
        "the migrations embedded in this build differ from {}: the build is stale\n  \
         embedded only: {only_embedded:?}\n  on disk only: {only_on_disk:?}",
        dir.display()
    );
    for ((version, description, sql), (_, _, file_sql)) in embedded.iter().zip(&on_disk) {
        assert!(
            sql == file_sql,
            "embedded SQL of migration {version:04} {description} differs from {}: the build is stale",
            dir.display()
        );
    }
}

#[test]
fn embedded_sqlite_migrations_match_migration_files() {
    assert_matches_disk(intent_store::embedded_migrator(), "migrations");
}

#[cfg(feature = "postgres")]
#[test]
fn embedded_postgres_migrations_match_migration_files() {
    assert_matches_disk(&intent_store::postgres::MIGRATOR, "postgres/migrations");
}
