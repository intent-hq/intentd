//! Make the embedded migrations a tracked compile input.
//!
//! `sqlx::migrate!` embeds `migrations/` (and `postgres/migrations/`) at
//! compile time and takes each migration's version from its file name. On
//! stable Rust the macro can only report the files it read, so:
//! - cargo does not rebuild this crate when a migration is added, and
//! - sccache keys on the contents of those files but not their names, so a
//!   pure rename or renumber was served the previous build (intentd#3861 got
//!   a duplicate 0203 back after renumbering it to 0204).
//!
//! This script reruns when either directory changes and passes a digest of
//! every file name and its contents to the crate as
//! `INTENT_STORE_MIGRATIONS_DIGEST`. `lib.rs` reads it with `env!`, which
//! records an `env-dep` line in the crate's dep-info: cargo and sccache both
//! hash that value, so any add, rename or edit forces a fresh compile.

use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;

const MIGRATION_DIRS: [&str; 2] = ["migrations", "postgres/migrations"];

/// Feeds the sorted file names and contents of `dir` into `hasher`
/// (`sqlx::migrate!` reads only the files directly in `dir`).
fn hash_dir(dir: &Path, hasher: &mut DefaultHasher) {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| {
            entry
                .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
                .path()
        })
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    for path in paths {
        let name = path.file_name().expect("migration dir entry has a name");
        let contents = fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        name.hash(hasher);
        contents.hash(hasher);
    }
}

fn main() {
    // The digest only needs to differ between two trees built by the same
    // toolchain, so std's hasher (stable within a Rust release) is enough.
    let mut hasher = DefaultHasher::new();
    for dir in MIGRATION_DIRS {
        println!("cargo:rerun-if-changed={dir}");
        dir.hash(&mut hasher);
        hash_dir(Path::new(dir), &mut hasher);
    }
    println!(
        "cargo:rustc-env=INTENT_STORE_MIGRATIONS_DIGEST={:016x}",
        hasher.finish()
    );
}
