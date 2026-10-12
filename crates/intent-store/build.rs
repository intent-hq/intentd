use std::{env, fs, path::PathBuf};

fn main() {
    let manifest =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"));
    // Workspace builds use the root lock; cargo package verification has its own
    // lock next to the extracted manifest. Never depend on the checkout layout.
    let lock = manifest
        .ancestors()
        .map(|directory| directory.join("Cargo.lock"))
        .find(|path| path.is_file())
        .expect("Cargo.lock is required for the note index profile revision");
    println!("cargo:rerun-if-changed={}", lock.display());
    println!("cargo:rerun-if-changed=build.rs");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo build output directory"));
    fs::copy(lock, out.join("profile-Cargo.lock")).expect("copy note index dependency fingerprint");
}
