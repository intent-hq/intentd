use std::{env, fs, path::PathBuf};

#[path = "build/note_index_dependencies.rs"]
mod note_index_dependencies;

fn main() {
    let manifest =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"));
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build/note_index_dependencies.rs");
    let inputs =
        note_index_dependencies::collect(&manifest).unwrap_or_else(|error| panic!("{error}"));
    for path in &inputs.watched {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    let manifests: Vec<&str> = inputs.manifests.iter().map(String::as_str).collect();
    let profile = note_index_dependencies::dependency_profile(&inputs.lock, &manifests)
        .unwrap_or_else(|error| panic!("{error}"));
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo build output directory"));
    fs::write(out.join("note-index-dependencies.txt"), profile)
        .expect("write note index dependency fingerprint");
}
