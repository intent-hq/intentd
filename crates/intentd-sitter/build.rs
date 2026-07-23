//! Re-export the compile-time target triple to the crate.
//!
//! `env!("TARGET")` is only set for build scripts, but the sitter needs the
//! triple at runtime as the platform key into the channel manifest
//! (`platforms.<triple>`), so forward it via `rustc-env`.

fn main() {
    println!(
        "cargo:rustc-env=SITTER_TARGET_TRIPLE={}",
        std::env::var("TARGET").expect("cargo sets TARGET for build scripts")
    );
    println!("cargo:rerun-if-changed=build.rs");
}
