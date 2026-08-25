use std::process::Command;

const BUILD_COMMIT_OVERRIDE: &str = "INTENTD_BUILD_COMMIT";
const EMBEDDED_BUILD_COMMIT: &str = "INTENTD_EMBEDDED_BUILD_COMMIT";

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn valid_commit(value: &str) -> bool {
    (7..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn main() {
    println!("cargo:rerun-if-env-changed={BUILD_COMMIT_OVERRIDE}");
    if let Some(head_path) = git_output(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");
    }

    let build_commit = std::env::var(BUILD_COMMIT_OVERRIDE)
        .ok()
        .filter(|value| valid_commit(value))
        .or_else(|| {
            git_output(&["rev-parse", "--verify", "HEAD"]).filter(|value| valid_commit(value))
        });

    if let Some(build_commit) = build_commit {
        println!("cargo:rustc-env={EMBEDDED_BUILD_COMMIT}={build_commit}");
    }
}
