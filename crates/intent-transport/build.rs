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

fn git_rerun_paths_with(mut output: impl FnMut(&[&str]) -> Option<String>) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(head_path) = output(&["rev-parse", "--git-path", "HEAD"]) {
        paths.push(head_path);
    }

    if let Some(symbolic_ref) = output(&["symbolic-ref", "-q", "HEAD"]) {
        if let Some(ref_path) = output(&["rev-parse", "--git-path", &symbolic_ref]) {
            paths.push(ref_path);
        }
        if let Some(packed_refs_path) = output(&["rev-parse", "--git-path", "packed-refs"]) {
            paths.push(packed_refs_path);
        }
    }
    paths
}

fn emit_git_rerun_paths() {
    for path in git_rerun_paths_with(git_output) {
        println!("cargo:rerun-if-changed={path}");
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed={BUILD_COMMIT_OVERRIDE}");
    emit_git_rerun_paths();

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

#[cfg(test)]
mod tests {
    use super::git_rerun_paths_with;

    #[test]
    fn symbolic_head_watches_branch_ref_and_packed_refs() {
        let paths = git_rerun_paths_with(|args| match args {
            ["rev-parse", "--git-path", "HEAD"] => Some(".git/HEAD".into()),
            ["symbolic-ref", "-q", "HEAD"] => Some("refs/heads/main".into()),
            ["rev-parse", "--git-path", "refs/heads/main"] => {
                Some("/repo/.git/refs/heads/main".into())
            }
            ["rev-parse", "--git-path", "packed-refs"] => Some("/repo/.git/packed-refs".into()),
            _ => None,
        });

        assert_eq!(
            paths,
            [
                ".git/HEAD",
                "/repo/.git/refs/heads/main",
                "/repo/.git/packed-refs"
            ]
        );
    }

    #[test]
    fn detached_head_only_watches_head() {
        let paths = git_rerun_paths_with(|args| match args {
            ["rev-parse", "--git-path", "HEAD"] => Some(".git/HEAD".into()),
            _ => None,
        });

        assert_eq!(paths, [".git/HEAD"]);
    }

    #[test]
    fn missing_git_metadata_has_no_paths() {
        assert!(git_rerun_paths_with(|_| None).is_empty());
    }
}
