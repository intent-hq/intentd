//! A source repository with one populated submodule, for e2e tests that
//! create a workspace from it.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A superproject on `main` (`tracked.txt`, content `seed\n`) whose
/// submodule `sub` holds `inner.txt` (content `inner\n`). `.gitmodules`
/// records the submodule as a `file://` URL, so a recursive clone transfers
/// it as a pack and streams its object progress like a remote one.
///
/// Two traps this fixture exists for:
///
/// - **Only a create from a URL populates the submodule.** A
///   `workspace.create` with `githubUrl: repo.url()` clones recursively and
///   fills `sub/`. A create from a local path (`repositoryPath`) is a plain
///   linked worktree: the gitlink is there, but `sub/` stays empty on every
///   daemon, so a test that waits for `sub/inner.txt` waits forever.
/// - **The daemon must allow `file://` submodules.** Modern git refuses the
///   file transport for submodules by default, so spawn every daemon that
///   clones this repository with [`SubmoduleRepo::DAEMON_ENV`] in its
///   environment.
pub struct SubmoduleRepo {
    /// The superproject: the source to create a workspace from.
    pub superproject: PathBuf,
    /// The submodule's upstream repository, as `.gitmodules` records it.
    pub upstream: PathBuf,
}

impl SubmoduleRepo {
    /// The daemon env entry that lets its child `git` clone local
    /// (`file://` or plain-path) submodules. `GIT_CONFIG_PARAMETERS` is command-line-scoped config that
    /// every git the daemon spawns inherits; the clone pipeline appends its
    /// own entries after it, so production behavior is unchanged.
    pub const DAEMON_ENV: (&'static str, &'static str) =
        ("GIT_CONFIG_PARAMETERS", "'protocol.file.allow=always'");
    /// The submodule's path in the superproject.
    pub const SUBMODULE: &'static str = "sub";
    /// The file committed in the submodule.
    pub const SUBMODULE_FILE: &'static str = "inner.txt";
    /// The file committed in the superproject.
    pub const TRACKED_FILE: &'static str = "tracked.txt";

    /// Build the superproject at `dir/source-repo` and the submodule's
    /// upstream at `dir/child-repo`. The superproject carries a repo-level
    /// identity, so daemon-side commits in its linked worktrees work on
    /// runners without a global one.
    #[track_caller]
    pub fn create(dir: &Path) -> Self {
        let upstream = dir.join("child-repo");
        init(&upstream);
        std::fs::write(upstream.join(Self::SUBMODULE_FILE), "inner\n").expect("write");
        git(&upstream, &["add", Self::SUBMODULE_FILE]);
        git(&upstream, &["commit", "-q", "-m", "inner seed"]);

        let superproject = dir.join("source-repo");
        init(&superproject);
        std::fs::write(superproject.join(Self::TRACKED_FILE), "seed\n").expect("write");
        git(&superproject, &["add", Self::TRACKED_FILE]);
        git(&superproject, &["commit", "-q", "-m", "seed"]);
        git(
            &superproject,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "-q",
                &format!("file://{}", upstream.display()),
                Self::SUBMODULE,
            ],
        );
        git(&superproject, &["commit", "-q", "-m", "add submodule"]);
        Self {
            superproject,
            upstream,
        }
    }

    /// The `githubUrl` to create a workspace from; see the type docs for why
    /// a local path does not do.
    pub fn url(&self) -> String {
        format!("file://{}", self.superproject.display())
    }

    /// Whether the checkout at `checkout` has the submodule populated.
    pub fn is_populated(checkout: &Path) -> bool {
        checkout
            .join(Self::SUBMODULE)
            .join(Self::SUBMODULE_FILE)
            .exists()
    }
}

#[track_caller]
fn init(dir: &Path) {
    std::fs::create_dir_all(dir).expect("mkdir");
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["config", "user.name", "e2e"]);
    git(dir, &["config", "user.email", "e2e@example.com"]);
}

#[track_caller]
fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} in {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
}
