//! Temp superproject + submodule fixture shared by the transfer and
//! checkpoint tests.

use std::path::{Path, PathBuf};

/// Run `git` in `dir` with a fixed identity; panics on failure.
pub(crate) fn git(dir: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "protocol.file.allow=always"])
        .args(args)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@t")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@t")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} in {} failed: {}",
        dir.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A repository with one seed commit on `main`.
pub(crate) fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).expect("mkdir");
    git(dir, &["init", "-q", "-b", "main"]);
    std::fs::write(dir.join("README.md"), "hello\n").expect("write");
    git(dir, &["add", "."]);
    git(dir, &["commit", "-q", "-m", "seed"]);
}

/// Superproject `super/` tracking submodule `sub` whose origin is the
/// bare `origin.git` (seeded from `sub-src/`). Returns
/// `(superproject, bare_origin)`; the submodule checkout at
/// `super/sub` has `origin/main` pointing at the seed commit.
pub(crate) fn superproject_with_submodule(root: &Path) -> (PathBuf, PathBuf) {
    let sub_src = root.join("sub-src");
    init_repo(&sub_src);
    let origin = root.join("origin.git");
    git(root, &["clone", "-q", "--bare", "sub-src", "origin.git"]);
    let sup = root.join("super");
    init_repo(&sup);
    git(
        sup.as_path(),
        &["submodule", "add", "-q", origin.to_str().unwrap(), "sub"],
    );
    git(sup.as_path(), &["commit", "-q", "-m", "add submodule"]);
    (sup, origin)
}

/// Commit a new file inside the checkout at `dir`; returns the SHA.
pub(crate) fn local_commit(dir: &Path, file: &str) -> String {
    std::fs::write(dir.join(file), format!("{file}\n")).expect("write");
    git(dir, &["add", file]);
    git(dir, &["commit", "-q", "-m", &format!("local {file}")]);
    git(dir, &["rev-parse", "HEAD"])
}

/// The nested-under-published-parent scenario (monorepo#4219 follow-up).
pub(crate) struct NestedFixture {
    pub sup: PathBuf,
    pub sub_origin: PathBuf,
    pub inner_origin: PathBuf,
    /// `sub`'s HEAD: records `inner` at `inner_sha`, pushed to its origin.
    pub sub_sha: String,
    /// `sub/inner`'s HEAD on `feat/x`: never pushed.
    pub inner_sha: String,
}

/// Superproject `super/` → submodule `sub` (PUBLISHED: its `main`,
/// bumping the nested `inner` gitlink to a local-only commit, is pushed
/// to `origin.git`) → nested `sub/inner` on `feat/x` at an UNPUBLISHED
/// commit (`deep.txt`). The superproject's gitlink for `sub` is stale
/// (`M sub`), as a WIP snapshot would carry it.
pub(crate) fn nested_unpublished_under_published_parent(root: &Path) -> NestedFixture {
    let inner_src = root.join("inner-src");
    init_repo(&inner_src);
    git(root, &["clone", "-q", "--bare", "inner-src", "inner.git"]);
    let inner_origin = root.join("inner.git");
    let (sup, sub_origin) = superproject_with_submodule(root);
    let sub = sup.join("sub");
    git(&sub, &["checkout", "-q", "main"]);
    git(
        &sub,
        &[
            "submodule",
            "add",
            "-q",
            inner_origin.to_str().unwrap(),
            "inner",
        ],
    );
    git(&sub, &["commit", "-q", "-m", "add inner"]);
    let inner = sub.join("inner");
    git(&inner, &["checkout", "-q", "-b", "feat/x"]);
    let inner_sha = local_commit(&inner, "deep.txt");
    git(&sub, &["add", "inner"]);
    git(&sub, &["commit", "-q", "-m", "bump inner"]);
    git(&sub, &["push", "-q", "origin", "main"]);
    let sub_sha = git(&sub, &["rev-parse", "HEAD"]);
    NestedFixture {
        sup,
        sub_origin,
        inner_origin,
        sub_sha,
        inner_sha,
    }
}
