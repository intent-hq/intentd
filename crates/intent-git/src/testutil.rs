//! Test-only git fixtures: a self-cleaning temp directory plus helpers to seed a
//! repository, commit/stage/branch, and write loose blobs.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use git2::{Repository, Signature};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A temp directory removed on drop.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Create a fresh git repository in a unique temp directory.
pub fn init_repo(tag: &str) -> TempDir {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!("intent-git-{tag}-{nanos}-{n}"));
    std::fs::create_dir_all(&path).unwrap();
    let repo = Repository::init(&path).unwrap();
    let mut cfg = repo.config().unwrap();
    cfg.set_str("user.name", "Test").unwrap();
    cfg.set_str("user.email", "test@example.com").unwrap();
    TempDir { path }
}

/// Write a file under the worktree (creating parent dirs).
pub fn write_file(worktree: &Path, rel: &str, contents: &str) {
    let full = worktree.join(rel);
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(full, contents).unwrap();
}

/// Write `rel` and commit it, returning nothing (HEAD advances).
pub fn commit_file(worktree: &Path, rel: &str, contents: &str) {
    write_file(worktree, rel, contents);
    let repo = Repository::open(worktree).unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(Path::new(rel)).unwrap();
    index.write().unwrap();
    let tree_oid = index.write_tree().unwrap();
    let tree = repo.find_tree(tree_oid).unwrap();
    let sig = Signature::now("Test", "test@example.com").unwrap();
    let parents = match repo.head().ok().and_then(|h| h.target()) {
        Some(oid) => vec![repo.find_commit(oid).unwrap()],
        None => Vec::new(),
    };
    let parent_refs: Vec<&git2::Commit> = parents.iter().collect();
    repo.commit(Some("HEAD"), &sig, &sig, "commit", &tree, &parent_refs)
        .unwrap();
}

/// Create a branch named `name` at the current HEAD commit.
pub fn create_branch(worktree: &Path, name: &str) {
    let repo = Repository::open(worktree).unwrap();
    let head = repo.head().unwrap().target().unwrap();
    let commit = repo.find_commit(head).unwrap();
    repo.branch(name, &commit, false).unwrap();
}

/// Check out an existing branch so it becomes the current branch.
pub fn checkout_branch(worktree: &Path, name: &str) {
    let repo = Repository::open(worktree).unwrap();
    let refname = format!("refs/heads/{name}");
    repo.set_head(&refname).unwrap();
    let mut opts = git2::build::CheckoutBuilder::new();
    opts.force();
    repo.checkout_head(Some(&mut opts)).unwrap();
}

/// Write a loose blob into the object DB and return its SHA.
pub(crate) fn write_blob(worktree: &Path, bytes: &[u8]) -> String {
    let repo = Repository::open(worktree).unwrap();
    repo.blob(bytes).unwrap().to_string()
}

/// Commit the superproject's current index with `msg` (HEAD advances).
pub(crate) fn commit_super_index(super_path: &Path, msg: &str) {
    let repo = Repository::open(super_path).unwrap();
    let mut index = repo.index().unwrap();
    let tree_oid = index.write_tree().unwrap();
    let tree = repo.find_tree(tree_oid).unwrap();
    let sig = Signature::now("Test", "test@example.com").unwrap();
    let parent = repo.head().unwrap().peel_to_commit().unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, msg, &tree, &[&parent])
        .unwrap();
}

/// Point the gitlink at `sub_rel` to `sha` and commit — how an upstream
/// bumps a submodule pin (the gitlink target need not exist in the
/// superproject's odb, exactly like the real thing).
pub(crate) fn commit_gitlink_bump(super_path: &Path, sub_rel: &str, sha: &str) {
    let repo = Repository::open(super_path).unwrap();
    let mut index = repo.index().unwrap();
    let entry = git2::IndexEntry {
        ctime: git2::IndexTime::new(0, 0),
        mtime: git2::IndexTime::new(0, 0),
        dev: 0,
        ino: 0,
        mode: 0o160000,
        uid: 0,
        gid: 0,
        file_size: 0,
        id: git2::Oid::from_str(sha).unwrap(),
        flags: 0,
        flags_extended: 0,
        path: sub_rel.as_bytes().to_vec(),
    };
    index.add(&entry).unwrap();
    index.write().unwrap();
    commit_super_index(super_path, "bump gitlink");
}

/// Allow local-path/`file://` submodule clones for this test process:
/// git ≥ 2.38 blocks the `file` transport for *submodule* operations by
/// default (CVE-2022-39253), which would fail a recursive clone /
/// `submodule update --init` of local fixtures. Production caches GitHub
/// repos over https and never hits this override.
pub(crate) fn allow_file_submodules() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("GIT_CONFIG_PARAMETERS", "'protocol.file.allow=always'");
    });
}

/// Register `child_worktree` (an existing repo with at least one commit) as a
/// submodule of `worktree` at the worktree-relative path `sub_rel`, then
/// commit the resulting gitlink + `.gitmodules` — a real submodule fixture
/// for testing the gitlink guard. `worktree` must already have a commit
/// (parent commit for the gitlink-add commit).
pub fn add_submodule(worktree: &Path, child_worktree: &Path, sub_rel: &str) {
    let repo = Repository::open(worktree).unwrap();
    let url = child_worktree.to_string_lossy().to_string();
    let sub_path = worktree.join(sub_rel);
    // `submodule()` (git_submodule_add_setup) creates the target directory;
    // remove it so the local clone below can populate a clean directory.
    let mut sm = repo.submodule(&url, Path::new(sub_rel), true).unwrap();
    let _ = std::fs::remove_dir_all(&sub_path);
    Repository::clone(&url, &sub_path).unwrap();
    sm.add_to_index(true).unwrap();
    sm.add_finalize().unwrap();

    let mut index = repo.index().unwrap();
    index.read(true).unwrap();
    let tree_oid = index.write_tree().unwrap();
    let tree = repo.find_tree(tree_oid).unwrap();
    let sig = Signature::now("Test", "test@example.com").unwrap();
    let parents = match repo.head().ok().and_then(|h| h.target()) {
        Some(oid) => vec![repo.find_commit(oid).unwrap()],
        None => Vec::new(),
    };
    let parent_refs: Vec<&git2::Commit> = parents.iter().collect();
    repo.commit(
        Some("HEAD"),
        &sig,
        &sig,
        "add submodule",
        &tree,
        &parent_refs,
    )
    .unwrap();
}
