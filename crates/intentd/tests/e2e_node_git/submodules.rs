//! The prepared checkout's submodules over the link (nodeProtocol 10),
//! with real git on both sides: the machine checks out every submodule
//! head has populated, at head's submodule `HEAD`s, from per-module caches,
//! and pushes module work back leaves first, then the superproject.
//!
//! Head's layout: `a` (with nested `a/b`), `c` (`update = none`) and `d`
//! (not populated). `a`'s `HEAD` is a head-only commit ahead of its
//! gitlink, and `c` has an uncommitted file on head.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use intent_core::node_link::SubmodulePin;
use intent_git::hub::AgentRef;
use intent_git::node_git;

use super::{git, remote, try_ref, wait, Fixture, AGENT, REPO};

fn commit(dir: &Path, message: &str) {
    git(
        dir,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "protocol.file.allow=always",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            message,
        ],
    );
}

fn commit_file(dir: &Path, file: &str, text: &str) -> String {
    std::fs::write(dir.join(file), text).unwrap();
    git(dir, &["add", file]);
    commit(dir, file);
    git(dir, &["rev-parse", "HEAD"])
}

fn add_submodule(dir: &Path, url: &Path, path: &str) {
    git(
        dir,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "--quiet",
            url.to_str().unwrap(),
            path,
        ],
    );
}

/// An upstream repository with one commit, for `git submodule add`.
fn upstream(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "--quiet"]);
    commit_file(&dir, "README.md", &format!("{name}\n"));
    dir
}

/// The submodules' state as `git submodule status --recursive` shows it,
/// without the describe column (it names local refs).
fn status(dir: &Path) -> Vec<String> {
    git(dir, &["submodule", "status", "--recursive"])
        .lines()
        .map(|line| {
            let mut fields = line.split_whitespace();
            let sha = fields.next().unwrap_or_default();
            let path = fields.next().unwrap_or_default();
            format!("{sha} {path}")
        })
        .collect()
}

struct Layout {
    pins: Vec<SubmodulePin>,
    /// Head's submodule work trees.
    a: PathBuf,
    b: PathBuf,
    c: PathBuf,
}

/// Build the layout in head's repository and do what head does at a
/// placement: capture the pins, then write the agent's `base` refs.
fn layout(fixture: &mut Fixture) -> Layout {
    let head = fixture.head.path().to_path_buf();
    let upstreams = fixture.node.path().join("upstreams");
    let b_url = upstream(&upstreams, "b");
    let a_url = upstream(&upstreams, "a");
    add_submodule(&a_url, &b_url, "b");
    commit(&a_url, "add b");
    for name in ["c", "d"] {
        upstream(&upstreams, name);
    }
    add_submodule(&head, &a_url, "a");
    git(
        &head,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "update",
            "--quiet",
            "--init",
            "--recursive",
        ],
    );
    add_submodule(&head, &upstreams.join("c"), "c");
    add_submodule(&head, &upstreams.join("d"), "d");
    git(
        &head,
        &["config", "-f", ".gitmodules", "submodule.c.update", "none"],
    );
    git(&head, &["add", ".gitmodules"]);
    commit(&head, "submodules");
    git(&head, &["submodule", "deinit", "--quiet", "--force", "d"]);
    let a = head.join("a");
    commit_file(&a, "ahead.txt", "only on head\n");
    let c = head.join("c");
    std::fs::write(c.join("dirty.txt"), "uncommitted on head\n").unwrap();

    fixture.base = git(&head, &["rev-parse", "HEAD"]);
    let agent = remote(AGENT);
    git(
        &head,
        &["update-ref", &agent.ref_name(AgentRef::Base), &fixture.base],
    );
    let pins = node_git::submodule_pins(&head).unwrap();
    assert_eq!(
        pins.iter().map(|pin| pin.path.as_str()).collect::<Vec<_>>(),
        ["a", "a/b", "c"]
    );
    for pin in &pins {
        let module = node_git::module_repository(&head, &pin.path, &pin.module_key).unwrap();
        git(
            &module.worktree,
            &["update-ref", &agent.ref_name(AgentRef::Base), &pin.commit],
        );
    }
    fixture.head_state.pins.lock().unwrap().clone_from(&pins);
    Layout {
        pins,
        b: a.join("b"),
        a,
        c,
    }
}

async fn checkout(fixture: &Fixture, worktree: &Path, pins: &[SubmodulePin]) {
    let agent = remote(AGENT);
    tokio::time::timeout(wait(), fixture.cache.checkout(REPO, &agent, worktree))
        .await
        .unwrap()
        .unwrap();
    let modules = tokio::time::timeout(
        wait(),
        fixture.cache.checkout_modules(REPO, &agent, worktree, pins),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(modules.len(), pins.len());
}

async fn push(
    fixture: &Fixture,
    worktree: &Path,
    pins: &[SubmodulePin],
) -> intent_core::Result<node_git::WipRefs> {
    tokio::time::timeout(
        wait(),
        fixture
            .cache
            .git()
            .push_work(worktree, &remote(AGENT), pins),
    )
    .await
    .unwrap()
}

fn agent_ref(module: &Path, kind: AgentRef) -> Option<String> {
    try_ref(module, &remote(AGENT).ref_name(kind))
}

/// The gitlink at `path` in `commit`'s tree.
fn gitlink(repo: &Path, commit: &str, path: &str) -> String {
    git(repo, &["rev-parse", &format!("{commit}:{path}")])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_machine_checks_out_every_populated_submodule_at_heads_commits() {
    let mut fixture = Fixture::start().await;
    let layout = layout(&mut fixture);
    let worktree = fixture.worktree("a");
    checkout(&fixture, &worktree, &layout.pins).await;

    for (path, on_head) in [("a", &layout.a), ("a/b", &layout.b), ("c", &layout.c)] {
        assert_eq!(
            git(&worktree.join(path), &["rev-parse", "HEAD"]),
            git(on_head, &["rev-parse", "HEAD"]),
            "{path}"
        );
    }
    // The head-only commit of `a` arrived, and `a` is ahead of its gitlink
    // as on head.
    assert_eq!(
        std::fs::read_to_string(worktree.join("a/ahead.txt")).unwrap(),
        "only on head\n"
    );
    assert!(worktree.join("d").is_dir());
    assert_eq!(std::fs::read_dir(worktree.join("d")).unwrap().count(), 0);
    assert!(!worktree.join("c/dirty.txt").exists());
    assert_eq!(status(&worktree), status(fixture.head.path()));
    assert!(status(&worktree).iter().any(|line| line.starts_with('+')));
    // No submodule URL is configured on the machine, and the superproject
    // cache's other worktrees do not see the activation.
    let other = fixture.worktree("plain");
    tokio::time::timeout(wait(), fixture.cache.checkout(REPO, &remote(AGENT), &other))
        .await
        .unwrap()
        .unwrap();
    let other_config = std::process::Command::new("git")
        .arg("-C")
        .arg(&other)
        .args(["config", "--get-regexp", "^submodule\\."])
        .output()
        .unwrap();
    assert!(other_config.stdout.is_empty(), "{other_config:?}");
    let urls = std::process::Command::new("git")
        .arg("-C")
        .arg(&worktree)
        .args(["config", "--get-regexp", "^submodule\\..*\\.url$"])
        .output()
        .unwrap();
    assert!(urls.stdout.is_empty(), "{urls:?}");
    assert!(!git(&other, &["rev-parse", "--is-bare-repository"]).contains("true"));

    // A pin whose base on head is another commit fails the checkout.
    let mut wrong = layout.pins.clone();
    wrong[2].commit = fixture.base.clone();
    let mismatched = fixture.worktree("wrong");
    tokio::time::timeout(
        wait(),
        fixture.cache.checkout(REPO, &remote(AGENT), &mismatched),
    )
    .await
    .unwrap()
    .unwrap();
    let error = fixture
        .cache
        .checkout_modules(REPO, &remote(AGENT), &mismatched, &wrong)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("submodule c:"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn module_work_comes_back_leaves_first() {
    let mut fixture = Fixture::start().await;
    let layout = layout(&mut fixture);
    let worktree = fixture.worktree("a");
    checkout(&fixture, &worktree, &layout.pins).await;

    // The agent commits in `a/b` and leaves an edit in `a`.
    let b_commit = commit_file(&worktree.join("a/b"), "work.txt", "b work\n");
    std::fs::write(worktree.join("a/edit.txt"), "a edit\n").unwrap();
    let pushed = push(&fixture, &worktree, &layout.pins).await.unwrap();

    assert_eq!(agent_ref(&layout.b, AgentRef::Head), Some(b_commit.clone()));
    assert_eq!(agent_ref(&layout.b, AgentRef::Wip), Some(b_commit.clone()));
    let a_wip = agent_ref(&layout.a, AgentRef::Wip).unwrap();
    assert_eq!(
        git(&layout.a, &["show", &format!("{a_wip}:edit.txt")]),
        "a edit"
    );
    assert_eq!(gitlink(&layout.a, &a_wip, "b"), b_commit);
    assert_eq!(gitlink(fixture.head.path(), &pushed.wip, "a"), a_wip);
    // `c` is clean: its refs are its pin, and the superproject names it.
    let c_pin = &layout.pins[2].commit;
    assert_eq!(agent_ref(&layout.c, AgentRef::Wip).as_ref(), Some(c_pin));
    assert_eq!(gitlink(fixture.head.path(), &pushed.wip, "c"), *c_pin);
    // The module objects live in the module repositories only.
    let superproject_has = std::process::Command::new("git")
        .arg("-C")
        .arg(fixture.head.path())
        .args(["cat-file", "-e", &b_commit])
        .status()
        .unwrap();
    assert!(!superproject_has.success());

    // A refused superproject push: the modules landed first and name a
    // consistent set; the superproject's refs keep their last value.
    std::fs::write(worktree.join("a/b/more.txt"), "more\n").unwrap();
    fixture
        .head_state
        .refuse_superproject_push
        .store(true, Ordering::SeqCst);
    let error = push(&fixture, &worktree, &layout.pins).await.unwrap_err();
    assert!(error.to_string().contains("refused"), "{error}");
    let b_wip = agent_ref(&layout.b, AgentRef::Wip).unwrap();
    assert_ne!(b_wip, b_commit);
    assert_eq!(
        git(&layout.b, &["show", &format!("{b_wip}:more.txt")]),
        "more"
    );
    let a_wip2 = agent_ref(&layout.a, AgentRef::Wip).unwrap();
    assert_eq!(gitlink(&layout.a, &a_wip2, "b"), b_wip);
    assert_eq!(
        try_ref(fixture.head.path(), &remote(AGENT).ref_name(AgentRef::Wip)),
        Some(pushed.wip)
    );
    // The next push lands everything. It makes the `wip` commits again
    // (stamped with the push's time, so their ids may differ from the
    // refused run's): the superproject names the modules' current `wip`s,
    // which carry the same work.
    fixture
        .head_state
        .refuse_superproject_push
        .store(false, Ordering::SeqCst);
    let last = push(&fixture, &worktree, &layout.pins).await.unwrap();
    let a_wip3 = agent_ref(&layout.a, AgentRef::Wip).unwrap();
    let b_wip3 = agent_ref(&layout.b, AgentRef::Wip).unwrap();
    assert_eq!(gitlink(fixture.head.path(), &last.wip, "a"), a_wip3);
    assert_eq!(gitlink(&layout.a, &a_wip3, "b"), b_wip3);
    assert_eq!(
        git(&layout.b, &["show", &format!("{b_wip3}:more.txt")]),
        "more"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ordinary_merge_on_head_leaves_the_submodules_clean() {
    let mut fixture = Fixture::start().await;
    let layout = layout(&mut fixture);
    let worktree = fixture.worktree("a");
    checkout(&fixture, &worktree, &layout.pins).await;

    commit_file(&worktree.join("a/b"), "work.txt", "b work\n");
    git(&worktree.join("a"), &["add", "b"]);
    commit(&worktree.join("a"), "bump b");
    git(&worktree, &["add", "a"]);
    commit(&worktree, "bump a");
    push(&fixture, &worktree, &layout.pins).await.unwrap();

    let head_ref = remote(AGENT).ref_name(AgentRef::Head);
    for dir in [&layout.b, &layout.a, &fixture.head.path().to_path_buf()] {
        git(dir, &["merge", "--quiet", "--ff-only", &head_ref]);
    }
    let after = status(fixture.head.path());
    assert!(after.iter().all(|line| !line.starts_with('+')), "{after:?}");
    assert_eq!(
        std::fs::read_to_string(layout.b.join("work.txt")).unwrap(),
        "b work\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_relaunch_restores_every_module_and_pins_a_conflicting_module_wip() {
    let mut fixture = Fixture::start().await;
    let layout = layout(&mut fixture);
    let worktree = fixture.worktree("a");
    checkout(&fixture, &worktree, &layout.pins).await;

    let b_commit = commit_file(&worktree.join("a/b"), "work.txt", "b work\n");
    std::fs::write(worktree.join("a/b/dirty.txt"), "b dirty\n").unwrap();
    std::fs::write(worktree.join("a/edit.txt"), "a edit\n").unwrap();
    std::fs::write(worktree.join("top.txt"), "top\n").unwrap();
    push(&fixture, &worktree, &layout.pins).await.unwrap();

    tokio::time::timeout(wait(), fixture.cache.remove(REPO, &worktree))
        .await
        .unwrap()
        .unwrap();
    let restored = fixture.worktree("a-restored");
    checkout(&fixture, &restored, &layout.pins).await;
    assert_eq!(git(&restored.join("a/b"), &["rev-parse", "HEAD"]), b_commit);
    for (file, text) in [
        ("a/b/dirty.txt", "b dirty\n"),
        ("a/edit.txt", "a edit\n"),
        ("top.txt", "top\n"),
    ] {
        assert_eq!(
            std::fs::read_to_string(restored.join(file)).unwrap(),
            text,
            "{file}"
        );
    }
    assert_eq!(
        git(&restored.join("a/b"), &["status", "--porcelain"]),
        "?? dirty.txt"
    );

    // `c`'s saved head moved under a wip made on its pin: that wip does
    // not apply, so it is pinned under `unapplied/` in c's repository.
    let c = &layout.c;
    let pin = layout.pins[2].commit.clone();
    let agent = remote(AGENT);
    git(c, &["checkout", "--quiet", "--detach", &pin]);
    let wip = commit_file(c, "README.md", "wip\n");
    git(c, &["checkout", "--quiet", "--detach", &pin]);
    let saved_head = commit_file(c, "README.md", "head\n");
    git(c, &["checkout", "--quiet", "--detach", &pin]);
    git(c, &["update-ref", &agent.ref_name(AgentRef::Wip), &wip]);
    git(
        c,
        &["update-ref", &agent.ref_name(AgentRef::Head), &saved_head],
    );
    tokio::time::timeout(wait(), fixture.cache.remove(REPO, &restored))
        .await
        .unwrap()
        .unwrap();
    let again = fixture.worktree("a-again");
    tokio::time::timeout(wait(), fixture.cache.checkout(REPO, &agent, &again))
        .await
        .unwrap()
        .unwrap();
    let modules = tokio::time::timeout(
        wait(),
        fixture
            .cache
            .checkout_modules(REPO, &agent, &again, &layout.pins),
    )
    .await
    .unwrap()
    .unwrap();
    let c_checkout = modules.iter().find(|module| module.path == "c").unwrap();
    assert_eq!(c_checkout.at, saved_head);
    assert_eq!(c_checkout.unrestored_wip, Some(wip.clone()));
    assert_eq!(try_ref(c, &agent.unapplied_ref(&wip)), Some(wip.clone()));
    assert_eq!(
        try_ref(fixture.head.path(), &agent.unapplied_ref(&wip)),
        None
    );
    assert_eq!(git(&again.join("c"), &["rev-parse", "HEAD"]), saved_head);
    assert_eq!(git(&again.join("c"), &["status", "--porcelain"]), "");
    assert!(modules
        .iter()
        .filter(|module| module.path != "c")
        .all(|module| module.unrestored_wip.is_none()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pins_that_are_not_a_safe_ordered_set_are_refused() {
    let mut fixture = Fixture::start().await;
    let layout = layout(&mut fixture);
    let pin = |path: &str| SubmodulePin {
        path: path.into(),
        ..layout.pins[0].clone()
    };
    for (pins, why) in [
        (vec![pin("")], "empty"),
        (vec![pin("/a")], "absolute"),
        (vec![pin("a/../c")], "dot-dot"),
        (vec![pin("a/./b")], "dot"),
        (vec![pin("a//b")], "empty segment"),
        (vec![pin("a"), pin("a")], "repeated"),
        (vec![pin("a/b"), pin("a")], "child before parent"),
    ] {
        let error = node_git::validate_pins(&pins).unwrap_err().to_string();
        assert!(!error.is_empty(), "{why}");
    }
    node_git::validate_pins(&layout.pins).unwrap();

    // A symlinked step inside the checkout is refused before git runs.
    let worktree = fixture.worktree("link");
    tokio::time::timeout(
        wait(),
        fixture.cache.checkout(REPO, &remote(AGENT), &worktree),
    )
    .await
    .unwrap()
    .unwrap();
    std::fs::remove_dir(worktree.join("c")).unwrap();
    let outside = fixture.node.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, worktree.join("c")).unwrap();
    let error = fixture
        .cache
        .checkout_modules(REPO, &remote(AGENT), &worktree, &layout.pins)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("symlink"), "{error}");
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
}

/// A kept checkout whose submodule the agent removed by hand gets it back
/// at the next launch, though the module cache still registers the old
/// worktree; a submodule the agent removed in a commit is not brought back
/// by a relaunch, nor pushed again as a gitlink.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_submodules_are_re_added_by_hand_and_stay_removed_by_commit() {
    let mut fixture = Fixture::start().await;
    let layout = layout(&mut fixture);
    let worktree = fixture.worktree("a");
    checkout(&fixture, &worktree, &layout.pins).await;

    std::fs::remove_dir_all(worktree.join("c")).unwrap();
    std::fs::create_dir(worktree.join("c")).unwrap();
    let readded = tokio::time::timeout(
        wait(),
        fixture
            .cache
            .checkout_modules(REPO, &remote(AGENT), &worktree, &layout.pins),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        readded.iter().map(|m| m.path.as_str()).collect::<Vec<_>>(),
        ["c"]
    );
    assert_eq!(
        git(&worktree.join("c"), &["rev-parse", "HEAD"]),
        layout.pins[2].commit
    );

    git(&worktree, &["rm", "--quiet", "-r", "--force", "c"]);
    commit(&worktree, "drop c");
    let pushed = push(&fixture, &worktree, &layout.pins).await.unwrap();
    tokio::time::timeout(wait(), fixture.cache.remove(REPO, &worktree))
        .await
        .unwrap()
        .unwrap();
    let restored = fixture.worktree("a-restored");
    tokio::time::timeout(
        wait(),
        fixture.cache.checkout(REPO, &remote(AGENT), &restored),
    )
    .await
    .unwrap()
    .unwrap();
    let modules = tokio::time::timeout(
        wait(),
        fixture
            .cache
            .checkout_modules(REPO, &remote(AGENT), &restored, &layout.pins),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        modules.iter().map(|m| m.path.as_str()).collect::<Vec<_>>(),
        ["a", "a/b"]
    );
    assert!(!restored.join("c").exists());
    let again = push(&fixture, &restored, &layout.pins).await.unwrap();
    assert_eq!(again.head, pushed.head);
    let files = git(fixture.head.path(), &["ls-tree", "--name-only", &again.wip]);
    assert!(files.lines().all(|file| file != "c"), "{files}");
}
