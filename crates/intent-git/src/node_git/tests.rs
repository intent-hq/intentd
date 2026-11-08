//! Head services are driven through git's `ext::` transport with the same
//! scope arguments head uses, so these tests cover the ref confinement and
//! the node's cache, worktree and WIP steps without the link. The full path
//! through `git-remote-intent` and the link is covered by intentd's
//! `e2e_node_git` tests.

use std::path::Path;
use std::process::Command as StdCommand;

use super::*;
use crate::testutil::{commit_file, init_repo, write_file};

const WS: &str = "ws-1";
const AGENT: &str = "agent-1";
const OTHER: &str = "agent-2";

fn remote(agent: &str) -> AgentRemote {
    AgentRemote::new(WS, agent).unwrap()
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = StdCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn try_ref(dir: &Path, name: &str) -> Option<String> {
    let output = StdCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--verify", "--quiet", name])
        .output()
        .unwrap();
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// A head workspace repository with `base` set for AGENT and OTHER, and a
/// node checkout cache that reaches it through `ext::`.
struct Fixture {
    head: crate::testutil::TempDir,
    node: tempfile::TempDir,
    cache: CheckoutCache,
    base: String,
}

impl Fixture {
    fn new() -> Self {
        let head = init_repo("node-git-head");
        commit_file(head.path(), "README.md", "base\n");
        let base = git(head.path(), &["rev-parse", "HEAD"]);
        let oid = git2::Oid::from_str(&base).unwrap();
        write_base_ref(head.path(), &remote(AGENT), oid).unwrap();
        write_base_ref(head.path(), &remote(OTHER), oid).unwrap();
        let node = tempfile::tempdir().unwrap();
        configure_receive_hooks(&node.path().join("data")).unwrap();
        let mut node_git = NodeGit::new(node.path().join("bin"), node.path().join("git.sock"));
        node_git.test_head = Some(head.path().to_path_buf());
        let cache = CheckoutCache::new(node.path().join("cache"), node_git);
        Self {
            head,
            node,
            cache,
            base,
        }
    }

    fn worktree(&self, name: &str) -> std::path::PathBuf {
        self.node.path().join("worktrees").join(name)
    }

    async fn checkout(&self, name: &str) -> std::path::PathBuf {
        let worktree = self.worktree(name);
        let checkout = self
            .cache
            .checkout("repo-1", &remote(AGENT), &worktree)
            .await
            .unwrap();
        assert_eq!(checkout.base, self.base);
        worktree
    }
}

#[test]
fn remote_url_round_trips_and_rejects_other_spellings() {
    let remote = remote(AGENT);
    assert_eq!(remote.url(), "intent://ws/ws-1/agents/agent-1");
    assert_eq!(AgentRemote::parse(&remote.url()).unwrap(), remote);
    assert_eq!(
        remote.ref_name(AgentRef::Wip),
        "refs/intent/ws/ws-1/agents/agent-1/wip"
    );
    for url in [
        "intent://ws/ws-1/agents/agent-1/",
        "intent://ws/ws-1/agents/../agent-1",
        "intent://ws/ws-1/agents/a/b",
        "intent://ws//agents/agent-1",
        "intent://ws/ws-1/agents/.agent",
        "intent://ws/ws-1/agents/agent-1?x",
        "intent://hub/ws-1/agents/agent-1",
        "intent://user@ws/ws-1/agents/agent-1",
        "ext::sh -c id",
        "intent://ws/ws-1/agents/agent-1/modules/",
        "intent://ws/ws-1/agents/agent-1/modules/a/b",
        "intent://ws/ws-1/agents/agent-1/modules/../repo-1",
        "intent://ws/ws-1/agents/agent-1/modules/.repo",
        "intent://ws/ws-1/agents/agent-1/module/repo-1",
        "intent://ws/ws-1/agents/modules/repo-1",
    ] {
        assert!(AgentRemote::parse(url).is_err(), "{url}");
    }
    let module = remote.with_module("repo-1").unwrap();
    assert_eq!(
        module.url(),
        "intent://ws/ws-1/agents/agent-1/modules/repo-1"
    );
    assert_eq!(AgentRemote::parse(&module.url()).unwrap(), module);
    assert_eq!(module.module_key(), Some("repo-1"));
    assert_eq!(remote.module_key(), None);
    assert_eq!(
        module.ref_name(AgentRef::Base),
        remote.ref_name(AgentRef::Base)
    );
    assert!(remote.with_module("a/b").is_err());
    assert!(GitService::parse("git-upload-archive").is_err());
}

#[tokio::test]
async fn node_fetches_base_and_creates_a_worktree() {
    let fixture = Fixture::new();
    let worktree = fixture.checkout("a").await;
    assert_eq!(git(&worktree, &["rev-parse", "HEAD"]), fixture.base);
    assert_eq!(
        std::fs::read_to_string(worktree.join("README.md")).unwrap(),
        "base\n"
    );
    // A second agent of the same repository reuses the cache.
    let second = fixture.checkout("b").await;
    assert_eq!(git(&second, &["rev-parse", "HEAD"]), fixture.base);
    fixture.cache.remove("repo-1", &second).await.unwrap();
    assert!(!second.exists());
}

#[tokio::test]
async fn upload_pack_advertises_only_the_agents_refs() {
    let fixture = Fixture::new();
    git(
        fixture.head.path(),
        &["update-ref", "refs/heads/secret", "HEAD"],
    );
    let mut command =
        service_command(GitService::UploadPack, fixture.head.path(), &remote(AGENT)).unwrap();
    command.stdin(Stdio::piped());
    let mut child = command.spawn().unwrap();
    {
        use tokio::io::AsyncWriteExt as _;
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(b"0000").await.unwrap();
    }
    let output = child.wait_with_output().await.unwrap();
    let advertised = String::from_utf8_lossy(&output.stdout);
    // Each ref line is `<len><oid> <name>`, the first with `\0<capabilities>`.
    let names: Vec<&str> = advertised
        .lines()
        .filter_map(|line| {
            line.split('\0')
                .next()?
                .split_once(' ')
                .map(|(_, name)| name)
        })
        .collect();
    assert_eq!(
        names,
        ["refs/intent/ws/ws-1/agents/agent-1/base"],
        "{advertised}"
    );
}

#[tokio::test]
async fn fetching_another_agents_refs_is_refused() {
    let fixture = Fixture::new();
    std::fs::create_dir_all(fixture.node.path().join("cache")).unwrap();
    let cache = fixture.node.path().join("cache").join("repo-1.git");
    git(
        fixture.node.path(),
        &["init", "--quiet", "--bare", cache.to_str().unwrap()],
    );
    let url = fixture.cache.git().remote_url(&remote(AGENT));
    let other_base = remote(OTHER).ref_name(AgentRef::Base);
    commit_file(fixture.head.path(), "secret.txt", "secret\n");
    let secret = git(fixture.head.path(), &["rev-parse", "HEAD"]);
    for refspec in [
        format!("{other_base}:refs/stolen"),
        "refs/heads/master:refs/stolen".to_owned(),
        // A want for an unadvertised object is refused even when it exists.
        format!("{secret}:refs/stolen"),
    ] {
        let mut fetch = fixture.cache.git().command(&cache);
        fetch.args(["fetch", "--quiet", &url, &refspec]);
        assert!(run(fetch).await.is_err(), "{refspec}");
    }
    assert_eq!(try_ref(&cache, "refs/stolen"), None);
}

#[tokio::test]
async fn wip_push_lands_under_the_agents_refs() {
    let fixture = Fixture::new();
    let worktree = fixture.checkout("a").await;
    git(
        &worktree,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "agent work",
        ],
    );
    write_file(&worktree, "README.md", "dirty\n");
    write_file(&worktree, "new.txt", "untracked\n");
    let pushed = fixture
        .cache
        .git()
        .push_wip(&worktree, &remote(AGENT))
        .await
        .unwrap();

    let head = fixture.head.path();
    assert_eq!(pushed.head, git(&worktree, &["rev-parse", "HEAD"]));
    assert_ne!(pushed.wip, pushed.head);
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Head)),
        Some(pushed.head.clone())
    );
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Wip)),
        Some(pushed.wip.clone())
    );
    assert_eq!(
        git(head, &["rev-parse", &format!("{}^", pushed.wip)]),
        pushed.head
    );
    assert_eq!(
        git(head, &["show", &format!("{}:new.txt", pushed.wip)]),
        "untracked"
    );
    assert_eq!(
        git(head, &["show", &format!("{}:README.md", pushed.wip)]),
        "dirty"
    );
    // The worktree's own index and HEAD are untouched.
    assert_eq!(
        git(&worktree, &["status", "--porcelain"]),
        "M README.md\n?? new.txt".to_owned()
    );

    // A clean tree pushes wip == head, replacing the earlier wip.
    git(&worktree, &["checkout", "--quiet", "--", "README.md"]);
    std::fs::remove_file(worktree.join("new.txt")).unwrap();
    let clean = fixture
        .cache
        .git()
        .push_wip(&worktree, &remote(AGENT))
        .await
        .unwrap();
    assert_eq!(clean.wip, clean.head);
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Wip)),
        Some(clean.head)
    );
}

#[tokio::test]
async fn pushes_outside_the_agents_prefix_are_rejected() {
    let fixture = Fixture::new();
    let worktree = fixture.checkout("a").await;
    let head = fixture.head.path();
    let url = fixture.cache.git().remote_url(&remote(AGENT));
    for target in [
        "refs/heads/master".to_owned(),
        "refs/heads/evil".to_owned(),
        "refs/tags/evil".to_owned(),
        remote(OTHER).ref_name(AgentRef::Head),
        "refs/intent/ws/ws-2/agents/agent-1/head".to_owned(),
    ] {
        let before = try_ref(head, &target);
        let mut push = fixture.cache.git().command(&worktree);
        push.args(["push", "--force", &url, &format!("HEAD:{target}")]);
        let error = run(push).await.unwrap_err().to_string();
        // The kept stderr tail can cut the start of git's message.
        assert!(error.contains("a hidden ref"), "{target}: {error}");
        assert_eq!(try_ref(head, &target), before, "{target} changed");
    }
}

#[test]
fn head_commit_and_repo_key_name_the_shared_repository() {
    let repo = init_repo("node-git-head");
    commit_file(repo.path(), "a.txt", "a\n");
    let head = git(repo.path(), &["rev-parse", "HEAD"]);
    assert_eq!(head_commit(repo.path()).unwrap(), head);

    // A linked worktree of the same repository shares its cache key.
    let linked = repo.path().join("linked");
    git(
        repo.path(),
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            linked.to_str().unwrap(),
        ],
    );
    let key = repo_key(repo.path()).unwrap();
    assert_eq!(repo_key(&linked).unwrap(), key);
    assert!(key.starts_with("repo-") && key.len() == 5 + 32, "{key}");
    assert!(hub::component(&key).is_ok());

    let other = init_repo("node-git-other");
    commit_file(other.path(), "b.txt", "b\n");
    assert_ne!(repo_key(other.path()).unwrap(), key);
}

#[test]
fn head_commit_fails_without_a_commit() {
    let empty = tempfile::tempdir().unwrap();
    git(empty.path(), &["init", "--quiet"]);
    assert!(head_commit(empty.path()).is_err());
}

fn commit_all(worktree: &Path, message: &str) {
    git(worktree, &["add", "--all"]);
    git(
        worktree,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "--quiet",
            "-m",
            message,
        ],
    );
}

#[tokio::test]
async fn relaunch_without_a_checkout_restores_head_and_wip() {
    let fixture = Fixture::new();
    let worktree = fixture.checkout("a").await;
    write_file(&worktree, "committed.txt", "agent commit\n");
    commit_all(&worktree, "agent work");
    write_file(&worktree, "README.md", "dirty\n");
    write_file(&worktree, "new.txt", "untracked\n");
    let pushed = fixture
        .cache
        .git()
        .push_wip(&worktree, &remote(AGENT))
        .await
        .unwrap();
    assert_ne!(pushed.wip, pushed.head);
    // The final stop (or the bind sweep) removes the checkout.
    fixture.cache.remove("repo-1", &worktree).await.unwrap();

    let restored = fixture.worktree("a-restored");
    let checkout = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &restored)
        .await
        .unwrap();
    assert_eq!(checkout.base, fixture.base);
    assert_eq!(checkout.at, pushed.head);
    assert_eq!(checkout.unrestored_wip, None);
    assert_eq!(git(&restored, &["rev-parse", "HEAD"]), pushed.head);
    assert_eq!(
        std::fs::read_to_string(restored.join("committed.txt")).unwrap(),
        "agent commit\n"
    );
    assert_eq!(
        std::fs::read_to_string(restored.join("README.md")).unwrap(),
        "dirty\n"
    );
    assert_eq!(
        std::fs::read_to_string(restored.join("new.txt")).unwrap(),
        "untracked\n"
    );
    // The wip changes are uncommitted, as they were when pushed.
    assert_eq!(
        git(&restored, &["status", "--porcelain"]),
        "M README.md\n?? new.txt".to_owned()
    );
    // Head's refs are untouched.
    let head = fixture.head.path();
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Head)),
        Some(pushed.head)
    );
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Wip)),
        Some(pushed.wip)
    );
}

#[tokio::test]
async fn relaunch_with_a_clean_wip_checks_out_head_only() {
    let fixture = Fixture::new();
    let worktree = fixture.checkout("a").await;
    write_file(&worktree, "committed.txt", "agent commit\n");
    commit_all(&worktree, "agent work");
    let pushed = fixture
        .cache
        .git()
        .push_wip(&worktree, &remote(AGENT))
        .await
        .unwrap();
    assert_eq!(pushed.wip, pushed.head);
    fixture.cache.remove("repo-1", &worktree).await.unwrap();

    let restored = fixture.worktree("a-restored");
    let checkout = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &restored)
        .await
        .unwrap();
    assert_eq!(checkout.at, pushed.head);
    assert_eq!(checkout.unrestored_wip, None);
    assert_eq!(git(&restored, &["status", "--porcelain"]), "");
}

/// Point the agent's refs on head at a `wip` that changes README on top of
/// `on` and a saved head that changes it differently, so the wip does not
/// apply. Returns `(wip, saved_head)`; head's own `HEAD` ends at `on`.
fn conflicting_refs(head: &Path, on: &str, wip_text: &str, head_text: &str) -> (String, String) {
    git(head, &["reset", "--quiet", "--hard", on]);
    commit_file(head, "README.md", wip_text);
    let wip = git(head, &["rev-parse", "HEAD"]);
    git(head, &["reset", "--quiet", "--hard", on]);
    commit_file(head, "README.md", head_text);
    let saved_head = git(head, &["rev-parse", "HEAD"]);
    git(head, &["reset", "--quiet", "--hard", on]);
    git(
        head,
        &["update-ref", &remote(AGENT).ref_name(AgentRef::Wip), &wip],
    );
    git(
        head,
        &[
            "update-ref",
            &remote(AGENT).ref_name(AgentRef::Head),
            &saved_head,
        ],
    );
    (wip, saved_head)
}

/// Push `refspec` from `dir` to the agent's refs on head; whether head
/// accepted it.
async fn push_raw(cache: &CheckoutCache, dir: &Path, refspec: &str) -> bool {
    let url = cache.git.remote_url(&remote(AGENT));
    cache
        .git
        .git(dir, &["push", "--quiet", &url, refspec])
        .await
        .is_ok()
}

#[tokio::test]
async fn a_wip_that_does_not_apply_is_pinned_under_unapplied_and_reported() {
    let fixture = Fixture::new();
    let head = fixture.head.path();
    let (wip, saved_head) = conflicting_refs(head, &fixture.base, "wip\n", "head\n");
    let wip_ref = remote(AGENT).ref_name(AgentRef::Wip);
    let head_ref = remote(AGENT).ref_name(AgentRef::Head);
    let pinned = remote(AGENT).unapplied_ref(&wip);
    assert_eq!(
        pinned,
        format!("refs/intent/ws/ws-1/agents/agent-1/unapplied/{wip}")
    );

    let restored = fixture.worktree("a");
    let checkout = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &restored)
        .await
        .unwrap();
    assert_eq!(checkout.at, saved_head);
    assert_eq!(checkout.unrestored_wip, Some(wip.clone()));
    assert_eq!(git(&restored, &["rev-parse", "HEAD"]), saved_head);
    assert_eq!(
        std::fs::read_to_string(restored.join("README.md")).unwrap(),
        "head\n"
    );
    assert_eq!(git(&restored, &["status", "--porcelain"]), "");
    // The wip is pinned on head before the agent runs, and readable here.
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));
    assert_eq!(git(&restored, &["rev-parse", &pinned]), wip);
    assert_eq!(try_ref(head, &head_ref), Some(saved_head.clone()));

    // The agent's first push replaces wip; the pinned ref keeps the work.
    write_file(&restored, "README.md", "second\n");
    let pushed = fixture
        .cache
        .git()
        .push_wip(&restored, &remote(AGENT))
        .await
        .unwrap();
    assert_ne!(pushed.wip, wip);
    assert_eq!(try_ref(head, &wip_ref), Some(pushed.wip.clone()));
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));

    // A second failure pins its own wip without touching the first.
    commit_file(head, "README.md", "head2\n");
    let head2 = git(head, &["rev-parse", "HEAD"]);
    git(head, &["update-ref", &head_ref, &head2]);
    fixture.cache.remove("repo-1", &restored).await.unwrap();
    let again = fixture.worktree("a-again");
    let checkout = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &again)
        .await
        .unwrap();
    assert_eq!(checkout.at, head2);
    assert_eq!(checkout.unrestored_wip, Some(pushed.wip.clone()));
    let second = remote(AGENT).unapplied_ref(&pushed.wip);
    assert_eq!(try_ref(head, &second), Some(pushed.wip.clone()));
    assert_eq!(try_ref(head, &pinned), Some(wip));
}

#[tokio::test]
async fn unapplied_refs_are_create_only_and_named_by_their_commit() {
    let fixture = Fixture::new();
    let head = fixture.head.path();
    let (wip, _) = conflicting_refs(head, &fixture.base, "wip\n", "head\n");
    let worktree = fixture.worktree("a");
    fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &worktree)
        .await
        .unwrap();
    let pinned = remote(AGENT).unapplied_ref(&wip);
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));
    let at = git(&worktree, &["rev-parse", "HEAD"]);

    // No update, forced or not, and no delete.
    assert!(!push_raw(&fixture.cache, &worktree, &format!("+HEAD:{pinned}")).await);
    assert!(!push_raw(&fixture.cache, &worktree, &format!(":{pinned}")).await);
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));
    // A new ref must be named by the commit it holds.
    let misnamed = remote(AGENT).unapplied_ref(&fixture.base);
    assert!(!push_raw(&fixture.cache, &worktree, &format!("HEAD:{misnamed}")).await);
    assert_eq!(try_ref(head, &misnamed), None);
    let other = format!("{}unapplied/other", remote(AGENT).ref_prefix());
    assert!(!push_raw(&fixture.cache, &worktree, &format!("HEAD:{other}")).await);
    assert_eq!(try_ref(head, &other), None);
    // Creating one named by its commit is allowed; head and wip still
    // update as before.
    let own = remote(AGENT).unapplied_ref(&at);
    assert!(push_raw(&fixture.cache, &worktree, &format!("HEAD:{own}")).await);
    assert_eq!(try_ref(head, &own), Some(at));
    let wip_ref = remote(AGENT).ref_name(AgentRef::Wip);
    assert!(push_raw(&fixture.cache, &worktree, &format!("+HEAD:{wip_ref}")).await);
}

/// A saved wip head will not pin fails the checkout, so the agent never
/// starts over it.
#[cfg(unix)]
#[tokio::test]
async fn a_wip_head_will_not_pin_fails_the_checkout() {
    use std::os::unix::fs::PermissionsExt as _;
    let fixture = Fixture::new();
    let head = fixture.head.path();
    conflicting_refs(head, &fixture.base, "wip\n", "head\n");
    // Head cannot create refs under the agent's prefix.
    let refs = head.join(".git/refs/intent/ws/ws-1/agents/agent-1");
    assert!(refs.is_dir(), "the agent's refs are loose");
    std::fs::set_permissions(&refs, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(refs.join("probe"), "").is_ok() {
        // Permissions do not bind this user (root): nothing to test.
        std::fs::remove_file(refs.join("probe")).unwrap();
        std::fs::set_permissions(&refs, std::fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!("skipping: the refs directory stays writable");
        return;
    }
    let result = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &fixture.worktree("a"))
        .await;
    std::fs::set_permissions(&refs, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = result.unwrap_err().to_string();
    assert!(error.contains("pin the unapplied wip"), "{error}");
    assert!(!fixture.worktree("a").exists());
}

/// A pin that landed but whose answer was lost: the relaunch pins the same
/// commit again, which succeeds, and the checkout proceeds.
#[tokio::test]
async fn repinning_the_same_commit_succeeds() {
    let fixture = Fixture::new();
    let head = fixture.head.path();
    let (wip, saved_head) = conflicting_refs(head, &fixture.base, "wip\n", "head\n");
    let pinned = remote(AGENT).unapplied_ref(&wip);
    git(head, &["update-ref", &pinned, &wip]);
    let checkout = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &fixture.worktree("a"))
        .await
        .unwrap();
    assert_eq!(checkout.at, saved_head);
    assert_eq!(checkout.unrestored_wip, Some(wip.clone()));
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));
    // An explicit same-value push is accepted too.
    let worktree = fixture.worktree("a");
    assert!(push_raw(&fixture.cache, &worktree, &format!("{wip}:{pinned}")).await);
    assert_eq!(try_ref(head, &pinned), Some(wip));
}

/// An existing unapplied ref holding another commit is never moved: the
/// pin is refused and the checkout fails.
#[tokio::test]
async fn a_pin_over_a_different_commit_is_refused() {
    let fixture = Fixture::new();
    let head = fixture.head.path();
    let (wip, _) = conflicting_refs(head, &fixture.base, "wip\n", "head\n");
    let pinned = remote(AGENT).unapplied_ref(&wip);
    // Hand-made: the ref holds wip's parent (base), so the pin would be a
    // fast-forward.
    git(head, &["update-ref", &pinned, &fixture.base]);
    let error = fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &fixture.worktree("a"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("pin the unapplied wip"), "{error}");
    assert_eq!(try_ref(head, &pinned), Some(fixture.base.clone()));
    // No checkout is left for a later launch to reuse unpinned.
    assert!(!fixture.worktree("a").exists());
}

#[test]
fn only_receive_pack_runs_heads_update_hook() {
    let data = tempfile::tempdir().unwrap();
    configure_receive_hooks(data.path()).unwrap();
    let hooks_path = |service| {
        let command = service_command(service, Path::new("/repo"), &remote(AGENT)).unwrap();
        command
            .as_std()
            .get_args()
            .filter_map(|arg| {
                arg.to_str()?
                    .strip_prefix("core.hooksPath=")
                    .map(str::to_owned)
            })
            .last()
    };
    let receive = hooks_path(GitService::ReceivePack).unwrap();
    assert!(Path::new(&receive).join("update").is_file(), "{receive}");
    assert_eq!(
        hooks_path(GitService::UploadPack).as_deref(),
        Some("/dev/null")
    );
}

/// Inside the agent's prefix only `head`, `wip` and
/// `unapplied/<full lowercase sha>` may be pushed.
#[tokio::test]
async fn the_hook_allows_only_head_wip_and_well_formed_unapplied_refs() {
    let fixture = Fixture::new();
    let worktree = fixture.checkout("a").await;
    let head = fixture.head.path();
    // A commit no agent ref holds yet, so every push below moves a ref.
    write_file(&worktree, "work.txt", "work\n");
    commit_all(&worktree, "work");
    let at = git(&worktree, &["rev-parse", "HEAD"]);
    let prefix = remote(AGENT).ref_prefix();
    for name in [
        format!("UNAPPLIED/{at}"),
        format!("unapplied/{}", at.to_uppercase()),
        format!("unapplied/x/{at}"),
        format!("unapplied/{}", &at[..12]),
        "unknown".to_owned(),
        "base".to_owned(),
        "head/extra".to_owned(),
    ] {
        let target = format!("{prefix}{name}");
        let before = try_ref(head, &target);
        assert!(
            !push_raw(&fixture.cache, &worktree, &format!("+HEAD:{target}")).await,
            "{name} accepted"
        );
        assert_eq!(try_ref(head, &target), before, "{name} changed");
    }
    assert!(push_raw(&fixture.cache, &worktree, &format!("+HEAD:{prefix}head")).await);
    assert!(push_raw(&fixture.cache, &worktree, &format!("+HEAD:{prefix}wip")).await);
    assert!(
        push_raw(
            &fixture.cache,
            &worktree,
            &format!("HEAD:{prefix}unapplied/{at}")
        )
        .await
    );
}

/// A hook removed or altered between pushes is reinstalled before the next
/// one, which is still checked; one that cannot be reinstalled refuses the
/// push.
#[tokio::test]
async fn a_removed_or_altered_hook_is_reinstalled_or_the_push_refused() {
    let fixture = Fixture::new();
    let head = fixture.head.path();
    let (wip, _) = conflicting_refs(head, &fixture.base, "wip\n", "head\n");
    let worktree = fixture.worktree("a");
    fixture
        .cache
        .checkout("repo-1", &remote(AGENT), &worktree)
        .await
        .unwrap();
    let pinned = remote(AGENT).unapplied_ref(&wip);
    let hooks = fixture.node.path().join("data").join("node-git-hooks");

    std::fs::remove_dir_all(&hooks).unwrap();
    assert!(!push_raw(&fixture.cache, &worktree, &format!("+HEAD:{pinned}")).await);
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));
    assert!(hooks.join("update").is_file(), "reinstalled");

    std::fs::write(hooks.join("update"), "#!/bin/sh\nexit 0\n").unwrap();
    assert!(!push_raw(&fixture.cache, &worktree, &format!("+HEAD:{pinned}")).await);
    assert_eq!(try_ref(head, &pinned), Some(wip.clone()));
    assert_eq!(
        std::fs::read_to_string(hooks.join("update")).unwrap(),
        UPDATE_HOOK
    );

    // The hooks directory cannot be made: no command, so no push.
    std::fs::remove_dir_all(&hooks).unwrap();
    std::fs::remove_dir_all(fixture.node.path().join("data")).unwrap();
    std::fs::write(fixture.node.path().join("data"), "not a directory").unwrap();
    assert!(service_command(GitService::ReceivePack, head, &remote(AGENT)).is_err());
    assert!(service_command(GitService::UploadPack, head, &remote(AGENT)).is_ok());
}

/// Startup clears whatever an earlier run left in the hooks directory.
#[test]
fn configuring_the_hooks_removes_stale_files() {
    let data = tempfile::tempdir().unwrap();
    let hooks = data.path().join("node-git-hooks");
    std::fs::create_dir_all(&hooks).unwrap();
    std::fs::write(hooks.join("stale"), "x").unwrap();
    std::fs::write(hooks.join("update"), "#!/bin/sh\nexit 0\n").unwrap();
    configure_receive_hooks(data.path()).unwrap();
    assert!(!hooks.join("stale").exists());
    assert_eq!(
        std::fs::read_to_string(hooks.join("update")).unwrap(),
        UPDATE_HOOK
    );
}

/// The machine's unpinned submodules: populated ones not among the pins,
/// top-most only, searched for under pinned parents; unpopulated ones are
/// skipped, and a checkout that is not a repository is an error.
#[test]
fn unpinned_submodules_are_the_populated_ones_outside_the_pins() {
    let root = tempfile::tempdir().unwrap();
    let run = |dir: &Path, args: &[&str]| {
        let mut all = vec![
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "protocol.file.allow=always",
        ];
        all.extend_from_slice(args);
        git(dir, &all)
    };
    let repo = |name: &str| {
        let dir = root.path().join("src").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        run(&dir, &["init", "--quiet"]);
        std::fs::write(dir.join("f.txt"), name).unwrap();
        run(&dir, &["add", "f.txt"]);
        run(&dir, &["commit", "--quiet", "-m", name]);
        dir
    };
    let add = |dir: &Path, url: &Path, path: &str| {
        run(
            dir,
            &["submodule", "add", "--quiet", url.to_str().unwrap(), path],
        );
        run(dir, &["commit", "--quiet", "-m", path]);
    };
    let (inner, deep, absent) = (repo("q"), repo("v"), repo("d"));
    let pinned = repo("p");
    add(&pinned, &inner, "q");
    let unpinned = repo("u");
    add(&unpinned, &deep, "v");
    let work = root.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    run(&work, &["init", "--quiet"]);
    for (url, path) in [(&pinned, "p"), (&unpinned, "u"), (&absent, "d")] {
        add(&work, url, path);
    }
    run(
        &work,
        &["submodule", "update", "--quiet", "--init", "--recursive"],
    );
    run(&work, &["submodule", "deinit", "--quiet", "-f", "d"]);
    let pin = |path: &str| SubmodulePin {
        path: path.into(),
        name: path.into(),
        commit: String::new(),
        module_key: String::new(),
    };

    assert_eq!(
        unpinned_submodules(&work, &[pin("p")]).unwrap(),
        ["p/q", "u"]
    );
    assert_eq!(
        unpinned_submodules(&work, &[pin("p"), pin("p/q"), pin("u")]).unwrap(),
        ["u/v"]
    );
    assert!(unpinned_submodules(&root.path().join("missing"), &[]).is_err());
}

const FORGE_URL: &str = "https://github.com/acme/widget.git";

fn try_config(dir: &Path, key: &str) -> Option<String> {
    let output = StdCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(["config", "--get", key])
        .output()
        .unwrap();
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// The forge branch and `origin` belong to one checkout of the shared
/// cache, keep the agent's uncommitted work, survive a relaunch (also one
/// stopped mid-rebase), go with a launch without `forge`, and the branch
/// goes with the checkout.
#[tokio::test]
async fn a_forge_checkout_gets_its_own_branch_and_origin() {
    let fixture = Fixture::new();
    let a = fixture.checkout("a").await;
    let b = fixture.checkout("b").await;
    std::fs::write(a.join("wip.txt"), "wip\n").unwrap();
    fixture
        .cache
        .set_forge(&a, "intent/a", FORGE_URL, "main")
        .await
        .unwrap();
    assert_eq!(git(&a, &["symbolic-ref", "HEAD"]), "refs/heads/intent/a");
    assert_eq!(git(&a, &["rev-parse", "HEAD"]), fixture.base);
    assert_eq!(std::fs::read_to_string(a.join("wip.txt")).unwrap(), "wip\n");
    assert_eq!(
        try_config(&a, "remote.origin.url").as_deref(),
        Some(FORGE_URL)
    );
    assert_eq!(
        try_config(&a, "branch.intent/a.merge").as_deref(),
        Some("refs/heads/main")
    );
    assert_eq!(try_config(&a, "push.default").as_deref(), Some("upstream"));
    assert_eq!(
        try_config(&b, "remote.origin.url"),
        None,
        "b sees no origin"
    );
    assert_eq!(try_config(&b, "push.default"), None);

    // A relaunch keeps the branch and the agent's commits on it.
    std::fs::write(a.join("wip.txt"), "more\n").unwrap();
    git(&a, &["add", "wip.txt"]);
    git(
        &a,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-qm",
            "x",
        ],
    );
    let moved = git(&a, &["rev-parse", "HEAD"]);
    fixture
        .cache
        .set_forge(&a, "intent/a", FORGE_URL, "main")
        .await
        .unwrap();
    assert_eq!(git(&a, &["rev-parse", "intent/a"]), moved);

    // Stopped mid-rebase (detached, rebase state present): no switch.
    git(&a, &["checkout", "-q", "--detach"]);
    let rebase = git(
        &a,
        &[
            "rev-parse",
            "--path-format=absolute",
            "--git-path",
            "rebase-merge",
        ],
    );
    std::fs::create_dir_all(&rebase).unwrap();
    fixture
        .cache
        .set_forge(&a, "intent/a", FORGE_URL, "main")
        .await
        .unwrap();
    assert_eq!(git(&a, &["rev-parse", "--abbrev-ref", "HEAD"]), "HEAD");
    std::fs::remove_dir_all(&rebase).unwrap();

    // A launch without forge: no origin.
    fixture.cache.clear_forge(&a, "intent/a").await;
    assert_eq!(try_config(&a, "remote.origin.url"), None);
    assert_eq!(try_config(&a, "push.default"), None);

    // The branch goes with the checkout.
    fixture.cache.remove("repo-1", &a).await.unwrap();
    fixture.cache.delete_branch("intent/a").await;
    assert_eq!(try_ref(&b, "refs/heads/intent/a"), None);
}
