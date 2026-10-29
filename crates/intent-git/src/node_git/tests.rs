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
        let commit = self
            .cache
            .checkout("repo-1", &remote(AGENT), &worktree)
            .await
            .unwrap();
        assert_eq!(commit, self.base);
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
    ] {
        assert!(AgentRemote::parse(url).is_err(), "{url}");
    }
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
    let mut command = service_command(GitService::UploadPack, fixture.head.path(), &remote(AGENT));
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
        assert!(
            error.contains("deny updating a hidden ref"),
            "{target}: {error}"
        );
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
