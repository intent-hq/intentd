//! Agent refs over the node link, end to end: head serves git sessions on
//! a real pinned-TLS git connection, the node fetches and pushes through the
//! intentd binary running as `git-remote-intent`, and head confines every
//! session to the refs of agents on the node's lease.
#![cfg(unix)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use intent_core::node_link::{LeaseBinding, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS};
use intent_core::nodes::{NodeArch, NodeCounter, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::{BoxFuture, Error, Result};
use intent_git::hub::AgentRef;
use intent_git::node_git::{AgentRemote, CheckoutCache, NodeGit, HELPER_NAME, SOCKET_ENV};
use intent_transport::generate_certificate;
use intent_transport::node_link::{
    dial_git, dial_link, serve_git, serve_helpers, BindingFile, CertificatePin, LeaseToken,
    NodeConnection, NodeEndpoint, NodeGitAccess, NodeGitSessions, NodeListener, NodeListenerConfig,
};

use crate::common::test_tempdir_in;

const WS: &str = "ws-1";
const AGENT: &str = "agent-1";
/// An agent that runs on another lease.
const OTHER: &str = "agent-2";
const REPO: &str = "repo-1";

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(30))
}

fn remote(agent: &str) -> AgentRemote {
    AgentRemote::new(WS, agent).unwrap()
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
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
    let output = Command::new("git")
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

/// Head's authority: agents on the lease, and the current binding.
struct Lease {
    repository: PathBuf,
    binding: LeaseBinding,
    agents: BTreeSet<String>,
}

impl NodeGitAccess for Lease {
    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            binding.ensure_current(&self.binding)?;
            if remote.workspace_id() != WS || !self.agents.contains(remote.agent_id()) {
                return Err(Error::Forbidden("agent not on this lease".into()));
            }
            Ok(self.repository.clone())
        })
    }
}

struct Fixture {
    head: tempfile::TempDir,
    node: tempfile::TempDir,
    cache: CheckoutCache,
    helper_dir: PathBuf,
    socket: PathBuf,
    base: String,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
    _link: intent_transport::node_link::DialedLink,
    _node_link: Box<dyn std::any::Any + Send>,
}

impl Fixture {
    async fn start() -> Self {
        let head = test_tempdir_in("/tmp", "node-git-head");
        git(head.path(), &["init", "--quiet"]);
        std::fs::write(head.path().join("README.md"), "base\n").unwrap();
        git(head.path(), &["add", "README.md"]);
        git(
            head.path(),
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "--quiet",
                "-m",
                "base",
            ],
        );
        let base = git(head.path(), &["rev-parse", "HEAD"]);
        for agent in [AGENT, OTHER] {
            git(
                head.path(),
                &["update-ref", &remote(agent).ref_name(AgentRef::Base), &base],
            );
        }

        // The node's listener; head dials the link, then the git connection.
        let node = test_tempdir_in("/tmp", "node-git-node");
        let certificate =
            generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let token = LeaseToken::new(format!("token-{}", "x".repeat(32))).unwrap();
        let mut listener = NodeListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            NodeListenerConfig {
                node_id: "node-1".into(),
                token: token.clone(),
                binding_file: BindingFile::new(node.path().join("binding.json")),
                certificate: certificate.clone(),
            },
            Arc::new(|_| LinkWelcome {
                protocol: 0,
                node_id: String::new(),
                running_agents: vec![],
                capabilities: intent_core::node_link::MachineCapabilities::new(
                    AgentPlatform {
                        os: NodeOs::Linux,
                        arch: NodeArch::X86_64,
                    },
                    "test",
                    2,
                    "2026-10-30T00:00:00Z",
                ),
                hosts: Vec::new(),
                tc_address: None,
            }),
        )
        .await
        .unwrap();
        let endpoint = NodeEndpoint {
            url: format!("wss://{}", listener.local_addr()),
            pin: CertificatePin::parse(&certificate.fingerprint256).unwrap(),
            token,
            tailcat: None,
        };
        let binding = LeaseBinding {
            head_id: "head-1".into(),
            node_id: "node-1".into(),
            lease_id: "lease-1".into(),
            generation: NodeCounter(1),
        };
        let link = dial_link(
            &endpoint,
            LinkHello {
                protocol: NODE_LINK_PROTOCOLS,
                intentd_version: "test".into(),
                binding: binding.clone(),
                acked_seq: NodeCounter(0),
            },
        )
        .await
        .unwrap();
        let node_link = tokio::time::timeout(wait(), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let head_git = dial_git(&endpoint, &binding).await.unwrap();
        let Some(NodeConnection::Git(node_git)) = tokio::time::timeout(wait(), listener.accept())
            .await
            .unwrap()
        else {
            panic!("expected the git connection");
        };

        let lease = Lease {
            repository: head.path().to_path_buf(),
            binding,
            agents: BTreeSet::from([AGENT.to_owned()]),
        };
        let serving = tokio::spawn(serve_git(head_git, Arc::new(lease)));

        // The node side: sessions on the git connection, the helper socket,
        // and intentd linked as the helper.
        let sessions = NodeGitSessions::new();
        sessions.attach(node_git);
        let socket = node.path().join("git.sock");
        let helpers = tokio::spawn(serve_helpers(
            tokio::net::UnixListener::bind(&socket).unwrap(),
            sessions,
        ));
        let helper_dir = node.path().join("bin");
        NodeGit::install_helper(&helper_dir, Path::new(env!("CARGO_BIN_EXE_intentd"))).unwrap();
        let cache = CheckoutCache::new(
            node.path().join("cache"),
            NodeGit::new(helper_dir.clone(), socket.clone()),
        );
        Self {
            head,
            node,
            cache,
            helper_dir,
            socket,
            base,
            _tasks: vec![serving, helpers],
            _link: link,
            _node_link: Box::new((listener, node_link)),
        }
    }

    fn worktree(&self, name: &str) -> PathBuf {
        self.node.path().join("worktrees").join(name)
    }

    /// A plain `git push` from `dir` the way an agent's shell would run it
    /// on the node.
    fn push(&self, dir: &Path, url: &str, refspec: &str) -> std::process::Output {
        let path = std::env::join_paths(std::iter::once(self.helper_dir.clone()).chain(
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
        ))
        .unwrap();
        Command::new("git")
            .arg("-C")
            .arg(dir)
            .env("PATH", path)
            .env(SOCKET_ENV, &self.socket)
            .env_remove("GIT_DIR")
            .args(["push", "--force", url, refspec])
            .output()
            .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_fetches_base_and_pushes_wip_over_the_link() {
    let fixture = Fixture::start().await;
    assert!(fixture.helper_dir.join(HELPER_NAME).exists());
    let worktree = fixture.worktree("a");
    let commit = tokio::time::timeout(
        wait(),
        fixture.cache.checkout(REPO, &remote(AGENT), &worktree),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(commit, fixture.base);
    assert_eq!(git(&worktree, &["rev-parse", "HEAD"]), fixture.base);
    assert_eq!(
        std::fs::read_to_string(worktree.join("README.md")).unwrap(),
        "base\n"
    );

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
            "work",
        ],
    );
    std::fs::write(worktree.join("notes.txt"), "dirty\n").unwrap();
    let pushed = tokio::time::timeout(
        wait(),
        fixture.cache.git().push_wip(&worktree, &remote(AGENT)),
    )
    .await
    .unwrap()
    .unwrap();
    let head = fixture.head.path();
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Head)),
        Some(pushed.head.clone())
    );
    assert_eq!(
        try_ref(head, &remote(AGENT).ref_name(AgentRef::Wip)),
        Some(pushed.wip.clone())
    );
    assert_eq!(
        git(head, &["show", &format!("{}:notes.txt", pushed.wip)]),
        "dirty"
    );
    assert_eq!(
        git(head, &["rev-parse", &format!("{}^", pushed.wip)]),
        pushed.head
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushes_outside_the_agents_refs_are_rejected() {
    let fixture = Fixture::start().await;
    let worktree = fixture.worktree("a");
    tokio::time::timeout(
        wait(),
        fixture.cache.checkout(REPO, &remote(AGENT), &worktree),
    )
    .await
    .unwrap()
    .unwrap();
    let head = fixture.head.path();
    let url = remote(AGENT).url();
    for target in [
        "refs/heads/evil".to_owned(),
        "refs/tags/evil".to_owned(),
        remote(OTHER).ref_name(AgentRef::Head),
    ] {
        let before = try_ref(head, &target);
        let output = fixture.push(&worktree, &url, &format!("HEAD:{target}"));
        assert!(!output.status.success(), "{target}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("deny updating a hidden ref"),
            "{target}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(try_ref(head, &target), before, "{target} changed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sessions_for_another_leases_agent_are_refused() {
    let fixture = Fixture::start().await;
    let error = tokio::time::timeout(
        wait(),
        fixture
            .cache
            .checkout(REPO, &remote(OTHER), &fixture.worktree("b")),
    )
    .await
    .unwrap()
    .unwrap_err()
    .to_string();
    assert!(error.contains("node-git-denied"), "{error}");
    assert!(!fixture.worktree("b").exists());

    // Pushing to the other agent's remote is refused before git runs.
    let worktree = fixture.worktree("a");
    tokio::time::timeout(
        wait(),
        fixture.cache.checkout(REPO, &remote(AGENT), &worktree),
    )
    .await
    .unwrap()
    .unwrap();
    let target = remote(OTHER).ref_name(AgentRef::Head);
    let output = fixture.push(&worktree, &remote(OTHER).url(), &format!("HEAD:{target}"));
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("node-git-denied"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(try_ref(fixture.head.path(), &target), None);
}
