//! The node end to end over a real link: head (the test) dials with the
//! certificate pin and lease token, binds, launches, reads the journal
//! stream, acknowledges and re-binds.

use std::time::Duration;

use intent_core::node_link::{
    JournalBody, JournalRecord, LaunchCommand, LeaseBinding, LinkFrame, LinkHello,
    NODE_LINK_PROTOCOL,
};
use intent_core::nodes::NodeCounter;
use intent_core::{AgentId, WorkspaceId};
use intent_transport::node_link::{
    dial_link, CertificatePin, DialedLink, LeaseToken, LinkEvent, NodeEndpoint,
};

use super::config::Permission;
use super::*;

const NODE_ID: &str = "node-1";

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(10))
}

fn token() -> String {
    "k".repeat(48)
}

fn config(max_agents: u32) -> Config {
    Config {
        node_id: NODE_ID.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        token: LeaseToken::new(token()).unwrap(),
        max_agents,
        permission: Permission::default(),
        providers: BTreeMap::from([(
            "mock".to_owned(),
            ProviderEntry {
                binary: std::env::current_exe().unwrap(),
                args: None,
            },
        )]),
    }
}

struct Running {
    endpoint: NodeEndpoint,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Running {
    async fn start(root: &std::path::Path, max_agents: u32) -> Self {
        Self::start_with(root, &config(max_agents)).await
    }

    async fn start_with(root: &std::path::Path, config: &Config) -> Self {
        // Agents work in plain directories here: no head serves git. The
        // git checkout path is covered by the e2e_remote_launch tests.
        let node = Node::start_with_checkouts(config, root, |storage, _cache| {
            crate::node::checkout::PlainCheckouts::new(storage)
        })
        .await
        .unwrap();
        let endpoint = NodeEndpoint {
            url: format!("wss://{}", node.listener.local_addr()),
            pin: CertificatePin::parse(&node.certificate_sha256).unwrap(),
            token: LeaseToken::new(token()).unwrap(),
        };
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(node.serve(async move {
            let _ = stopped.await;
            Ok(())
        }));
        Self {
            endpoint,
            stop: Some(stop),
            task,
        }
    }

    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.await.unwrap().unwrap();
    }
}

fn hello(generation: u64, lease: &str, acked: u64) -> LinkHello {
    LinkHello {
        protocol: NODE_LINK_PROTOCOL,
        intentd_version: "test".into(),
        binding: LeaseBinding {
            head_id: "head-1".into(),
            node_id: NODE_ID.into(),
            lease_id: lease.into(),
            generation: NodeCounter(generation),
        },
        acked_seq: NodeCounter(acked),
    }
}

/// A launch the node refuses without spawning anything (no such provider
/// configured), which journals exactly one exit record.
fn refused_launch(id: &str) -> LinkFrame {
    LinkFrame::Launch(LaunchCommand {
        agent_id: AgentId::from(id),
        workspace_id: WorkspaceId::from("ws-1"),
        provider: "codex".into(),
        model: None,
        base_commit: "base".into(),
        repo_key: "repo".into(),
        profile_env: None,
    })
}

async fn next_frame(dialed: &mut DialedLink) -> Option<LinkFrame> {
    loop {
        match tokio::time::timeout(wait(), dialed.events.recv())
            .await
            .expect("timed out waiting for a frame")
        {
            Some(LinkEvent::Frame(frame)) => return Some(frame),
            Some(LinkEvent::Request(_)) => {}
            None => return None,
        }
    }
}

async fn next_record(dialed: &mut DialedLink) -> JournalRecord {
    match next_frame(dialed).await {
        Some(LinkFrame::Journal(record)) => record,
        other => panic!("expected a journal record, got {other:?}"),
    }
}

async fn replay_complete(dialed: &mut DialedLink) -> NodeCounter {
    match next_frame(dialed).await {
        Some(LinkFrame::ReplayComplete { through_seq }) => through_seq,
        other => panic!("expected replay complete, got {other:?}"),
    }
}

#[tokio::test]
async fn node_binds_reports_capacity_and_streams_launch_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(&dir.path().join("node"), 3).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(dialed.welcome.node_id, NODE_ID);
    assert_eq!(dialed.welcome.max_agents, 3);
    assert_eq!(dialed.welcome.platform, host_platform().unwrap());
    assert!(dialed.welcome.running_agents.is_empty());
    assert_eq!(replay_complete(&mut dialed).await, NodeCounter(0));

    dialed.link.send(&refused_launch("a1")).await.unwrap();
    let record = next_record(&mut dialed).await;
    assert_eq!(record.seq, NodeCounter(1));
    assert_eq!(record.agent_id, AgentId::from("a1"));
    assert!(
        matches!(&record.body, JournalBody::Exited { error: Some(e), .. }
            if e.starts_with(runner::codes::PROVIDER)),
        "{record:?}"
    );
    node.stop().await;
}

#[tokio::test]
async fn node_replays_unacknowledged_records_after_rebind_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("node");
    let node = Running::start(&root, 2).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    for id in ["a1", "a2", "a3"] {
        first.link.send(&refused_launch(id)).await.unwrap();
    }
    for seq in 1..=3 {
        assert_eq!(next_record(&mut first).await.seq, NodeCounter(seq));
    }
    first
        .link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(1),
        })
        .await
        .unwrap();

    // A newer generation replaces the link; head says it ingested 2.
    let mut second = dial_link(&node.endpoint, hello(2, "lease-1", 2))
        .await
        .unwrap();
    assert!(next_frame(&mut first).await.is_none(), "older link closed");
    assert_eq!(next_record(&mut second).await.seq, NodeCounter(3));
    assert_eq!(replay_complete(&mut second).await, NodeCounter(3));
    // An older generation can no longer bind.
    assert!(dial_link(&node.endpoint, hello(2, "lease-1", 0))
        .await
        .is_err());
    node.stop().await;

    // The unacknowledged record survives a node restart.
    let node = Running::start(&root, 2).await;
    let mut third = dial_link(&node.endpoint, hello(3, "lease-1", 2))
        .await
        .unwrap();
    let replayed = next_record(&mut third).await;
    assert_eq!(replayed.seq, NodeCounter(3));
    assert_eq!(replayed.agent_id, AgentId::from("a3"));
    assert_eq!(replay_complete(&mut third).await, NodeCounter(3));
    third
        .link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(3),
        })
        .await
        .unwrap();
    // New records continue the sequence.
    third.link.send(&refused_launch("a4")).await.unwrap();
    assert_eq!(next_record(&mut third).await.seq, NodeCounter(4));
    node.stop().await;
}

#[tokio::test]
async fn node_refuses_a_link_that_acknowledges_unwritten_records() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(&dir.path().join("node"), 2).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 7))
        .await
        .unwrap();
    assert!(next_frame(&mut dialed).await.is_none());

    // A good link still works afterwards.
    let mut dialed = dial_link(&node.endpoint, hello(2, "lease-1", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut dialed).await, NodeCounter(0));
    // So does a bad acknowledgement on a live link: it is closed.
    dialed
        .link
        .send(&LinkFrame::Ack {
            seq: NodeCounter(9),
        })
        .await
        .unwrap();
    assert!(next_frame(&mut dialed).await.is_none());
    node.stop().await;
}

#[tokio::test]
async fn node_restarts_the_sequence_for_a_new_lease() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(&dir.path().join("node"), 2).await;
    let mut first = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut first).await;
    first.link.send(&refused_launch("a1")).await.unwrap();
    assert_eq!(next_record(&mut first).await.seq, NodeCounter(1));

    let mut second = dial_link(&node.endpoint, hello(2, "lease-2", 0))
        .await
        .unwrap();
    assert_eq!(replay_complete(&mut second).await, NodeCounter(0));
    second.link.send(&refused_launch("a2")).await.unwrap();
    let record = next_record(&mut second).await;
    assert_eq!(
        (record.seq, record.agent_id),
        (NodeCounter(1), AgentId::from("a2"))
    );
    node.stop().await;
}

#[tokio::test]
async fn node_answers_head_requests_as_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let node = Running::start(&dir.path().join("node"), 1).await;
    let dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    let outcome = dialed
        .link
        .call(
            AgentId::from("a1"),
            "workspace.read",
            serde_json::json!({}),
            wait(),
        )
        .await
        .unwrap();
    assert!(
        matches!(&outcome, intent_core::node_link::LinkOutcome::Err(e) if e.code == -32601),
        "{outcome:?}"
    );
    node.stop().await;
}

/// Launches that keep arriving on a live link while the node shuts down
/// are either stopped by that shutdown or refused: every agent that left a
/// record has its exit journaled, and no provider process survives.
#[tokio::test]
async fn node_shutdown_settles_launches_arriving_on_a_live_link() {
    let Some(node_bin) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping: node not on PATH");
        return;
    };
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut config = config(8);
    config.providers = BTreeMap::from([(
        "mock".to_owned(),
        ProviderEntry {
            binary: node_bin,
            args: Some(vec![fixture]),
        },
    )]);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("node");
    let mut node = Running::start_with(&root, &config).await;
    let mut dialed = dial_link(&node.endpoint, hello(1, "lease-1", 0))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    let launch = |id: String| {
        LinkFrame::Launch(LaunchCommand {
            agent_id: AgentId::from(id),
            workspace_id: WorkspaceId::from("ws-1"),
            provider: "mock".into(),
            model: None,
            base_commit: "base".into(),
            repo_key: "repo".into(),
            profile_env: None,
        })
    };
    dialed.link.send(&launch("a0".into())).await.unwrap();
    dialed
        .link
        .send(&LinkFrame::Prompt {
            agent_id: AgentId::from("a0"),
            turn_id: "t1".into(),
            prompt: vec![serde_json::json!({ "type": "text", "text": "many" })],
        })
        .await
        .unwrap();
    // Shut down while a0's burst is being journaled (after its Started).
    let mut first = next_record(&mut dialed).await;
    if matches!(first.body, JournalBody::Started { .. }) {
        first = next_record(&mut dialed).await;
    }
    assert!(matches!(first.body, JournalBody::Notification { .. }));
    let _ = node.stop.take().unwrap().send(());
    for i in 1..=20 {
        if dialed.link.send(&launch(format!("late{i}"))).await.is_err() {
            break;
        }
    }
    node.task.await.unwrap().unwrap();

    let journal = Journal::open(&root.join("journal")).unwrap();
    let records = journal.records_after(NodeCounter(0)).unwrap();
    let mut agents: Vec<&str> = records.iter().map(|r| r.agent_id.0.as_str()).collect();
    agents.sort_unstable();
    agents.dedup();
    assert!(agents.contains(&"a0"), "{records:#?}");
    for agent in agents {
        let exits = records
            .iter()
            .filter(|r| r.agent_id.0 == agent && matches!(r.body, JournalBody::Exited { .. }))
            .count();
        assert_eq!(exits, 1, "{agent}: {records:#?}");
    }
    for entry in std::fs::read_dir(root.join("agents")).unwrap() {
        let pid_file = entry.unwrap().path().join("work").join("agent.pid");
        let Ok(bytes) = std::fs::read(&pid_file) else {
            continue;
        };
        let pids: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for key in ["agent", "child"] {
            let pid = i32::try_from(pids[key].as_i64().unwrap()).unwrap();
            let deadline = tokio::time::Instant::now() + wait();
            while !process_gone(pid) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{} {key} {pid} survived shutdown",
                    pid_file.display()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

/// Whether `pid` has exited (a zombie awaiting its reaper counts).
fn process_gone(pid: i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            Ok(stat) => stat
                .rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z')),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err()
    }
}

#[test]
fn node_platform_is_linux_or_macos_on_x86_64_or_aarch64() {
    assert_eq!(
        platform_of("linux", "x86_64").unwrap(),
        AgentPlatform {
            os: NodeOs::Linux,
            arch: NodeArch::X86_64
        }
    );
    assert_eq!(
        platform_of("macos", "aarch64").unwrap(),
        AgentPlatform {
            os: NodeOs::Macos,
            arch: NodeArch::Aarch64
        }
    );
    assert!(platform_of("windows", "x86_64").is_err());
    assert!(platform_of("linux", "riscv64").is_err());
}

#[test]
fn provider_args_override_replaces_only_launch_arguments() {
    let binary = std::env::current_exe().unwrap();
    let launches = provider_launches(&BTreeMap::from([
        (
            "mock".to_owned(),
            ProviderEntry {
                binary: binary.clone(),
                args: Some(vec!["/opt/agent.mjs".into(), "--flag".into()]),
            },
        ),
        (
            "claude-code".to_owned(),
            ProviderEntry {
                binary: binary.clone(),
                args: None,
            },
        ),
    ]));
    let mock = &launches["mock"];
    assert_eq!(mock.config.base_args, &["/opt/agent.mjs", "--flag"]);
    assert_eq!(mock.config.id, "mock");
    assert_eq!(mock.binary, binary);
    let claude = &launches["claude-code"];
    assert_eq!(
        claude.config.base_args,
        intent_providers::find_provider("claude-code")
            .unwrap()
            .base_args
    );
}
