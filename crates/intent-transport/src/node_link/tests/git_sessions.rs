//! Git sessions on a real git connection. Git-level confinement and the
//! helper are covered by intent-git's `node_git` tests and intentd's
//! `e2e_node_git`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use intent_core::{BoxFuture, Error, Result};
use intent_git::node_git::{AgentRemote, GitService};

use super::*;

struct AllowAgent {
    repository: PathBuf,
}

impl NodeGitAccess for AllowAgent {
    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            binding.ensure_current(&super::binding(1))?;
            if remote.agent_id() == "agent-1" {
                Ok(self.repository.clone())
            } else {
                Err(Error::Forbidden("not on this lease".into()))
            }
        })
    }
}

fn repository(dir: &Path) -> PathBuf {
    let path = dir.join("head");
    let status = Command::new("git")
        .args(["init", "--quiet"])
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());
    path
}

/// A node with head serving git sessions for `agent-1`, and the node's
/// sessions attached to the git connection.
async fn connected() -> (
    Node,
    DialedLink,
    NodeGitSessions,
    tokio::task::JoinHandle<()>,
) {
    let mut node = Node::start(token("t")).await;
    let link = dial(&node, 1).await;
    let _ = node.accept_link().await;
    let head_git = dial_git(&node.endpoint(), &binding(1)).await.unwrap();
    let node_git = node.accept_git().await;
    let access = AllowAgent {
        repository: repository(node.dir.path()),
    };
    let serving = tokio::spawn(serve_git(head_git, Arc::new(access)));
    let sessions = NodeGitSessions::new();
    sessions.attach(node_git);
    (node, link, sessions, serving)
}

fn remote(agent: &str) -> AgentRemote {
    AgentRemote::new("ws-1", agent).unwrap()
}

fn session_code(error: &Error) -> String {
    error.to_string()
}

#[tokio::test]
async fn open_without_a_connection_is_unavailable() {
    let sessions = NodeGitSessions::new();
    let error = sessions
        .open(GitService::UploadPack, &remote("agent-1"))
        .await
        .unwrap_err();
    assert!(
        session_code(&error).contains(git_codes::UNAVAILABLE),
        "{error}"
    );
}

#[tokio::test]
async fn upload_pack_session_streams_and_finishes() {
    let (_node, _link, sessions, _serving) = connected().await;
    let mut session = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap();
    // An empty repository advertises nothing; a flush ends the session.
    session.send(b"0000").await.unwrap();
    session.finish().await.unwrap();
    let mut output = Vec::new();
    while let Some(bytes) = tokio::time::timeout(wait(), session.recv())
        .await
        .unwrap()
        .unwrap()
    {
        output.extend(bytes);
    }
    assert!(
        output.ends_with(b"0000"),
        "{}",
        String::from_utf8_lossy(&output)
    );
}

#[tokio::test]
async fn sessions_for_agents_off_the_lease_are_denied() {
    let (_node, _link, sessions, _serving) = connected().await;
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::ReceivePack, &remote("agent-2")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(session_code(&error).contains(git_codes::DENIED), "{error}");
}

#[tokio::test]
async fn head_caps_concurrent_sessions() {
    let (_node, _link, sessions, _serving) = connected().await;
    let mut open = Vec::new();
    for _ in 0..MAX_SESSIONS {
        // upload-pack waits for the client after advertising, so these
        // sessions stay open.
        open.push(
            tokio::time::timeout(
                wait(),
                sessions.open(GitService::UploadPack, &remote("agent-1")),
            )
            .await
            .unwrap()
            .unwrap(),
        );
    }
    let error = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(session_code(&error).contains(git_codes::BUSY), "{error}");
    // Dropping a session frees its slot.
    drop(open.pop());
    tokio::time::timeout(wait(), async {
        loop {
            if let Ok(session) = sessions
                .open(GitService::UploadPack, &remote("agent-1"))
                .await
            {
                break session;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn link_loss_ends_open_sessions() {
    let (_node, _link, sessions, serving) = connected().await;
    let mut session = tokio::time::timeout(
        wait(),
        sessions.open(GitService::UploadPack, &remote("agent-1")),
    )
    .await
    .unwrap()
    .unwrap();
    serving.abort();
    let error = tokio::time::timeout(wait(), async {
        loop {
            match session.recv().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("the session must not end cleanly"),
                Err(error) => break error,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        session_code(&error).contains(git_codes::UNAVAILABLE),
        "{error}"
    );
    assert!(tokio::time::timeout(wait(), async {
        while sessions.is_connected() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok());
}
