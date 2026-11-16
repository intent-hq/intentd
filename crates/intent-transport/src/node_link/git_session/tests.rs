use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::*;

/// A session whose connection's outbound queue is already full.
fn session_on_full_queue() -> (GitSession, GitSender, mpsc::Receiver<Vec<u8>>) {
    let (out, rx) = mpsc::channel(1);
    let (close, _) = watch::channel(false);
    let sender = GitSender { out, close };
    assert!(sender.try_send(vec![0]).is_ok());
    let session = GitSession {
        id: 7,
        epoch: 0,
        sender: sender.clone(),
        events: None,
        state: Arc::new(Mutex::new(NodeState::default())),
        closed: false,
        warning: None,
        _slot: None,
    };
    (session, sender, rx)
}

#[tokio::test]
async fn dropping_a_session_on_a_full_queue_still_cancels_it() {
    let (session, sender, mut rx) = session_on_full_queue();
    drop(session);
    let wait = intentd_test_support::test_timeout(Duration::from_secs(5));
    assert_eq!(rx.recv().await, Some(vec![0]));
    let close = tokio::time::timeout(wait, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        decode(close).map(|(kind, id, _)| (kind, id)),
        Some((CLOSE, 7))
    );
    assert!(!*sender.close.borrow());
}

#[test]
fn dropping_a_session_on_a_full_queue_without_a_runtime_closes_the_connection() {
    let (session, sender, _rx) = session_on_full_queue();
    drop(session);
    assert!(*sender.close.borrow());
}

/// A sub-agent's own socket opens only the forge remotes of its launch:
/// another key, another agent's forge remote or any other remote is
/// refused before a session opens.
#[cfg(unix)]
#[tokio::test]
async fn a_forge_socket_refuses_remotes_outside_its_launch() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let own = AgentRemote::new("ws-1", "agent-1").unwrap();
    let other = AgentRemote::new("ws-1", "agent-2").unwrap();
    let allowed = vec![own.forge(), own.forge_module("k1").unwrap()];
    let server = tokio::spawn(serve_forge_helpers(
        listener,
        NodeGitSessions::default(),
        allowed,
    ));
    let ask = |request: String| {
        let path = path.clone();
        async move {
            let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).await.unwrap();
            String::from_utf8_lossy(&reply).into_owned()
        }
    };
    for (request, code) in [
        (own.forge_module("k2").unwrap().url(), codes::DENIED),
        (other.forge().url(), codes::DENIED),
        (other.forge_module("k1").unwrap().url(), codes::DENIED),
        (own.with_module("k1").unwrap().url(), codes::INVALID),
        (own.url(), codes::INVALID),
    ] {
        let reply = ask(format!("git-upload-pack {request}\n")).await;
        assert_eq!(reply, format!("\x01{code}\n"), "{request}");
    }
    let reply = ask("credential github.com\n".into()).await;
    assert_eq!(reply, format!("\x01{}\n", codes::INVALID));
    server.abort();
}
