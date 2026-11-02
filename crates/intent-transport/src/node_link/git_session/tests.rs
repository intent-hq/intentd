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
