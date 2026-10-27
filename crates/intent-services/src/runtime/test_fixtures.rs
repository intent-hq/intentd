//! Fixtures the runtime's tests share. These three mirror helpers in
//! core's `test_support` that stay `cfg(test)` there: their hang guards go
//! through the dev-only `intentd_test_support::test_timeout`, which core's
//! `test-hooks` build cannot use.

use std::time::Duration;

use intent_core::AgentId;
use serde_json::Value;

use crate::{Services, Subscription};

/// All persisted message texts for an agent (each message's text blocks
/// joined), oldest-first — the wake-delivery capture used by the goldens.
/// Wake persistence can lag the op return, so poll until at least `expected`
/// messages are present (generous deadline, monorepo#1358 precedent); on
/// timeout return whatever was seen and let the caller's assert report it.
pub(crate) async fn wake_texts_when(svc: &Services, id: &AgentId, expected: usize) -> Vec<String> {
    let deadline = std::time::Instant::now()
        + intentd_test_support::test_timeout(std::time::Duration::from_secs(10));
    loop {
        let session = svc.store().get_agent_session(id).await.expect("session");
        let texts: Vec<String> = session
            .messages
            .iter()
            .map(|m| {
                m.content
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            })
            .collect();
        if texts.len() >= expected || std::time::Instant::now() >= deadline {
            return texts;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Receive one setup event (generous timeout: script paths cross a PTY
/// spawn + reap poll), serialized to wire JSON.
pub(crate) async fn recv_setup(sub: &mut Subscription) -> Value {
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        sub.recv(),
    )
    .await
    .expect("setup event delivered")
    .expect("subscription open");
    assert_eq!(batch.len(), 1, "expected exactly one event per delivery");
    serde_json::to_value(&batch[0]).expect("serialize event")
}

pub(crate) async fn assert_quiet(sub: &mut Subscription) {
    let quiet = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(300)),
        sub.recv(),
    )
    .await;
    assert!(quiet.is_err(), "no further setup events, got: {quiet:?}");
}
