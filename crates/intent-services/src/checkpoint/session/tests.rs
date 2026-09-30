use super::*;
use std::fs;

const FIXTURE: CheckpointPolicy = CheckpointPolicy {
    session_files: &["sessions/conversation.json"],
};

#[test]
fn checkpoint_session_fixture_roundtrip_excludes_auth_and_spawn_secrets() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("provider");
    fs::create_dir_all(root.join("sessions")).unwrap();
    fs::write(
        root.join("sessions/conversation.json"),
        br#"{"messages":["hello"],"portable":true}"#,
    )
    .unwrap();
    for name in [
        "auth.json",
        "credentials.json",
        "spawn-secret",
        "sessions/token.json",
    ] {
        fs::write(root.join(name), b"SECRET_FIXTURE").unwrap();
    }
    let bundle = capture_policy("fixture", &root, "42", Some("42"), true, FIXTURE).unwrap();
    assert_eq!(bundle.session.mode, Mode::Load);
    assert_eq!(bundle.payloads.len(), 1);
    assert!(!bundle.payloads[0]
        .1
        .windows(14)
        .any(|w| w == b"SECRET_FIXTURE"));
    let dst = temp.path().join("restore");
    restore_policy(&bundle, "42", &dst, FIXTURE).unwrap();
    assert_eq!(
        fs::read(dst.join("sessions/conversation.json")).unwrap(),
        bundle.payloads[0].1
    );
    assert!(!dst.join("auth.json").exists());
    assert!(!dst.join("sessions/token.json").exists());
    // No shipped provider inherits the fixture's portable-session permission.
    for id in intent_providers::all_provider_ids()
        .into_iter()
        .chain(["unknown"])
    {
        let fallback = capture(id, &root, "42", Some("42"), true).unwrap();
        assert_eq!(fallback.session.mode, Mode::History);
        assert!(fallback.payloads.is_empty() && fallback.session.files.is_empty());
    }
}

#[test]
fn checkpoint_session_inconsistent_cut_uses_history_without_reading_files() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("does-not-exist");
    for (watermark, load) in [(None, true), (Some("41"), true), (Some("42"), false)] {
        let bundle = capture_policy("fixture", &missing, "42", watermark, load, FIXTURE).unwrap();
        assert_eq!(bundle.session.mode, Mode::History);
        assert_eq!(bundle.session.through_seq, "42");
        assert!(bundle.payloads.is_empty());
    }
}

#[test]
fn checkpoint_session_partial_or_corrupt_files_fail_before_restore() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("sessions")).unwrap();
    assert!(capture_policy("fixture", temp.path(), "42", Some("42"), true, FIXTURE).is_err());
    fs::write(temp.path().join("sessions/conversation.json"), b"valid").unwrap();
    let mut bundle =
        capture_policy("fixture", temp.path(), "42", Some("42"), true, FIXTURE).unwrap();
    bundle.payloads[0].1.push(0);
    let dst = temp.path().join("restore");
    assert!(restore_policy(&bundle, "42", &dst, FIXTURE).is_err());
    assert!(!dst.exists());
    let unsafe_policy = CheckpointPolicy {
        session_files: &["auth.json"],
    };
    fs::write(temp.path().join("auth.json"), b"SECRET_FIXTURE").unwrap();
    assert!(capture_policy(
        "fixture",
        temp.path(),
        "42",
        Some("42"),
        true,
        unsafe_policy
    )
    .is_err());
}

#[cfg(unix)]
#[test]
fn checkpoint_session_symlink_escape_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let secret = temp.path().join("secret");
    fs::write(&secret, b"SECRET_FIXTURE").unwrap();
    fs::create_dir(temp.path().join("sessions")).unwrap();
    std::os::unix::fs::symlink(&secret, temp.path().join("sessions/conversation.json")).unwrap();
    assert!(capture_policy("fixture", temp.path(), "42", Some("42"), true, FIXTURE).is_err());
}

#[test]
fn checkpoint_history_replay_is_bounded_quoted_context() {
    use intent_core::AgentId;
    use serde_json::json;
    let messages: Vec<_> = (0..100).map(|seq| AgentMessage {
        id: format!("m-{seq}"), agent_id: AgentId::from("agent-1"), seq,
        role: if seq % 2 == 0 { "user" } else { "assistant" }.into(),
        content: json!([{"type":"text","text":format!("{} {}", seq, "x".repeat(8000))},
            {"type":"tool_use","id":format!("old-{seq}"),"name":"shell","input":{"command":"<do-not-execute>"}},
            {"type":"tool_result","tool_use_id":format!("old-{seq}"),"output":"already completed"}]),
        metadata: None, app_message_id: None, author: None, created_at: "2026-09-28T09:00:00Z".into(),
    }).collect();
    let replay = replay_history(&messages, 100);
    assert!(replay.len() <= crate::history_xml::MAX_HISTORY_CHARS);
    assert!(replay.contains("&lt;do-not-execute&gt;"));
    assert!(replay.contains("earlier exchanges omitted"));
}
