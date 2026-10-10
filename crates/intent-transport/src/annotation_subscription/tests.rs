use super::*;

fn state() -> Value {
    json!({"kind":"notePageState","scope":{"backendId":"db","workspaceId":"ws","noteId":"spec","noteInstanceId":"inc"},"stateGeneration":"0","sourceRevision":"r:1","attributionGeneration":"a:1","attributionState":"pending","commentRevision":"c:1","deleted":false,"invalidation":"all"})
}

#[test]
fn page_state_admission_preserves_only_channel_specific_legacy_projections() {
    for channel in [PageStateChannel::Note, PageStateChannel::Comment] {
        assert_eq!(
            parse_page_state_subscription(channel, &json!({})).unwrap(),
            None
        );
        for projection in [
            json!(true),
            json!(1),
            json!({}),
            json!([]),
            json!("unknown"),
        ] {
            assert!(
                parse_page_state_subscription(channel, &json!({"projection":projection})).is_err()
            );
        }
    }
    for projection in [Value::Null, json!("slim"), json!("full")] {
        let params = json!({"projection":projection});
        assert_eq!(
            parse_page_state_subscription(PageStateChannel::Note, &params).unwrap(),
            None
        );
        assert!(parse_page_state_subscription(PageStateChannel::Comment, &params).is_err());
    }
    assert!(parse_page_state_subscription(PageStateChannel::Note, &Value::Null).is_err());
}

#[test]
fn page_state_admission_requires_one_bounded_note_without_unknown_options() {
    let valid = json!({"workspaceId":"ws","noteId":"spec","projection":"pageState","replaceGroup":"visible"});
    for channel in [PageStateChannel::Note, PageStateChannel::Comment] {
        assert_eq!(
            parse_page_state_subscription(channel, &valid).unwrap(),
            Some(PageStateSubscription {
                workspace_id: "ws".into(),
                note_id: "spec".into(),
                replace_group: Some("visible".into())
            })
        );
        for field in ["workspaceId", "noteId"] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(parse_page_state_subscription(channel, &missing).is_err());
            for value in [Value::Null, json!(""), json!("😀".repeat(65)), json!([])] {
                let mut bad = valid.clone();
                bad[field] = value;
                assert!(parse_page_state_subscription(channel, &bad).is_err());
            }
        }
        for (field, value) in [
            ("replaceGroup", Value::Null),
            ("maxWireBytes", json!(8192)),
            ("noteInstanceId", json!("invented")),
            ("page", json!({})),
        ] {
            let mut bad = valid.clone();
            bad[field] = value;
            assert!(parse_page_state_subscription(channel, &bad).is_err());
        }
    }
}

#[test]
fn page_state_frame_preserves_full_generation_and_exact_snapshot_envelope() {
    let mut snapshot = state();
    for generation in ["0", "9223372036854775808", "18446744073709551615"] {
        snapshot["stateGeneration"] = json!(generation);
        for deleted in [false, true] {
            snapshot["deleted"] = json!(deleted);
            let frame = build_page_state_push("sub-\"😀", MAX_SAFE_SEQUENCE, &snapshot).unwrap();
            assert!(frame.len() <= MAX_PUSH_BYTES);
            let frame: Value = serde_json::from_str(&frame).unwrap();
            assert_eq!(
                frame,
                json!({"jsonrpc":"2.0","method":"subscription.push","params":{"subscriptionId":"sub-\"😀","kind":"snapshot","seq":MAX_SAFE_SEQUENCE,"snapshot":snapshot}})
            );
        }
    }
    assert!(build_page_state_push("sub", MAX_SAFE_SEQUENCE + 1, &snapshot).is_err());
    assert!(build_page_state_push("", 0, &snapshot).is_err());
    assert!(build_page_state_push(&"x".repeat(257), 0, &snapshot).is_err());
}

#[test]
fn page_state_frame_rejects_malformed_or_unbounded_persisted_state() {
    for generation in ["", "01", "+1", "-1", "1.0", "18446744073709551616"] {
        let mut snapshot = state();
        snapshot["stateGeneration"] = json!(generation);
        assert!(build_page_state_push("sub", 0, &snapshot).is_err());
    }
    for (field, value) in [
        ("sourceRevision", json!("x".repeat(257))),
        ("attributionState", json!("unknown")),
        ("deleted", json!(0)),
        ("stateGeneration", json!(0)),
        ("invalidation", json!("ranges")),
        ("kind", json!("note")),
        ("scope", json!({})),
    ] {
        let mut snapshot = state();
        snapshot[field] = value;
        assert!(build_page_state_push("sub", 0, &snapshot).is_err());
    }
    let mut snapshot = state();
    snapshot["content"] = json!("x".repeat(100_000));
    assert!(build_page_state_push("sub", 0, &snapshot).is_err());
    snapshot = state();
    snapshot["scope"]["extra"] = json!("unknown");
    assert!(build_page_state_push("sub", 0, &snapshot).is_err());
}

#[test]
fn page_state_frame_measures_escaped_utf8_without_truncation() {
    let mut snapshot = state();
    for field in ["sourceRevision", "attributionGeneration", "commentRevision"] {
        snapshot[field] = json!("\u{0001}".repeat(256));
    }
    let original = snapshot.clone();
    assert!(build_page_state_push("sub", 0, &snapshot).is_err());
    assert_eq!(snapshot, original);
    snapshot["sourceRevision"] = json!("😀".repeat(64));
    snapshot["attributionGeneration"] = json!("a");
    snapshot["commentRevision"] = json!("c");
    let encoded = build_page_state_push("sub", 0, &snapshot).unwrap();
    assert!(encoded.len() <= MAX_PUSH_BYTES);
    assert_eq!(
        serde_json::from_str::<Value>(&encoded).unwrap()["params"]["snapshot"],
        snapshot
    );
}
