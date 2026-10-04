use super::*;
use serde_json::json;
fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/source_sessions_prepared.json"
    ))
    .unwrap()
}
#[test]
fn frozen_f725_descriptor_identity_is_not_authority() {
    let f = fixture();
    let op: Operation = serde_json::from_value(
        json!({"descriptor":f["descriptor"],"operationId":f["operationId"]}),
    )
    .unwrap();
    op.validate().unwrap();
    let mut changed = op.clone();
    changed.descriptor.accept_until = "2026-10-04T09:35:00.000000001Z".into();
    assert_eq!(
        instant(&changed.descriptor.accept_until),
        instant(&op.descriptor.accept_until)
    );
    assert_eq!(changed.validate(), Err(SessionError::Identity));
}
#[test]
fn exact_raw_field_sets_duplicates_ids_and_size() {
    let f = fixture();
    let raw = f["open"].to_string();
    wire::parse(&raw).unwrap();
    let mut changed = f["open"].clone();
    changed["params"]["principal"] = json!("forged");
    assert!(wire::parse(&changed.to_string()).is_err());
    assert!(wire::parse(&raw.replacen("\"nonce\":", "\"nonce\":\"bad\",\"nonce\":", 1)).is_err());
    for id in [
        json!(-1),
        json!("\0".repeat(64)),
        json!(9_007_199_254_740_991_i64),
    ] {
        changed = f["open"].clone();
        changed["id"] = id;
        wire::parse(&changed.to_string()).unwrap();
    }
    for id in [
        Value::Null,
        json!("x".repeat(65)),
        json!(9_007_199_254_740_992_i64),
    ] {
        assert!(validate_id(&id).is_err());
    }
    let padded = format!("{raw}{}", " ".repeat(REQUEST_BYTES - raw.len()));
    wire::parse(&padded).unwrap();
    assert_eq!(
        wire::parse(&(padded + " ")).unwrap_err(),
        SessionError::Budget
    );
}
#[test]
fn raw_total_depth_and_bom_identity_are_not_normalized() {
    let d = |n: usize| format!("{}0{}", "[".repeat(n), "]".repeat(n));
    assert!(wire::strict(&d(32), REQUEST_BYTES).is_ok());
    assert!(wire::strict(&d(33), REQUEST_BYTES).is_err());
    assert_eq!(
        wire::strict("\"\\ufeffprincipal\"", 4096).unwrap(),
        json!("\u{feff}principal")
    );
    assert!(wire::strict("{\"x\":1,\"\\u0078\":2}", 4096).is_err());
}
#[test]
fn read_shape_cannot_smuggle_other_snapshot_fields_or_null_refs() {
    let valid = json!({"kind":"context","contextRef":"signed","maxItems":1,"maxWireBytes":4096});
    let r: PageRequest = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(
        r.into_page().unwrap().context_ref.as_deref(),
        Some("signed")
    );
    for (key, value) in [
        ("ref", json!("other")),
        ("cursor", Value::Null),
        ("snapshotId", json!("other")),
        ("maxSourceBytes", json!(1)),
    ] {
        let mut bad = valid.clone();
        bad[key] = value;
        assert!(serde_json::from_value::<PageRequest>(bad).is_err(), "{key}");
    }
    assert!(serde_json::from_value::<PageRequest>(
        json!({"kind":"metadata","ref":"x","cursor":null,"maxItems":1,"maxWireBytes":4096})
    )
    .is_err());
    for n in [4095, 8193] {
        let mut bad = valid.clone();
        bad["maxWireBytes"] = json!(n);
        assert!(serde_json::from_value::<PageRequest>(bad)
            .unwrap()
            .into_page()
            .is_err());
    }
}
#[test]
fn terminal_and_error_envelopes_are_exact_and_source_free() {
    let id = json!("\0".repeat(64));
    let c = Control::Settled {
        operation_id: "a".repeat(64),
        reason: Reason::Closed,
    };
    let raw = wire::control(&c, &id).unwrap();
    let mut result = serde_json::from_str::<Value>(&raw).unwrap()["result"].clone();
    result["source"] = json!("secret");
    assert!(serde_json::from_value::<Control>(result).is_err());
    for e in [
        SessionError::Identity,
        SessionError::Capacity,
        SessionError::Uncertain,
    ] {
        let v: Value = serde_json::from_str(&wire::error(e, &id).unwrap()).unwrap();
        assert_eq!(v["error"]["data"], json!({"code":e.code()}));
        assert_eq!(v["id"], id);
    }
}

#[test]
fn retained_f725_integral_numeric_spellings() {
    let corpus: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/source_session_numeric_spellings.json"
    ))
    .unwrap();
    let mut differences = Vec::new();
    for vector in corpus["results"].as_array().unwrap() {
        let raw = vector["raw"].as_str().unwrap();
        let accepted = if vector["field"] == "id" {
            let raw = wire::strict(raw, REQUEST_BYTES).unwrap();
            validate_id(&raw["id"]).is_ok()
        } else {
            wire::parse(raw).is_ok()
        };
        if accepted != vector["accepted"].as_bool().unwrap() {
            differences.push(format!(
                "{}={} accepted={accepted}",
                vector["field"], vector["spelling"]
            ));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}
