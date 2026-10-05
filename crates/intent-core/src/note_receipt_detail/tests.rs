use super::*;
use serde_json::json;
#[test]
fn receipt_detail_requests_are_strict_and_do_not_require_live_revision() {
    let value = json!({"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i",
        "page":{"kind":"mapping","operationId":"11111111-1111-4111-8111-111111111111","ref":"receipt:mapping"}});
    let parsed: NoteGetReceiptRequest = serde_json::from_value(value.clone()).unwrap();
    let q = parsed.query().unwrap();
    assert_eq!(q.max_items, 128);
    let mut bad = value.clone();
    bad["sourceRevision"] = json!("stale");
    assert!(serde_json::from_value::<NoteGetReceiptRequest>(bad).is_err());
    for (name, value) in [
        ("kind", json!("inverse")),
        ("ref", json!("x".repeat(257))),
        ("maxItems", json!(129)),
        ("maxWireBytes", json!(4095)),
    ] {
        let mut input = serde_json::json!({"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i","page":{"kind":"mapping","operationId":"11111111-1111-4111-8111-111111111111","ref":"r"}});
        input["page"][name] = value;
        assert!(serde_json::from_value::<NoteGetReceiptRequest>(input)
            .unwrap()
            .query()
            .is_err());
    }
}
#[test]
fn receipt_detail_operation_requires_digest_and_binds_budgets() {
    let value = json!({"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i",
        "operationId":"11111111-1111-4111-8111-111111111111","kind":"inverse","ref":"r","payloadDigest":"a".repeat(64)});
    assert!(
        serde_json::from_value::<NoteOperationReceiptRead>(value.clone())
            .unwrap()
            .query()
            .is_ok()
    );
    let mut bad = value.clone();
    bad.as_object_mut().unwrap().remove("payloadDigest");
    assert!(serde_json::from_value::<NoteOperationReceiptRead>(bad).is_err());
    let mut bad = value;
    bad["headerDigest"] = json!("b".repeat(64));
    assert!(serde_json::from_value::<NoteOperationReceiptRead>(bad).is_err());
}

#[test]
fn receipt_inverse_text_requires_named_text_and_omits_offset_on_continuation() {
    let value = json!({"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i",
        "operationId":"11111111-1111-4111-8111-111111111111","kind":"inverseText","ref":"r","textId":"text:0","offset":1,"payloadDigest":"a".repeat(64)});
    assert!(
        serde_json::from_value::<NoteOperationReceiptRead>(value.clone())
            .unwrap()
            .query()
            .is_ok()
    );
    let mut bad = value.clone();
    bad.as_object_mut().unwrap().remove("textId");
    assert!(serde_json::from_value::<NoteOperationReceiptRead>(bad)
        .unwrap()
        .query()
        .is_err());
    let mut bad = value;
    bad["cursor"] = json!("cursor");
    assert!(serde_json::from_value::<NoteOperationReceiptRead>(bad)
        .unwrap()
        .query()
        .is_err());
}

#[test]
fn receipt_context_selects_retained_revision_and_rejects_other_selectors() {
    let value = json!({"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i",
        "sourceRevision":"after","page":{"kind":"context","contextRef":"owner:detail:0"}});
    let request: NoteGetReceiptContextRequest = serde_json::from_value(value.clone()).unwrap();
    let query = request
        .query("11111111-1111-4111-8111-111111111111".into())
        .unwrap();
    assert!(query.context_envelope);
    assert!(!query.operation_envelope);
    assert_eq!(query.kind, ReceiptDetailKind::Detail);
    let mut bad = value.clone();
    bad["payloadDigest"] = json!("a".repeat(64));
    assert!(serde_json::from_value::<NoteGetReceiptContextRequest>(bad).is_err());
    let mut bad = value;
    bad["page"]["maxSourceBytes"] = json!(4096);
    assert!(serde_json::from_value::<NoteGetReceiptContextRequest>(bad).is_err());
}

#[test]
fn receipt_context_route_shape_never_grants_reference_authority() {
    assert!(super::is_receipt_context_reference(
        "00000000-0000-0000-0000-000000000000:inverse-detail:0"
    ));
    assert!(!super::is_receipt_context_reference("np1.source-context"));
    assert!(!super::is_receipt_context_reference(
        "na1.annotation-context"
    ));
    assert!(!super::is_receipt_context_reference("malformed:receipt"));
}
