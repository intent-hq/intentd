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
