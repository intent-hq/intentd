use super::*;
fn values() -> Fields<'static> {
    Fields {
        class: 3,
        state: 3,
        reason: 0,
        flags: 127,
        principal: "\u{feff}p\0😀",
        root: "r",
        workspace: "\u{feff}w",
        digest: [0xab; 32],
        epoch: [2; 16],
        accept_until: i128::MIN,
        source_expiry: i128::MAX,
        sequence: u64::MAX,
        outstanding: 0,
    }
}
#[test]
fn exact_layout_bom_and_fixed_integer_roundtrip() {
    let v = values();
    let m = Metadata::new(v).unwrap();
    assert_eq!(m.fields().unwrap(), v);
    assert_eq!(&m.bytes()[..8], &[1, 3, 3, 0, 0, 0, 0, 127]);
    let end = FIXED + v.principal.len() + v.root.len() + v.workspace.len();
    assert!(m.bytes()[end..].iter().all(|b| *b == 0));
    let mut no_bom = v;
    no_bom.principal = "p\0😀";
    assert_ne!(Metadata::new(no_bom).unwrap().bytes(), m.bytes());
}
#[test]
fn exact_capacity_and_cleanup_fit_preserve_actual_identity() {
    let root = "r".repeat(36);
    let p = "x".repeat(3692);
    assert!(context_fits(&p, &root));
    assert!(!context_fits(&(p.clone() + "x"), &root));
    let w = "w".repeat(256);
    let mut v = values();
    v.principal = &p;
    v.root = &root;
    v.workspace = &w;
    let m = Metadata::new(v).unwrap();
    assert_eq!(m.fields().unwrap(), v);
    let over = p.clone() + "x";
    v.principal = &over;
    assert!(Metadata::new(v).is_err());
}
#[test]
fn malformed_utf8_absence_tail_and_lengths_fail_closed() {
    let m = Metadata::new(values()).unwrap();
    for at in [0, 1, 2, 4, 12, 4095] {
        let mut b = *m.bytes();
        b[at] = 255;
        assert!(decode(&b).is_err(), "{at}");
    }
    let mut b = *m.bytes();
    b[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode(&b).is_err());
    let mut v = values();
    v.flags = 0;
    assert!(Metadata::new(v).is_err());
}
#[test]
fn publication_rejects_extra_source_and_uncertain_without_mutation() {
    let mut m = Metadata::new(values()).unwrap();
    let mut r = Receipt::default();
    let before = *m.bytes();
    for field in ["source", "unexpected"] {
        let mut result = serde_json::json!({"kind":"sourceSessionSettled","operationId":hex(&values().digest),"reason":"closed"});
        result[field] = serde_json::json!("must not persist");
        assert!(r.publish(&mut m, result).is_err());
        assert_eq!(m.bytes(), &before);
        assert_eq!(r.bytes(), &[0; CELL]);
    }
    m.progress(4, 0, 127, 0, 0).unwrap();
    assert!(r.publish(&mut m,serde_json::json!({"kind":"sourceSessionSettled","operationId":hex(&values().digest),"reason":"closed"})).is_err());
    assert_eq!(m.fields().unwrap().state, 4);
}
#[test]
fn settled_receipt_uses_current_id_and_never_rewrites() {
    let mut m = Metadata::new(values()).unwrap();
    let mut r = Receipt::default();
    let dto = serde_json::json!({"kind":"sourceSessionSettled","operationId":hex(&values().digest),"reason":"closed"});
    r.publish(&mut m, dto.clone()).unwrap();
    assert_eq!(m.fields().unwrap().state, 5);
    let id = Value::String("\0".repeat(64));
    let f: Value = serde_json::from_str(&r.frame(&id).unwrap()).unwrap();
    assert_eq!(f["id"], id);
    assert_eq!(f["result"], dto);
    assert!(r.publish(&mut m, dto).is_err());
}
#[test]
fn receipt_prefix_is_storage_not_wire_and_bom_is_not_stripped() {
    let raw = format!("\"{}\"", "x".repeat(RECEIPT_PAYLOAD - 2));
    let mut r = Receipt::default();
    r.bytes[..4].copy_from_slice(&3677_u32.to_be_bytes());
    r.bytes[4..4 + raw.len()].copy_from_slice(raw.as_bytes());
    assert_eq!(
        r.frame(&Value::String("\0".repeat(64))).unwrap().len(),
        4096
    );
    let mut malformed = Receipt::default();
    let raw = "\u{feff}{}";
    malformed.bytes[..4].copy_from_slice(&u32::try_from(raw.len()).unwrap().to_be_bytes());
    malformed.bytes[4..4 + raw.len()].copy_from_slice(raw.as_bytes());
    assert!(malformed.payload().is_err());
}

#[test]
fn generic_progress_cannot_reopen_uncertain_or_terminal_history() {
    for state in [4, 5, 6] {
        let mut v = values();
        v.state = state;
        let mut metadata = Metadata::new(v).unwrap();
        let before = *metadata.bytes();
        let receipt = Receipt::default();
        assert!(
            metadata.progress(3, 0, 127, 0, 0).is_err(),
            "state {state} reopened"
        );
        assert_eq!(metadata.bytes(), &before);
        assert_eq!(receipt.bytes(), &[0; CELL]);
    }
}

#[test]
fn terminal_reason_cannot_change_cancellation_or_invent_prevention() {
    for (state, reason) in [(6, "closed"), (6, "expired"), (3, "cancelled")] {
        let mut fields = values();
        fields.state = state;
        let mut metadata = Metadata::new(fields).unwrap();
        let mut receipt = Receipt::default();
        let before = *metadata.bytes();
        let result = serde_json::json!({"kind":"sourceSessionSettled","operationId":hex(&fields.digest),"reason":reason});
        assert!(
            receipt.publish(&mut metadata, result).is_err(),
            "state {state} reason {reason}"
        );
        assert_eq!(metadata.bytes(), &before);
        assert_eq!(receipt.bytes(), &[0; CELL]);
    }
}

#[test]
fn zero_count_never_refunds_pending_read_write_or_consumer_ownership() {
    for (state, reason) in [(3, "closed"), (3, "expired"), (6, "cancelled")] {
        for held in [128, 256, 512] {
            let mut fields = values();
            fields.state = state;
            fields.flags |= held;
            fields.outstanding = 0;
            let mut metadata = Metadata::new(fields).unwrap();
            let before = *metadata.bytes();
            let mut receipt = Receipt::default();
            let result = serde_json::json!({"kind":"sourceSessionSettled","operationId":hex(&fields.digest),"reason":reason});
            assert!(
                receipt.publish(&mut metadata, result).is_err(),
                "state {state} held {held}"
            );
            assert_eq!(metadata.bytes(), &before);
            assert_eq!(receipt.bytes(), &[0; CELL]);
        }
    }
}
