use super::*;

const OP: &str = "11111111-1111-4111-8111-111111111111";
fn begin() -> NoteStageBegin {
    let mut request:NoteStageBegin=serde_json::from_value(json!({"backendId":"b","workspaceId":"w","noteId":"n","noteInstanceId":"i","operationId":OP,"headerDigest":"0".repeat(64),"expiresAt":"2026-10-05T12:00:00.000Z",
        "header":{"baseRevision":"rev","editorSessionId":"session","localEditSequence":9,"liveGeneration":2,"selectionGeneration":3,"action":"mutate","output":"source","selection":"ranges"}})).unwrap();
    request.header_digest = request.computed_digest().unwrap();
    request
}
fn append(stream: NoteStageStream, records: Vec<Value>) -> NoteStageAppend {
    let b = begin();
    let mut request = NoteStageAppend {
        backend_id: b.backend_id,
        workspace_id: b.workspace_id,
        note_id: b.note_id,
        note_instance_id: b.note_instance_id,
        operation_id: b.operation_id,
        header_digest: b.header_digest,
        stream,
        sequence: 0,
        previous_digest: None,
        records,
        chunk_digest: String::new(),
    };
    request.chunk_digest = request.computed_digest().unwrap();
    request
}
fn text_ref() -> Value {
    json!({"textId":"text:1","length":2,"utf8Bytes":4,"sha256":"a".repeat(64)})
}
fn splice(group: u64, ordinal: u64, start: u64, end: u64) -> Value {
    json!({"kind":"splice","localSequence":group,"ordinal":ordinal,"start":start,"end":end,"replacement":text_ref()})
}

#[test]
fn stage_begin_digest_is_method_bound_and_expiry_is_only_new_admission() {
    let b = begin();
    assert_eq!(
        b.header_digest,
        "7860b69538ccc6ccf51476d363c6092c397b35efe6b469f478301b299581f441"
    );
    assert!(b.validate().is_ok());
    let now = time::OffsetDateTime::parse(
        "2026-10-05T11:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    assert!(b.validate_new_admission(now).is_ok());
    assert_eq!(
        b.validate_new_admission(now + time::Duration::hours(2)),
        Err(NoteMutationError::Expired)
    );
    assert_eq!(
        b.validate_new_admission(now - time::Duration::hours(24)),
        Err(NoteMutationError::Invalid)
    );
    let mut changed = b.clone();
    changed.header.local_edit_sequence += 1;
    assert_eq!(changed.validate(), Err(NoteMutationError::Mismatch));
    assert!(b.validate().is_ok());
    let mut raw = serde_json::to_value(&b).unwrap();
    raw["payloadDigest"] = json!("a".repeat(64));
    assert!(serde_json::from_value::<NoteStageBegin>(raw).is_err());
}
#[test]
fn stage_search_header_and_safe_counters_are_strict() {
    let mut h = begin().header;
    h.query = Some(NoteStageSearch {
        text: "literal😀".into(),
        case_sensitive: false,
        mode: NoteStageSearchMode::RenderedText,
    });
    assert!(h.validate().is_err());
    h.output = NoteStageOutput::Search;
    assert!(h.validate().is_ok());
    h.query.as_mut().unwrap().case_sensitive = true;
    assert!(h.validate().is_err());
    h.query.as_mut().unwrap().case_sensitive = false;
    h.query.as_mut().unwrap().text = "😀".repeat(257);
    assert!(h.validate().is_err());
    h.query = None;
    h.output = NoteStageOutput::Source;
    h.local_edit_sequence = SAFE + 1;
    assert!(h.validate().is_err());
}
#[test]
fn stage_chunk_shape_hash_byte_and_frame_limits() {
    let header = begin().header;
    let a = append(
        NoteStageStream::Text,
        vec![json!({"kind":"text","id":"t","offset":0,"text":"\r\n😀\\\""})],
    );
    assert_eq!(a.validate(&header).unwrap().len(), 1);
    let mut bad = a.clone();
    bad.records[0]["text"] = json!("changed");
    assert_eq!(
        bad.validate(&header).unwrap_err(),
        NoteMutationError::Mismatch
    );
    bad = a.clone();
    bad.previous_digest = Some("a".repeat(64));
    assert!(bad.validate(&header).is_err());
    bad = a.clone();
    bad.sequence = 1;
    assert!(bad.validate(&header).is_err());
    let mut bad = append(
        NoteStageStream::Text,
        vec![json!({"kind":"text","id":"t","offset":0,"text":"x".repeat(16384)})],
    );
    assert!(bad.validate(&header).is_ok());
    bad.records
        .push(json!({"kind":"text","id":"t","offset":16384,"text":"x"}));
    assert_eq!(
        bad.validate(&header).unwrap_err(),
        NoteMutationError::Budget
    );
    let mut bad = a.clone();
    bad.records = vec![a.records[0].clone(); 129];
    assert_eq!(
        bad.validate(&header).unwrap_err(),
        NoteMutationError::Budget
    );
    let mut bad = a.clone();
    bad.records[0]["unexpected"] = json!(true);
    assert!(bad.validate(&header).is_err());
    assert!(validate_stage_frame_bytes(65536).is_ok());
    assert_eq!(
        validate_stage_frame_bytes(65537),
        Err(NoteMutationError::Budget)
    );
}
#[test]
fn stage_dirty_tail_preserves_chronology_across_chunks_and_rolls_back_on_error() {
    let h = begin().header;
    let first = append(
        NoteStageStream::Dirty,
        vec![splice(2, 0, 5, 7), splice(2, 1, 10, 10)],
    )
    .validate(&h)
    .unwrap();
    let tail = NoteStageTail::default()
        .advance(NoteStageStream::Dirty, &first)
        .unwrap();
    let second = append(
        NoteStageStream::Dirty,
        vec![splice(2, 2, 20, 22), splice(9, 0, 0, 1)],
    )
    .validate(&h)
    .unwrap();
    let next = tail.advance(NoteStageStream::Dirty, &second).unwrap();
    assert_eq!(next.local_sequence, Some(9));
    assert_eq!(next.next_ordinal, 1);
    for records in [
        vec![splice(2, 2, 10, 11)],
        vec![splice(2, 3, 20, 21)],
        vec![splice(1, 0, 0, 1)],
        vec![splice(9, 1, 0, 1)],
    ] {
        let parsed = append(NoteStageStream::Dirty, records)
            .validate(&h)
            .unwrap();
        assert!(tail.advance(NoteStageStream::Dirty, &parsed).is_err());
    }
    assert_eq!(tail.local_sequence, Some(2));
    assert_eq!(tail.next_ordinal, 2);
    assert!(append(NoteStageStream::Dirty, vec![splice(10, 0, 0, 1)])
        .validate(&h)
        .is_err());
    let reopened = append(NoteStageStream::Dirty, vec![splice(2, 3, 30, 31)])
        .validate(&h)
        .unwrap();
    assert!(next.advance(NoteStageStream::Dirty, &reopened).is_err());
}
#[test]
fn stage_streams_enforce_distinct_shapes_and_reference_bounds() {
    let h = begin().header;
    let mutation = json!({"kind":"splice","ordinal":0,"start":0,"end":0,"replacement":text_ref()});
    assert!(append(NoteStageStream::Mutation, vec![mutation.clone()])
        .validate(&h)
        .is_ok());
    assert!(append(NoteStageStream::Dirty, vec![mutation.clone()])
        .validate(&h)
        .is_err());
    let mut read = h.clone();
    read.action = NoteStageAction::Read;
    assert!(append(NoteStageStream::Mutation, vec![mutation])
        .validate(&read)
        .is_err());
    let range = json!({"kind":"range","ordinal":0,"start":0,"end":0,"anchorAffinity":"before","headAffinity":"after","direction":"backward"});
    assert!(append(NoteStageStream::Selection, vec![range.clone()])
        .validate(&h)
        .is_ok());
    let mut all = h.clone();
    all.selection = NoteStageSelection::All;
    assert!(append(NoteStageStream::Selection, vec![range])
        .validate(&all)
        .is_err());
    let mut projection = json!({"kind":"projection","ordinal":0,"sourceRange":{"start":0,"end":1},"role":"marker-occurrence","detail":text_ref()});
    assert!(append(NoteStageStream::Live, vec![projection.clone()])
        .validate(&h)
        .is_err());
    projection["canonicalId"] = json!("existing-marker");
    assert!(append(NoteStageStream::Live, vec![projection.clone()])
        .validate(&h)
        .is_ok());
    projection["detail"]["length"] = json!(SAFE + 1);
    assert!(append(NoteStageStream::Live, vec![projection])
        .validate(&h)
        .is_err());
}
#[test]
fn stage_manifest_has_exact_stream_order_without_total_record_cap() {
    let b = begin();
    let mut seal = NoteStageSeal {
        backend_id: b.backend_id,
        workspace_id: b.workspace_id,
        note_id: b.note_id,
        note_instance_id: b.note_instance_id,
        operation_id: b.operation_id,
        header_digest: b.header_digest,
        manifest: NOTE_STAGE_STREAMS
            .into_iter()
            .map(|stream| NoteStageManifestEntry {
                stream,
                chunks: 0,
                records: 0,
                last_digest: None,
            })
            .collect(),
        payload_digest: String::new(),
    };
    seal.payload_digest = seal.computed_digest().unwrap();
    assert!(seal.validate().is_ok());
    seal.manifest[0].chunks = 1_000_000;
    seal.manifest[0].records = 128_000_000;
    seal.manifest[0].last_digest = Some("a".repeat(64));
    seal.payload_digest = seal.computed_digest().unwrap();
    assert!(seal.validate().is_ok());
    let mut bad = seal.clone();
    bad.manifest.swap(0, 1);
    assert!(bad.validate().is_err());
    bad = seal.clone();
    bad.manifest.pop();
    assert!(bad.validate().is_err());
    bad = seal.clone();
    bad.manifest[0].records += 1;
    assert!(bad.validate().is_err());
    bad = seal;
    bad.payload_digest = "b".repeat(64);
    assert_eq!(bad.validate(), Err(NoteMutationError::Mismatch));
}
#[test]
fn stage_commit_and_cancel_keep_exact_identity_and_digest_shapes() {
    let b = begin();
    let mut raw = serde_json::to_value(b).unwrap();
    raw.as_object_mut().unwrap().remove("header");
    raw.as_object_mut().unwrap().remove("expiresAt");
    let cancel: NoteStageCancel = serde_json::from_value(raw.clone()).unwrap();
    assert!(cancel.validate().is_ok());
    raw["payloadDigest"] = json!("a".repeat(64));
    let commit: NoteStageCommit = serde_json::from_value(raw.clone()).unwrap();
    assert!(commit.validate().is_ok());
    assert!(serde_json::from_value::<NoteStageCancel>(raw.clone()).is_err());
    raw["payloadDigest"] = json!("A".repeat(64));
    assert!(serde_json::from_value::<NoteStageCommit>(raw)
        .unwrap()
        .validate()
        .is_err());
}

#[test]
fn stage_required_null_chain_and_optional_omission_are_distinct() {
    let a = append(NoteStageStream::Text, vec![]);
    let mut raw = serde_json::to_value(a).unwrap();
    assert!(serde_json::from_value::<NoteStageAppend>(raw.clone()).is_ok());
    raw.as_object_mut().unwrap().remove("previousDigest");
    assert!(serde_json::from_value::<NoteStageAppend>(raw).is_err());
    let missing = json!({"stream":"text","chunks":0,"records":0});
    assert!(serde_json::from_value::<NoteStageManifestEntry>(missing).is_err());
    let mut b = serde_json::to_value(begin()).unwrap();
    b["header"]["query"] = Value::Null;
    assert!(serde_json::from_value::<NoteStageBegin>(b).is_err());
}
