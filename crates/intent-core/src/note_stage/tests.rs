use super::*;

fn source_search_header() -> NoteStageHeader {
    let mut header = begin().header;
    header.output = NoteStageOutput::Search;
    header.query = Some(NoteStageSearch {
        text: "literal".into(),
        case_sensitive: false,
        mode: NoteStageSearchMode::Source,
    });
    header
}

fn selection_range(ordinal: u64, start: u64, end: u64) -> Value {
    json!({"kind":"range","ordinal":ordinal,"start":start,"end":end,
        "anchorAffinity":"after","headAffinity":"before","direction":"backward"})
}

#[test]
fn source_search_header_rejects_empty_but_preserves_literal_whitespace() {
    let mut header = source_search_header();
    header.query.as_mut().unwrap().text.clear();
    assert_eq!(header.validate(), Err(NoteMutationError::Invalid));
    assert!(NoteStageTail::default()
        .advance_for_header(NoteStageStream::Selection, &[], &header)
        .is_err());
    for text in [" ", "\t\n", "\u{feff}", "literal"] {
        header.query.as_mut().unwrap().text = text.into();
        assert!(header.validate().is_ok());
        assert_eq!(header.query.as_ref().unwrap().text, text);
    }
    header.query.as_mut().unwrap().text.clear();
    header.query.as_mut().unwrap().mode = NoteStageSearchMode::RenderedText;
    assert!(header.validate().is_ok()); // Existing rendered policy is unchanged.
}

#[test]
fn source_search_tail_preserves_unordered_selection_records_across_pages() {
    let header = source_search_header();
    let header_before = serde_json::to_value(&header).unwrap();
    let mut tail = NoteStageTail::default();
    for (ordinal, (start, end)) in [(8, 12), (2, 9), (2, 9), (9, 14), (0, 0)]
        .into_iter()
        .enumerate()
    {
        let request = append(
            NoteStageStream::Selection,
            vec![selection_range(ordinal as u64, start, end)],
        );
        let original = serde_json::to_value(&request).unwrap();
        let records = request.validate(&header).unwrap();
        tail = tail
            .advance_for_header(NoteStageStream::Selection, &records, &header)
            .unwrap();
        assert_eq!(tail.next_ordinal, ordinal as u64 + 1);
        assert_eq!(tail.previous_range, Some((start, end)));
        assert_eq!(serde_json::to_value(&request).unwrap(), original);
        assert_eq!(request.computed_digest().unwrap(), request.chunk_digest);
        // Persisted tails contain no union, sorted records or policy flag.
        tail = serde_json::from_value(serde_json::to_value(&tail).unwrap()).unwrap();
    }
    assert_eq!(serde_json::to_value(&header).unwrap(), header_before);
}

#[test]
fn source_search_selection_exception_is_header_specific_and_default_is_strict() {
    for (start, end) in [(1, 3), (4, 10), (8, 12), (8, 13)] {
        let request = append(
            NoteStageStream::Selection,
            vec![selection_range(0, 8, 12), selection_range(1, start, end)],
        );
        let header = source_search_header();
        let records = request.validate(&header).unwrap();
        let tail = NoteStageTail::default();
        assert!(tail.advance(NoteStageStream::Selection, &records).is_err());
        assert!(tail
            .advance_for_header(NoteStageStream::Selection, &records, &header)
            .is_ok());
        let mut rendered = header.clone();
        rendered.query.as_mut().unwrap().mode = NoteStageSearchMode::RenderedText;
        for strict in [rendered, begin().header, {
            let mut h = begin().header;
            h.output = NoteStageOutput::SelectionMarkdown;
            h
        }] {
            assert!(tail
                .advance_for_header(NoteStageStream::Selection, &records, &strict)
                .is_err());
        }
    }
    let header = begin().header;
    let request = append(
        NoteStageStream::Selection,
        vec![selection_range(0, 1, 3), selection_range(1, 3, 5)],
    );
    let records = request.validate(&header).unwrap();
    assert!(NoteStageTail::default()
        .advance_for_header(NoteStageStream::Selection, &records, &header)
        .is_ok()); // Touching nonempty ranges were already legal in strict mode.
}

#[test]
fn source_search_tail_keeps_ordinal_errors_atomic_and_rejects_invalid_headers() {
    let header = source_search_header();
    let request = append(NoteStageStream::Selection, vec![selection_range(0, 8, 12)]);
    let tail = NoteStageTail::default()
        .advance_for_header(
            NoteStageStream::Selection,
            &request.validate(&header).unwrap(),
            &header,
        )
        .unwrap();
    let original = tail.clone();
    for ordinal in [0, 2, SAFE] {
        let records = [NoteStageRecord::Range {
            ordinal,
            start: 0,
            end: 1,
            anchor_affinity: NoteStageAffinity::After,
            head_affinity: NoteStageAffinity::Before,
            direction: NoteStageDirection::Backward,
        }];
        assert!(tail
            .advance_for_header(NoteStageStream::Selection, &records, &header)
            .is_err());
        assert_eq!(tail, original);
    }
    let request = append(
        NoteStageStream::Selection,
        vec![selection_range(1, 0, 1), selection_range(3, 1, 2)],
    );
    assert!(tail
        .advance_for_header(
            NoteStageStream::Selection,
            &request.validate(&header).unwrap(),
            &header,
        )
        .is_err());
    assert_eq!(tail, original);
    for invalid in [
        {
            let mut h = header.clone();
            h.query = None;
            h
        },
        {
            let mut h = header.clone();
            h.query.as_mut().unwrap().case_sensitive = true;
            h
        },
    ] {
        assert!(tail
            .advance_for_header(NoteStageStream::Selection, &[], &invalid)
            .is_err());
    }
}

#[test]
fn source_search_tail_does_not_relax_dirty_or_mutation_ordering() {
    let header = source_search_header();
    for stream in [NoteStageStream::Dirty, NoteStageStream::Mutation] {
        for (start, end) in [(1, 3), (4, 10), (8, 12)] {
            let mut records = vec![splice(9, 0, 8, 12), splice(9, 1, start, end)];
            if stream == NoteStageStream::Mutation {
                for record in &mut records {
                    record.as_object_mut().unwrap().remove("localSequence");
                }
            }
            let request = append(stream, records);
            assert!(NoteStageTail::default()
                .advance_for_header(stream, &request.validate(&header).unwrap(), &header)
                .is_err());
        }
    }
    let request = append(
        NoteStageStream::Dirty,
        vec![splice(8, 0, 8, 12), splice(9, 0, 0, 1)],
    );
    let tail = NoteStageTail::default()
        .advance_for_header(
            NoteStageStream::Dirty,
            &request.validate(&header).unwrap(),
            &header,
        )
        .unwrap();
    let reopen = append(NoteStageStream::Dirty, vec![splice(8, 0, 20, 21)]);
    assert!(tail
        .advance_for_header(
            NoteStageStream::Dirty,
            &reopen.validate(&header).unwrap(),
            &header,
        )
        .is_err());
}

#[test]
fn source_search_selection_still_requires_valid_record_shapes_and_bounds() {
    let header = source_search_header();
    for value in [
        selection_range(0, 4, 3),
        selection_range(0, 0, SAFE + 1),
        selection_range(SAFE, 0, 1),
        json!({"kind":"range","ordinal":0,"start":0,"end":1,
            "anchorAffinity":"sideways","headAffinity":"before","direction":"forward"}),
    ] {
        assert!(append(NoteStageStream::Selection, vec![value])
            .validate(&header)
            .is_err());
    }
    let request = append(NoteStageStream::Selection, vec![selection_range(0, 1, 2)]);
    let records = request.validate(&header).unwrap();
    for stream in [
        NoteStageStream::Dirty,
        NoteStageStream::Mutation,
        NoteStageStream::Live,
        NoteStageStream::Text,
    ] {
        assert!(NoteStageTail::default()
            .advance_for_header(stream, &records, &header)
            .is_err());
    }
    let mut all = header;
    all.selection = NoteStageSelection::All;
    assert!(request.validate(&all).is_err());
}

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

#[test]
fn stage_max_density_chunks_fit_published_request_limits() {
    let header = begin().header;
    for stream in [
        NoteStageStream::Dirty,
        NoteStageStream::Mutation,
        NoteStageStream::Live,
    ] {
        let records=(0..128).map(|i|match stream {
            NoteStageStream::Dirty=>splice(9,i,i*2,i*2+1),
            NoteStageStream::Mutation=>json!({"kind":"splice","ordinal":i,"start":i*2,"end":i*2+1,"replacement":text_ref()}),
            _=>json!({"kind":"projection","ordinal":i,"sourceRange":{"start":i*2,"end":i*2+1},"role":"marker-occurrence","canonicalId":format!("marker-{i}"),"detail":text_ref()}),
        }).collect();
        let request = append(stream, records);
        let frame =
            json!({"jsonrpc":"2.0","id":"dense","method":"note.operation.append","params":request});
        assert!(frame.to_string().len() < 65536);
        assert_eq!(request.validate(&header).unwrap().len(), 128);
        let mut too_many = request.clone();
        too_many.records.push(request.records[0].clone());
        assert_eq!(
            too_many.validate(&header).unwrap_err(),
            NoteMutationError::Budget
        );
    }
}

#[test]
fn stage_canonical_hash_matches_shared_jcs_vectors_and_preserves_artifact_limits() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/native_artifact_canonicalization.json"
    ))
    .unwrap();
    let fixture = &fixture["canonicalization"];
    for vector in fixture["values"]
        .as_array()
        .unwrap()
        .iter()
        .chain(fixture["numbers"].as_array().unwrap())
        .chain(std::iter::once(&fixture["header"]))
        .chain(fixture["append"].as_array().unwrap())
    {
        let value: Value = serde_json::from_str(vector["rawJson"].as_str().unwrap()).unwrap();
        assert_eq!(
            canonical::bytes(&value).unwrap(),
            vector["canonical"],
            "{}",
            vector["id"]
        );
        assert_eq!(
            digest(&value).unwrap(),
            vector["sha256"],
            "{}",
            vector["id"]
        );
    }
    let value = json!({"stream":"dirty","sequence":0,"previousDigest":null,"records":(0..128).map(|i|splice(9,i,i*2,i*2+1)).collect::<Vec<_>>()});
    assert!(crate::note_artifact::canonical::digest(&value.to_string()).is_err());
    assert!(digest(&value).is_ok());
    assert_eq!(
        canonical::bytes(&json!("x".repeat(65534))).unwrap().len(),
        65536
    );
    assert_eq!(
        digest(&json!("x".repeat(65535))),
        Err(NoteMutationError::Budget)
    );
    assert_eq!(
        digest(&json!("\n".repeat(32768))),
        Err(NoteMutationError::Budget)
    );
    let mut deep = Value::Null;
    for _ in 0..34 {
        deep = json!([deep]);
    }
    assert_eq!(digest(&deep), Err(NoteMutationError::Budget));
}
