//! Frozen Store results for the frontend receipt consumer; no transport or native-upload claim.
use super::{append, dirty, query, reference, request, seal_request, setup, undo_groups, writer};
use crate::Store;
use intent_core::{
    note_receipt_detail::{ReceiptDetailKind, ReceiptDetailQuery},
    note_stage::{NoteStageCommit, NoteStageStream},
    NoteVersionAuthor,
};
use serde_json::{json, Value};

async fn pages(
    store: &Store,
    mut request: ReceiptDetailQuery,
    transcript: &mut Vec<Value>,
) -> Vec<Value> {
    let mut items = Vec::new();
    loop {
        let id = json!("fe-receipt-capture");
        let response = store
            .read_note_receipt_detail("alice", &request, &id)
            .await
            .unwrap();
        let frame = json!({"jsonrpc":"2.0","id":id,"result":response});
        assert!(frame.to_string().len() <= request.max_wire_bytes);
        let rows = response["items"].as_array().unwrap();
        assert!(rows.len() <= request.max_items);
        items.extend(rows.iter().cloned());
        transcript.push(json!({"normalizedRequest":request,"cursor":request.cursor,"offset":request.offset,"response":response,"rpcId":id}));
        let Some(next) = response["nextCursor"].as_str() else {
            break;
        };
        assert!(!rows.is_empty());
        assert_ne!(request.cursor.as_deref(), Some(next));
        request.cursor = Some(next.into());
        request.offset = None;
    }
    items
}

fn fe_query(receipt: &Value, kind: ReceiptDetailKind, reference: &str) -> ReceiptDetailQuery {
    let mut request = query(receipt, kind, reference);
    request.max_wire_bytes = 8192;
    request.max_source_bytes = 16384;
    request.max_items = if kind == ReceiptDetailKind::Detail {
        16
    } else {
        64
    };
    request
}

#[tokio::test]
async fn staged_receipt_fe_budget_transcript_retains_every_response() {
    let (store, _tmp, _note) = setup("ab😀cd").await;
    let mut begin = request(&store).await;
    begin.header.local_edit_sequence = 3;
    begin.header.live_generation = 3;
    begin.header_digest = begin.computed_digest().unwrap();
    let begin_response = store.begin_note_stage("alice", &begin).await.unwrap();
    let texts = [("upper-b", "BBB"), ("upper-c", "C"), ("tail", "Z")];
    let mut uploads = Vec::new();
    let mut previous = None;
    for (sequence, (id, text)) in texts.into_iter().enumerate() {
        let mut chunk = append(&begin, "");
        chunk.sequence = u64::try_from(sequence).unwrap();
        chunk.previous_digest = previous;
        chunk.records = vec![json!({"kind":"text","id":id,"offset":0,"text":text})];
        chunk.chunk_digest = chunk.computed_digest().unwrap();
        let response = store.append_note_stage("alice", &chunk).await.unwrap();
        previous = Some(chunk.chunk_digest.clone());
        uploads.push(json!({"request":chunk,"response":response}));
    }
    let records = vec![
        dirty(1, 0, 1, 2, &reference("upper-b", "BBB")),
        dirty(3, 0, 6, 7, &reference("upper-c", "C")),
        dirty(3, 1, 8, 8, &reference("tail", "Z")),
    ];
    let mut chunk = append(&begin, "");
    chunk.stream = NoteStageStream::Dirty;
    chunk.records = records.clone();
    chunk.chunk_digest = chunk.computed_digest().unwrap();
    let response = store.append_note_stage("alice", &chunk).await.unwrap();
    uploads.push(json!({"request":chunk,"response":response}));
    let seal = seal_request(&store, &begin).await;
    let seal_response = store.seal_note_stage("alice", &seal).await.unwrap();
    let commit = NoteStageCommit {
        backend_id: seal.backend_id.clone(),
        workspace_id: seal.workspace_id.clone(),
        note_id: seal.note_id.clone(),
        note_instance_id: seal.note_instance_id.clone(),
        operation_id: seal.operation_id.clone(),
        header_digest: seal.header_digest.clone(),
        payload_digest: seal.payload_digest.clone(),
    };
    let mut write = writer(&store, &commit).await;
    assert_eq!(write.source(), "aBBB😀CdZ");
    write
        .persist_source(
            &NoteVersionAuthor {
                id: "alice".into(),
                name: "Alice".into(),
                author_type: "user".into(),
            },
            &intent_core::now_iso(),
        )
        .await
        .unwrap();
    write.publish_annotation_anchors(&[]).await.unwrap();
    let receipt = write.commit().await.unwrap();
    let expected_inverse =
        undo_groups(&store, &receipt, "aBBB😀CdZ", &["aBBB😀cd", "ab😀cd"]).await;
    let mut transcript = Vec::new();
    let mut inverse = Vec::new();
    for kind in [
        ReceiptDetailKind::Mapping,
        ReceiptDetailKind::Effects,
        ReceiptDetailKind::Inverse,
    ] {
        let rows = pages(
            &store,
            fe_query(
                &receipt,
                kind,
                receipt[kind.reference_field()].as_str().unwrap(),
            ),
            &mut transcript,
        )
        .await;
        match kind {
            ReceiptDetailKind::Inverse => {
                assert_eq!(rows, expected_inverse);
                inverse = rows;
            }
            ReceiptDetailKind::Effects => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0]["kind"], "annotationInvalidation");
            }
            _ => {
                assert!(!rows.is_empty());
            }
        }
    }
    assert_eq!(inverse.len(), 3, "every expected inverse row is captured");
    for (row, expected_text) in inverse.iter().zip(["c", "", "b"]) {
        let mut text_query = fe_query(
            &receipt,
            ReceiptDetailKind::InverseText,
            receipt["inverseRef"].as_str().unwrap(),
        );
        text_query.max_items = 1;
        text_query.max_source_bytes = 4096;
        text_query.text_id = Some(row["replacement"]["textId"].as_str().unwrap().into());
        text_query.offset = Some(0);
        let text_rows = pages(&store, text_query, &mut transcript).await;
        assert_eq!(text_rows.len(), 1);
        assert_eq!(text_rows[0]["offset"], 0);
        assert_eq!(text_rows[0]["text"], expected_text);
        let mut pending = vec![row["provenanceRef"].as_str().unwrap().to_owned()];
        let mut nodes = std::collections::BTreeSet::new();
        while let Some(reference) = pending.pop() {
            for node in pages(
                &store,
                fe_query(&receipt, ReceiptDetailKind::Detail, &reference),
                &mut transcript,
            )
            .await
            {
                assert!(nodes.insert(node["id"].as_str().unwrap().to_owned()));
                assert!(
                    node.get("valueRef").is_none(),
                    "tiny fixture uses inline scalars"
                );
                if let Some(children) = node["childrenRef"].as_str() {
                    pending.push(children.into());
                }
            }
        }
        assert_eq!(nodes.len(), 15);
    }
    eprintln!(
        "STAGED_RECEIPT_FE_CAPTURE={}",
        json!({"base":"ab😀cd","final":"aBBB😀CdZ","capturedGroups":[1,3],"begin":{"request":begin,"response":begin_response},"uploads":uploads,"dirtyRecords":records,"seal":{"request":seal,"response":seal_response},"commitRequest":commit,"receipt":receipt,"transcript":transcript})
    );
}
