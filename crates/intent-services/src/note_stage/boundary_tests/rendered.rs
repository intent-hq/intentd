use super::{begin_request, guest, query, Boundary, BOUNDARY};
use crate::{tests::setup, Services};
use intent_core::{
    note_stage::{
        NoteStageAction, NoteStageAppend, NoteStageBegin, NoteStageOutput, NoteStageSeal,
        NoteStageSelection,
    },
    with_caller, Caller, Error,
};
use serde_json::{json, Value};
use std::fmt::Write as _;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};

async fn seal_rendered(
    service: &Services,
    caller: &Caller,
    mut begin: NoteStageBegin,
) -> NoteStageBegin {
    use sha2::{Digest, Sha256};
    begin.header.action = NoteStageAction::Read;
    begin.header.output = NoteStageOutput::Search;
    begin.header.selection = NoteStageSelection::Ranges;
    begin.header.query = Some(
        serde_json::from_value(
            json!({"text":"STRASSE","mode":"renderedText","caseSensitive":false}),
        )
        .unwrap(),
    );
    begin.header_digest = begin.computed_digest().unwrap();
    with_caller(caller.clone(), service.begin_note_stage(begin.clone()))
        .await
        .unwrap();
    let captured = "Straße😀";
    let hash = Sha256::digest(captured.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut out, byte| {
            write!(out, "{byte:02x}").unwrap();
            out
        },
    );
    let rendered = json!({"textId":"captured","length":8,"utf8Bytes":captured.len(),"sha256":hash});
    let mut text = vec![json!({"kind":"text","id":"captured","offset":0,"text":captured})];
    let mut details = Vec::new();
    for (id, resource) in [
        (
            "attrs",
            json!({"id":"root","parentId":null,"type":"object","childrenRef":"directory"}),
        ),
        (
            "directory",
            json!({"kind":"metadataChildren","items":[],"nextRef":null}),
        ),
        (
            "paragraph",
            json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":0,"to":10},"attributesRef":"attrs"}),
        ),
        (
            "inline",
            json!({"version":2,"nodeType":"text","parentOrdinal":0,"nativeRange":{"from":1,"to":9},"attributesRef":"attrs","renderedText":rendered}),
        ),
    ] {
        let raw =
            intent_core::note_artifact::canonical::canonical_json(&resource.to_string()).unwrap();
        let hash = Sha256::digest(raw.as_bytes()).iter().fold(
            String::with_capacity(64),
            |mut output, byte| {
                write!(output, "{byte:02x}").expect("writing to a String");
                output
            },
        );
        details.push(json!({"textId":id,"length":raw.encode_utf16().count(),"utf8Bytes":raw.len(),"sha256":hash}));
        text.push(json!({"kind":"text","id":id,"offset":0,"text":raw}));
    }
    let mut identity = serde_json::to_value(query(&begin)).unwrap();
    identity.as_object_mut().unwrap().remove("payloadDigest");
    let mut manifest = Vec::new();
    for (stream, records) in [
        ("text", text),
        ("dirty", vec![]),
        (
            "selection",
            vec![
                json!({"kind":"range","ordinal":0,"start":0,"end":8,"direction":"forward","anchorAffinity":"before","headAffinity":"after"}),
            ],
        ),
        ("mutation", vec![]),
        (
            "live",
            vec![
                json!({"kind":"projection","ordinal":0,"sourceRange":{"start":0,"end":8},"role":"selection-owner","detail":details[2]}),
                json!({"kind":"projection","ordinal":1,"sourceRange":{"start":0,"end":8},"role":"inline-span","detail":details[3]}),
            ],
        ),
    ] {
        let count = records.len();
        let digest = if count == 0 {
            Value::Null
        } else {
            let mut value = identity.clone();
            value["stream"] = json!(stream);
            value["sequence"] = json!(0);
            value["previousDigest"] = Value::Null;
            value["records"] = json!(records);
            value["chunkDigest"] = json!("0".repeat(64));
            let mut chunk: NoteStageAppend = serde_json::from_value(value).unwrap();
            chunk.chunk_digest = chunk.computed_digest().unwrap();
            with_caller(caller.clone(), service.append_note_stage(chunk.clone()))
                .await
                .unwrap();
            json!(chunk.chunk_digest)
        };
        manifest.push(json!({"stream":stream,"chunks":usize::from(count>0),"records":count,"lastDigest":digest}));
    }
    identity["manifest"] = json!(manifest);
    identity["payloadDigest"] = json!("0".repeat(64));
    let mut seal: NoteStageSeal = serde_json::from_value(identity).unwrap();
    seal.payload_digest = seal.computed_digest().unwrap();
    with_caller(caller.clone(), service.seal_note_stage(seal))
        .await
        .unwrap();
    begin
}

#[tokio::test]
async fn rendered_service_rechecks_search_and_detail_success_error_and_expiry() {
    for mode in 0..6 {
        let (_tmp, service, workspace, note) = setup("Straße😀").await;
        let caller = guest(&service, &workspace).await;
        let begin = seal_rendered(
            &service,
            &caller,
            begin_request(&service, &workspace, &note).await,
        )
        .await;
        let mut value = serde_json::to_value(query(&begin)).unwrap();
        value.as_object_mut().unwrap().remove("payloadDigest");
        value["kind"] = json!("search");
        let page = with_caller(
            caller.clone(),
            service.read_stage_source(serde_json::from_value(value.clone()).unwrap(), json!(1)),
        )
        .await
        .unwrap();
        assert_eq!(page["items"][0]["sourceRange"], json!({"start":0,"end":6}));
        if mode >= 3 {
            value["kind"] = json!("detail");
            value["ref"] = page["items"][0]["detailRef"].clone();
        }
        if mode % 3 == 1 {
            value["headerDigest"] = json!("f".repeat(64));
        }
        let boundary = Arc::new(Boundary {
            now: Mutex::new(None),
            expiry: Mutex::new(None),
            observed: AtomicU8::new(0),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let read = BOUNDARY.scope(
            boundary.clone(),
            with_caller(caller.clone(), async {
                if mode < 3 {
                    service
                        .read_stage_source(serde_json::from_value(value).unwrap(), json!(1))
                        .await
                } else {
                    let request: intent_core::note_receipt_detail::NoteOperationReceiptRead =
                        serde_json::from_value(value).unwrap();
                    service
                        .read_note_receipt(request.query().unwrap(), json!(1))
                        .await
                }
            }),
        );
        let change = async {
            boundary.reached.notified().await;
            assert_eq!(
                boundary.observed.load(Ordering::SeqCst),
                if mode % 3 == 1 { 3 } else { 1 }
            );
            if mode % 3 == 2 {
                let expiry = boundary.expiry.lock().unwrap().unwrap();
                assert_eq!(expiry, intent_core::parse_iso(&begin.expires_at).unwrap());
                *boundary.now.lock().unwrap() = Some(expiry);
            } else {
                let Caller::Wire { principal_id, .. } = &caller else {
                    panic!("wire caller")
                };
                service
                    .store
                    .remove_workspace_member(&workspace, principal_id)
                    .await
                    .unwrap();
            }
            boundary.release.notify_one();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(read, change)
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "rendered boundary mode {mode}: {error}; observed {}",
                boundary.observed.load(Ordering::SeqCst)
            )
        });
        if mode % 3 == 2 {
            assert!(matches!(
                result,
                Err(Error::NotePage(
                    intent_core::note_page::NotePageError::Expired
                ))
            ));
        } else {
            assert!(matches!(result, Err(Error::NotFound(_))));
        }
        assert!(service.stage_request_admission.0.lock().unwrap().is_empty());
    }
}
