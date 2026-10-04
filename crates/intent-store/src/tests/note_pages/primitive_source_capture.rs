//! Real Store capture for prepared consumer source-binding tests. These fixtures
//! do not exercise an artifact RPC, renderer-profile grant or production adapter.
use super::{page, record_page, request, setup, Store};
use intent_core::note_artifact::request::ArtifactHeader;
use serde_json::{json, Value};

async fn resources(store: &Store, mut input: Value, calls: &mut Vec<Value>) -> Vec<Value> {
    let mut items = Vec::new();
    for _ in 0..128 {
        let response = record_page(store, input.clone(), calls).await;
        items.extend(response["items"].as_array().unwrap().iter().cloned());
        if response["nextCursor"].is_null() {
            return items;
        }
        input["cursor"] = response["nextCursor"].clone();
    }
    panic!("small fixture resource pagination did not terminate");
}

async fn capture_sources(store: &Store) -> (Vec<Value>, Vec<Value>) {
    let mut calls = Vec::new();
    let first = record_page(
        store,
        json!({"kind":"source","maxSourceBytes":4096,"maxWireBytes":8192}),
        &mut calls,
    )
    .await;
    assert!(first["nextCursor"].is_null());
    let context = resources(
        store,
        json!({"kind":"context","contextRef":first["contextRef"],"maxItems":64,"maxWireBytes":8192}),
        &mut calls,
    )
    .await;
    let mut sources = Vec::new();
    for atom in context.iter().filter(|item| item["nodeClass"] == "atom") {
        let owner = resources(store, json!({"kind":"context","contextRef":atom["nativeRef"],"maxItems":1,"maxWireBytes":8192}), &mut calls).await;
        assert_eq!(owner, vec![atom.clone()]);
        let root = resources(
            store,
            json!({"kind":"metadata","ref":atom["attributesRef"],"maxItems":1,"maxWireBytes":8192}),
            &mut calls,
        )
        .await;
        let fields = resources(store, json!({"kind":"metadata","ref":root[0]["childrenRef"],"maxItems":1,"maxWireBytes":8192}), &mut calls).await;
        let code = fields.iter().find(|field| field["key"] == "code").unwrap();
        assert!(
            code["valueRef"].is_string(),
            "empty/short code needs a value ref"
        );
        let fragments = resources(store, json!({"kind":"context","contextRef":code["valueRef"],"maxItems":1,"maxWireBytes":8192}), &mut calls).await;
        let text: String = fragments
            .iter()
            .map(|item| item["text"].as_str().unwrap())
            .collect();
        let primitive = match atom["nodeType"].as_str().unwrap() {
            "mermaidBlock" => "mermaid",
            "diffBlock" => "diff",
            other => panic!("unexpected fixture atom {other}"),
        };
        let header = json!({
            "scope":first["scope"],
            "source":{"kind":"snapshot","snapshotId":first["snapshotId"],"sourceRevision":first["sourceRevision"],"ownerRef":atom["nativeRef"],"sourceRef":code["valueRef"]},
            "primitive":primitive,"profile":"test-profile-not-registered",
            "environment":{"width":800,"height":600,"theme":"light","fontRef":"test-font","fontSize":14,"devicePixelRatio":1},
            "reservation":{"payloadBytes":4096,"records":10,"indexEntries":10,"storageChargeBytes":65536}
        });
        let grant = store
            .authorize_note_artifact_source(
                "pages",
                "alice",
                &serde_json::from_value(header.clone()).unwrap(),
            )
            .await
            .unwrap();
        sources.push(json!({"node":atom,"codeField":code,"code":text,"header":header,
            "internalSourceGrant":{"scope":grant.scope,"snapshotId":grant.snapshot_id,"sourceRevision":grant.source_revision,"expiresAt":grant.expires_at},
            "grantLimit":"Source binding only; test profile is unregistered; no renderer or artifact publication authority."}));
    }
    (calls, sources)
}

async fn rejected_grant(store: &Store, label: &str, header: Value, principal: &str) -> Value {
    let typed: ArtifactHeader = serde_json::from_value(header.clone()).unwrap();
    let error = store
        .authorize_note_artifact_source("pages", principal, &typed)
        .await
        .unwrap_err();
    json!({"case":label,"principal":principal,"header":header,"internalStoreError":format!("{error:?}")})
}

async fn rejected_read(store: &Store, header: &Value) -> Value {
    let input = json!({"kind":"context","contextRef":header["source"]["sourceRef"],"maxItems":1,"maxWireBytes":8192});
    let error = store
        .read_note_page("pages", "spec", "alice", request(input.clone()), &json!(1))
        .await
        .unwrap_err();
    json!({"request":input,"internalStoreError":format!("{error:?}")})
}

#[tokio::test]
async fn indexed_mermaid_source_capture_preserves_exact_values_and_invalidation() {
    let groups: Value =
        serde_json::from_str(include_str!("../fixtures/note_primitive_native.json")).unwrap();
    let ids = [
        "adjacentHtmlBase64",
        "adjacentHtmlPlainWithoutArrow",
        "titledMermaidFence",
        "adjacentHtmlPlainAttributes",
        "adjacentHtmlPreCodePlain",
        "adjacentHtmlPreCodeBase64",
        "mixedFences",
    ];
    let mut captured = 0;
    for case in groups
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|group| group["cases"].as_array().unwrap())
        .filter(|case| ids.contains(&case["id"].as_str().unwrap()))
    {
        let source = case["source"].as_str().unwrap();
        let (store, _temporary, mut note) = setup(source).await;
        let (calls, sources) = capture_sources(&store).await;
        assert_eq!(sources.len(), case["atoms"].as_array().unwrap().len());
        for (actual, expected) in sources.iter().zip(case["atoms"].as_array().unwrap()) {
            assert_eq!(actual["node"]["nodeType"], expected["type"]);
            assert_eq!(actual["code"], expected["code"], "{}", case["id"]);
        }
        let mermaid = sources
            .iter()
            .find(|item| item["header"]["primitive"] == "mermaid")
            .unwrap();
        let header = &mermaid["header"];
        let mut negatives =
            vec![rejected_grant(&store, "different-principal", header.clone(), "bob").await];
        for (label, path, value) in [
            (
                "different-note-instance",
                "/scope/noteInstanceId",
                json!("00000000-0000-0000-0000-000000000001"),
            ),
            (
                "different-revision",
                "/source/sourceRevision",
                json!("not-the-captured-revision"),
            ),
        ] {
            let mut changed = header.clone();
            *changed.pointer_mut(path).unwrap() = value;
            negatives.push(rejected_grant(&store, label, changed, "alice").await);
        }
        if let Some(other) = sources
            .iter()
            .find(|item| item["header"]["primitive"] == "diff")
        {
            for field in ["ownerRef", "sourceRef"] {
                let mut changed = header.clone();
                changed["source"][field] = other["header"]["source"][field].clone();
                negatives.push(rejected_grant(&store, field, changed, "alice").await);
            }
        }
        let mut invalidations = Vec::new();
        if case["id"] == "adjacentHtmlPlainWithoutArrow" {
            note.title = "metadata revision changed".into();
            store.update_note(&note).await.unwrap();
            let old_read = rejected_read(&store, header).await;
            let old_grant =
                rejected_grant(&store, "after-metadata-edit", header.clone(), "alice").await;
            let (fresh_calls, fresh_sources) = capture_sources(&store).await;
            let fresh = &fresh_sources
                .iter()
                .find(|item| item["header"]["primitive"] == "mermaid")
                .unwrap()["header"];
            assert_ne!(
                fresh["source"]["sourceRevision"],
                header["source"]["sourceRevision"]
            );
            invalidations.push(json!({"mutation":"metadata-title","oldRead":old_read,"oldGrant":old_grant,"calls":fresh_calls,"sources":fresh_sources}));
            note.content = "<div data-type=\"mermaid-block\" data-mermaid-code=\"graph TD\n B[Beta]\n\"></div>".into();
            store.update_note(&note).await.unwrap();
            let old_read = rejected_read(&store, fresh).await;
            let old_grant = rejected_grant(&store, "after-code-edit", fresh.clone(), "alice").await;
            let (changed_calls, changed_sources) = capture_sources(&store).await;
            assert_eq!(changed_sources[0]["code"], "graph TD\n B[Beta]");
            invalidations.push(json!({"mutation":"canonical-code-value","source":note.content,"oldRead":old_read,"oldGrant":old_grant,"calls":changed_calls,"sources":changed_sources}));
            let before = changed_sources[0]["header"].clone();
            store
                .delete_note(&note.workspace_id, &note.id)
                .await
                .unwrap();
            store.insert_note(&note).await.unwrap();
            let old_read = rejected_read(&store, &before).await;
            let old_grant =
                rejected_grant(&store, "after-note-recreation", before.clone(), "alice").await;
            let fresh_page = page(&store, json!({"kind":"source","maxSourceBytes":4096})).await;
            assert_ne!(
                fresh_page["scope"]["noteInstanceId"],
                before["scope"]["noteInstanceId"]
            );
            invalidations.push(json!({"mutation":"delete-recreate-same-note-id","oldRead":old_read,"oldGrant":old_grant,"newSourcePage":fresh_page}));
        }
        if let Ok(directory) = std::env::var("NOTE_PAGE_TRANSCRIPT_DIR") {
            use std::io::Write;
            let output = json!({"caseId":case["id"],"source":source,"oracleAtoms":case["atoms"],"readiness":"test-only-real-store-capture", "calls":calls,"sources":sources,"internalGrantFailures":negatives,"internalInvalidationChecks":invalidations,"limitations":"Store results captured before transport. Internal source grants do not register a profile, authorize construction, publish an artifact or prove a production provider."});
            let path = std::path::Path::new(&directory)
                .join(format!("mermaid-{}.json", case["id"].as_str().unwrap()));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap();
            file.write_all(&serde_json::to_vec_pretty(&output).unwrap())
                .unwrap();
        }
        store.close().await;
        captured += 1;
    }
    assert_eq!(captured, ids.len());
}
