//! Actual authenticated prepared endpoints -> Services -> indexed Store. No production registration.
use super::*;
use intent_core::{
    note_artifact::request::Primitive,
    note_source_session::{Binding, Descriptor, Operation},
    ContentType, Note, NoteId, NoteMetadata, NoteVisibility, Workspace, WorkspaceActivity,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_store::CanonicalSourceBinding;
type Client = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
fn sample_ws() -> Workspace {
    Workspace {
        id: WorkspaceId::from("ws-1"),
        title: "WS One".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    }
}

fn sample_note(ws: &WorkspaceId) -> Note {
    Note {
        id: NoteId::from("note-1"),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: "# Hi".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: "t0".to_string(),
        rev: 0,
        updated_at: "t0".to_string(),
    }
}
async fn page(
    store: &Store,
    ws: &WorkspaceId,
    note: &NoteId,
    principal: &str,
    value: Value,
) -> Value {
    store
        .read_note_page(
            &ws.0,
            &note.0,
            principal,
            serde_json::from_value(value).unwrap(),
            &json!("source-fixture"),
        )
        .await
        .unwrap()
}

async fn binding_as(
    store: &Store,
    ws: &WorkspaceId,
    note: &NoteId,
    principal: &str,
) -> CanonicalSourceBinding {
    let first = page(
        store,
        ws,
        note,
        principal,
        json!({"kind":"source","maxSourceBytes":128,"maxWireBytes":8192}),
    )
    .await;
    let context = page(store, ws, note, principal, json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let owner = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["nodeType"] == "diffBlock" || v["nodeType"] == "mermaidBlock")
        .unwrap();
    let root = page(
        store,
        ws,
        note,
        principal,
        json!({"kind":"metadata","ref":owner["attributesRef"],"maxItems":1,"maxWireBytes":8192}),
    )
    .await;
    let fields = page(store, ws, note, principal, json!({"kind":"metadata","ref":root["items"][0]["childrenRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let code = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["key"] == "code")
        .unwrap();
    CanonicalSourceBinding {
        scope: serde_json::from_value(first["scope"].clone()).unwrap(),
        snapshot_id: first["snapshotId"].as_str().unwrap().into(),
        source_revision: first["sourceRevision"].as_str().unwrap().into(),
        primitive: if owner["nodeType"] == "diffBlock" {
            Primitive::Diff
        } else {
            Primitive::Mermaid
        },
        owner_ref: owner["nativeRef"].as_str().unwrap().into(),
        source_ref: code["valueRef"].as_str().unwrap().into(),
    }
}

fn operation(
    services: &Services,
    binding: &CanonicalSourceBinding,
    expiry: &str,
    nonce: u8,
) -> Operation {
    let descriptor = Descriptor {
        nonce: format!("{nonce:032x}"),
        daemon_incarnation: services.prepared_source_contexts().incarnation().into(),
        workspace_id: binding.scope.workspace_id.clone(),
        binding: Binding {
            scope: binding.scope.clone(),
            snapshot_id: binding.snapshot_id.clone(),
            source_revision: binding.source_revision.clone(),
            primitive: binding.primitive,
            owner_ref: binding.owner_ref.clone(),
            source_ref: binding.source_ref.clone(),
        },
        accept_until: expiry.into(),
    };
    let operation_id = intent_core::note_artifact::canonical::digest(
        &json!({"domain":"note.sourceSession.open.v1","descriptor":descriptor}).to_string(),
    )
    .unwrap();
    Operation {
        descriptor,
        operation_id,
    }
}

async fn ready(f: &Fixture, mode: Mode) -> Client {
    ready_token(f, mode, TOKEN).await
}
async fn ready_token(f: &Fixture, mode: Mode, token: &str) -> Client {
    let mut ws = f.socket(mode, token).await.unwrap();
    ws.send(Message::Text(hello(mode, &json!("hello")).into()))
        .await
        .unwrap();
    let response = ws.next().await.unwrap().unwrap().into_text().unwrap();
    let value: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(
        value["result"]["server"]["sourceSession"]["mode"],
        mode.as_str()
    );
    assert_eq!(
        value["result"]["server"]["sourceSession"]["daemonIncarnation"],
        f.shared.root.incarnation()
    );
    ws
}
async fn rpc(ws: &mut Client, id: Value, method: &str, params: Value, pad: usize) -> Value {
    let mut raw = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string();
    if pad > raw.len() {
        raw.extend(std::iter::repeat_n(' ', pad - raw.len()));
    }
    assert!(raw.len() <= 65536);
    ws.send(Message::Text(raw.into())).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("owned response")
        .expect("response frame")
        .expect("valid WS")
        .into_text()
        .unwrap();
    assert!(response.len() <= 8192);
    let response: Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["id"], id);
    assert!(response.get("error").is_none(), "{response}");
    response["result"].clone()
}
async fn seed(f: &Fixture, content: &str, nonce: u8) -> (CanonicalSourceBinding, Operation) {
    let workspace = sample_ws();
    f.store.insert_workspace(&workspace).await.unwrap();
    let mut note = sample_note(&workspace.id);
    note.content = content.into();
    f.store.insert_note(&note).await.unwrap();
    let principal = f.store.get_primary_principal().await.unwrap();
    let principal = format!("principal:{}", principal.id.0);
    let binding = binding_as(&f.store, &workspace.id, &note.id, &principal).await;
    let hold = f
        .store
        .hold_canonical_source(&workspace.id.0, &principal, &binding)
        .unwrap();
    let op = operation(&f.shared.api, &binding, hold.expires_at(), nonce);
    drop(hold);
    (binding, op)
}
async fn read(ws: &mut Client, op: &Operation, seq: &mut u64, request: Value) -> Value {
    let result=rpc(ws,json!(format!("actual-\"id-{}",seq)),"note.sourceSession.read",json!({"workspaceId":op.descriptor.workspace_id,"operationId":op.operation_id,"sequence":*seq,"request":request}),8192).await;
    *seq += 1;
    result
}
async fn traverse(
    ws: &mut Client,
    op: &Operation,
    binding: &CanonicalSourceBinding,
    expected: &str,
) -> usize {
    let mut seq = 0;
    let page = read(
        ws,
        op,
        &mut seq,
        json!({"kind":"context","contextRef":binding.owner_ref,"maxItems":1,"maxWireBytes":8192}),
    )
    .await;
    let page=read(ws,op,&mut seq,json!({"kind":"metadata","ref":page["items"][0]["attributesRef"],"maxItems":1,"maxWireBytes":8192})).await;
    let fields = page["items"][0]["childrenRef"].clone();
    let mut cursor = Value::Null;
    loop {
        let mut request = json!({"kind":"metadata","ref":fields,"maxItems":1,"maxWireBytes":8192});
        if !cursor.is_null() {
            request["cursor"] = cursor.clone();
        }
        let page = read(ws, op, &mut seq, request).await;
        if page["items"][0]["key"] == "code" {
            assert_eq!(page["items"][0]["valueRef"], binding.source_ref);
            break;
        }
        cursor = page["nextCursor"].clone();
        assert!(!cursor.is_null());
    }
    let mut next = json!(binding.source_ref);
    let mut bytes = 0;
    let mut fragments = 0;
    loop {
        let page = read(
            ws,
            op,
            &mut seq,
            json!({"kind":"context","contextRef":next,"maxItems":1,"maxWireBytes":8192}),
        )
        .await;
        let item = &page["items"][0];
        let fragment = item["text"].as_str().unwrap();
        assert_eq!(fragment, &expected[bytes..bytes + fragment.len()]);
        bytes += fragment.len();
        fragments += 1;
        next = item["nextRef"].clone();
        if next.is_null() {
            break;
        }
    }
    assert_eq!(bytes, expected.len());
    fragments
}
#[tokio::test]
async fn source_lifecycle_real_wss_incremental_raw_code_and_replacement_cleanup() {
    let f = Fixture::new().await;
    let raw = "🦀x".repeat(40000);
    let (binding, op) = seed(
        &f,
        &format!("<div data-type=\"diff-block\" data-diff-code=\"{raw}\"></div>"),
        31,
    )
    .await;
    let mut ws = ready(&f, Mode::Read).await;
    let opened = rpc(
        &mut ws,
        json!("\0".repeat(64)),
        "note.sourceSession.open",
        serde_json::to_value(&op).unwrap(),
        65536,
    )
    .await;
    assert_eq!(opened["kind"], "sourceSessionOpened");
    assert_eq!(opened["sourceExpiresAt"], op.descriptor.accept_until);
    let fragments = traverse(&mut ws, &op, &binding, &raw).await;
    assert!(fragments > 1);
    let closing = rpc(
        &mut ws,
        json!("original-close"),
        "note.sourceSession.close",
        serde_json::to_value(&op).unwrap(),
        0,
    )
    .await;
    assert_eq!(closing["kind"], "sourceSessionClosing");
    drop(ws);
    let mut cleanup = ready(&f, Mode::Cleanup).await;
    let settled = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = rpc(
                &mut cleanup,
                json!("cleanup-current"),
                "note.sourceSession.close",
                serde_json::to_value(&op).unwrap(),
                65536,
            )
            .await;
            if result["kind"] != "sourceSessionClosing" {
                break result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(settled["kind"], "sourceSessionSettled");
    assert_eq!(settled["reason"], "closed");
    drop(cleanup);
    condition(|| {
        f.shared
            .observe
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v.ready && v.closed)
            .count()
            >= 2
    })
    .await;
    let observations = f.shared.observe.lock().unwrap();
    let source = observations
        .iter()
        .find(|o| o.written > 65536)
        .expect("legitimate traversal exceeds handshake lifetime output ceiling");
    assert!(source.read > 65536);
    assert!(source.closed);
    eprintln!(
        "source-lifecycle real TLS: fragments={fragments} read={} written={} bytes={}",
        source.read,
        source.written,
        raw.len()
    );
    drop(observations);
    f.finish().await;
}

#[test]
fn source_hello_integral_numeric_spellings_match_prepared_contract() {
    for id in [
        "1.0",
        "1e0",
        "-0",
        "9007199254740991.0",
        "-9007199254740991.0",
    ] {
        for version in ["1.0", "1e0"] {
            let raw = format!(
                r#"{{"jsonrpc":"2.0","id":{id},"method":"client.hello","params":{{"sourceSession":{{"version":{version},"mode":"read"}}}}}}"#
            );
            assert!(hello::validate(&raw, Mode::Read).is_ok(), "{raw}");
        }
    }
    for id in ["1.5", "9007199254740992.0", "-9007199254740992"] {
        let raw = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"client.hello","params":{{"sourceSession":{{"version":1,"mode":"read"}}}}}}"#
        );
        assert!(hello::validate(&raw, Mode::Read).is_err());
    }
}

#[tokio::test]
async fn source_lifecycle_control_uncertainty_retains_original_debt() {
    for partial_write in [false, true] {
        let f = Fixture::new().await;
        let (_, op) = seed(&f, "```diff\n+one\n```", 41).await;
        let principal = f.store.get_primary_principal().await.unwrap().id;
        let mut ws = ready(&f, Mode::Read).await;
        let opened = rpc(
            &mut ws,
            json!(1),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0,
        )
        .await;
        assert_eq!(opened["kind"], "sourceSessionOpened");
        let mut held = Vec::new();
        if partial_write {
            f.shared.partial_control.store(true, Ordering::Release);
        } else {
            for _ in 0..32 {
                held.push(f.store.read_pool().acquire().await.unwrap());
            }
        }
        // Duplicate open returns a source-free control while the original owner exists.
        ws.send(Message::Text(json!({"jsonrpc":"2.0","id":"control-error","method":"note.sourceSession.open","params":op}).to_string().into())).await.unwrap();
        if !partial_write {
            tokio::time::timeout(Duration::from_secs(5), f.shared.control_pending.notified())
                .await
                .unwrap();
            assert!(f.shared.observe.lock().unwrap().is_empty());
            let close = f.store.read_pool().close();
            tokio::pin!(close);
            assert!(futures_util::poll!(&mut close).is_pending());
            drop(held);
            close.await;
        }
        assert!(!matches!(ws.next().await, Some(Ok(Message::Text(_)))));
        condition(|| !f.shared.observe.lock().unwrap().is_empty()).await;
        assert_eq!(f.shared.root.counts().uncertain, 1);
        // Services-only cleanup observation avoids pretending a closed SQL pool
        // can authenticate a replacement wire connection. Actual failure above
        // used authenticated TLS and either real pool wait/error or injected IO.
        let mut context = f.shared.root.try_admit(Mode::Cleanup).unwrap();
        assert!(context.bind(&principal.0, [41; 16], None));
        context.phase(5);
        let cleanup = f
            .shared
            .api
            .prepared_source_connection(
                &context,
                Caller::Wire {
                    principal_id: principal,
                    host_role: intent_core::HostRole::Owner,
                },
            )
            .unwrap();
        let outcome = cleanup.close(op).unwrap();
        assert!(
            matches!(
                outcome,
                intent_core::note_source_session::Control::Uncertain { .. }
            ),
            "Context uncertainty cannot coexist with a settled operation: {outcome:?}"
        );
        drop(cleanup);
        context.retire();
        drop(ws);
        f.server.stop().await;
        assert_eq!(f.shared.root.counts().uncertain, 1);
        f.store.close().await;
    }
}

async fn cleanup_receipt(f: &Fixture, op: &Operation) -> Value {
    let mut cleanup = ready(f, Mode::Cleanup).await;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = rpc(
                &mut cleanup,
                json!("cleanup"),
                "note.sourceSession.close",
                serde_json::to_value(op).unwrap(),
                0,
            )
            .await;
            if result["kind"] != "sourceSessionClosing" {
                break result;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(cleanup);
    result
}
fn raw_request(id: Value, method: &str, params: Value, bytes: usize) -> String {
    let mut raw = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string();
    if bytes > raw.len() {
        raw.extend(std::iter::repeat_n(' ', bytes - raw.len()));
    }
    raw
}
// Test-only masked wire frame construction; production uses the pinned parser.
fn masked_frame(opcode: u8, final_frame: bool, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![opcode | if final_frame { 128 } else { 0 }];
    if payload.len() < 126 {
        out.push(128 | u8::try_from(payload.len()).unwrap());
    } else if payload.len() <= 65535 {
        out.push(128 | 126);
        out.extend_from_slice(&u16::try_from(payload.len()).unwrap().to_be_bytes());
    } else {
        out.push(128 | 127);
        out.extend_from_slice(&u64::try_from(payload.len()).unwrap().to_be_bytes());
    }
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(payload);
    out
}
async fn fragmented(ws: &mut Client, raw: &str) {
    let midpoint = raw.len() / 2;
    let mut frames = masked_frame(1, false, &raw.as_bytes()[..midpoint]);
    frames.extend(masked_frame(0, true, &raw.as_bytes()[midpoint..]));
    ws.get_mut().write_all(&frames).await.unwrap();
    ws.get_mut().flush().await.unwrap();
}
async fn response(ws: &mut Client) -> Option<Value> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(raw))) => {
                    assert!(raw.len() <= 8192);
                    return Some(serde_json::from_str(&raw).unwrap());
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return None,
                Some(Ok(other)) => panic!("unexpected source frame: {other:?}"),
            }
        }
    })
    .await
    .expect("text response or explicit local connection termination; Ping/Pong are not terminal")
}
fn error_response(value: &Value, id: Value, error: intent_core::note_source_session::SessionError) {
    assert_eq!(
        value,
        &json!({"jsonrpc":"2.0","id":id,"error":{"code":error.number(),"message":error.code(),"data":{"code":error.code()}}})
    );
}

#[tokio::test]
async fn source_lifecycle_scalar_empty_and_encoded_sources_stay_predecoder() {
    for (primitive, attribute, raw) in [
        ("diff-block", "data-diff-code", ""),
        ("diff-block", "data-diff-code", "YWJjCg=="),
        ("mermaid-block", "data-mermaid-code", "graph TD\n A[🦀]"),
        ("mermaid-block", "data-mermaid-code", ""),
    ] {
        let f = Fixture::new().await;
        let (binding, op) = seed(
            &f,
            &format!("<div data-type=\"{primitive}\" {attribute}=\"{raw}\"></div>"),
            51,
        )
        .await;
        let mut ws = ready(&f, Mode::Read).await;
        rpc(
            &mut ws,
            json!(-1),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0,
        )
        .await;
        assert_eq!(traverse(&mut ws, &op, &binding, raw).await, 1);
        drop(ws);
        let result = cleanup_receipt(&f, &op).await;
        assert_eq!(result["kind"], "sourceSessionSettled");
        f.finish().await;
    }
}

#[tokio::test]
async fn source_lifecycle_fragmented_hello_and_request_aggregate_boundaries() {
    for extra in [0, 1] {
        let f = Fixture::new().await;
        let mut ws = f.socket(Mode::Read, TOKEN).await.unwrap();
        let mut raw = hello(Mode::Read, &json!("\0".repeat(64)));
        raw.extend(std::iter::repeat_n(' ', 8192 + extra - raw.len()));
        fragmented(&mut ws, &raw).await;
        let result = response(&mut ws).await;
        if extra == 0 {
            let result = result.expect("fragmented hello success");
            assert_eq!(result["jsonrpc"], "2.0");
            assert_eq!(result["id"], "\0".repeat(64));
            assert!(result.get("error").is_none());
            assert_eq!(
                result["result"]["server"]["sourceSession"],
                json!({"version":1,"mode":"read","daemonIncarnation":f.shared.root.incarnation()})
            );
        } else {
            assert!(result.is_none());
        }
        drop(ws);
        f.finish().await;
    }
    for fragmented_input in [false, true] {
        for extra in [0, 1] {
            let f = Fixture::new().await;
            let (_, op) = seed(
                &f,
                "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
                52,
            )
            .await;
            let mut ws = ready(&f, Mode::Read).await;
            let raw = raw_request(
                json!("bounded-open"),
                "note.sourceSession.open",
                serde_json::to_value(&op).unwrap(),
                65536 + extra,
            );
            if fragmented_input {
                fragmented(&mut ws, &raw).await;
            } else {
                ws.send(Message::Text(raw.into())).await.unwrap();
            }
            let result = response(&mut ws).await;
            if extra == 0 {
                assert_eq!(result.unwrap()["result"]["kind"], "sourceSessionOpened");
            } else {
                assert!(result.is_none());
            }
            drop(ws);
            let result = cleanup_receipt(&f, &op).await;
            assert_eq!(result["kind"], "sourceSessionSettled");
            assert_eq!(
                result["reason"],
                if extra == 0 { "closed" } else { "cancelled" }
            );
            f.finish().await;
        }
    }
}

#[tokio::test]
async fn source_lifecycle_duplicate_epoch_sequence_and_cleanup_mode_are_terminal() {
    let f = Fixture::new().await;
    let (binding, op) = seed(
        &f,
        "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
        53,
    )
    .await;
    let mut original = ready(&f, Mode::Read).await;
    rpc(
        &mut original,
        json!(1),
        "note.sourceSession.open",
        serde_json::to_value(&op).unwrap(),
        0,
    )
    .await;
    let duplicate = rpc(
        &mut original,
        json!(2),
        "note.sourceSession.open",
        serde_json::to_value(&op).unwrap(),
        0,
    )
    .await;
    assert_eq!(duplicate["kind"], "sourceSessionAlreadyRegistered");
    let mut replacement = ready(&f, Mode::Read).await;
    assert_eq!(
        rpc(
            &mut replacement,
            json!(3),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0
        )
        .await["kind"],
        "sourceSessionAlreadyRegistered"
    );
    let request = json!({"workspaceId":op.descriptor.workspace_id,"operationId":op.operation_id,"sequence":0,"request":{"kind":"context","contextRef":binding.owner_ref,"maxItems":1,"maxWireBytes":8192}});
    replacement
        .send(Message::Text(
            raw_request(json!(4), "note.sourceSession.read", request.clone(), 0).into(),
        ))
        .await
        .unwrap();
    assert!(response(&mut replacement)
        .await
        .unwrap()
        .get("error")
        .is_some());
    drop(replacement);
    let owner_page = rpc(
        &mut original,
        json!(5),
        "note.sourceSession.read",
        request.clone(),
        0,
    )
    .await;
    let mut next = json!({"workspaceId":op.descriptor.workspace_id,"operationId":op.operation_id,"sequence":0,"request":{"kind":"metadata","ref":owner_page["items"][0]["attributesRef"],"maxItems":1,"maxWireBytes":8192}});
    original
        .send(Message::Text(
            raw_request(json!(6), "note.sourceSession.read", next.clone(), 0).into(),
        ))
        .await
        .unwrap();
    error_response(
        &response(&mut original).await.unwrap(),
        json!(6),
        intent_core::note_source_session::SessionError::Sequence,
    );
    next["sequence"] = json!(1);
    // An otherwise-valid continuation cannot revive this same terminal connection.
    let sent = original
        .send(Message::Text(
            raw_request(json!(9), "note.sourceSession.read", next, 0).into(),
        ))
        .await;
    if let Some(value) = response(&mut original).await {
        assert!(sent.is_ok());
        error_response(
            &value,
            json!(9),
            intent_core::note_source_session::SessionError::Unavailable,
        );
        assert!(response(&mut original).await.is_none());
    }
    drop(original);
    assert_eq!(
        cleanup_receipt(&f, &op).await["kind"],
        "sourceSessionSettled"
    );
    let mut cleanup = ready(&f, Mode::Cleanup).await;
    cleanup
        .send(Message::Text(
            raw_request(
                json!(7),
                "note.sourceSession.open",
                serde_json::to_value(&op).unwrap(),
                0,
            )
            .into(),
        ))
        .await
        .unwrap();
    assert!(response(&mut cleanup).await.unwrap().get("error").is_some());
    cleanup
        .send(Message::Text(
            raw_request(json!(8), "note.sourceSession.read", request, 0).into(),
        ))
        .await
        .unwrap();
    assert!(response(&mut cleanup).await.unwrap().get("error").is_some());
    drop(cleanup);
    f.finish().await;
}

fn owner_request(op: &Operation, binding: &CanonicalSourceBinding, seq: u64) -> Value {
    json!({"workspaceId":op.descriptor.workspace_id,"operationId":op.operation_id,"sequence":seq,"request":{"kind":"context","contextRef":binding.owner_ref,"maxItems":1,"maxWireBytes":8192}})
}
#[tokio::test]
async fn source_lifecycle_pending_input_observer_preserves_partial_frame_without_dispatch() {
    let f = Fixture::new().await;
    let (binding, op) = seed(
        &f,
        "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
        61,
    )
    .await;
    let mut ws = ready(&f, Mode::Read).await;
    rpc(
        &mut ws,
        json!(1),
        "note.sourceSession.open",
        serde_json::to_value(&op).unwrap(),
        0,
    )
    .await;
    let mut held = Vec::new();
    for _ in 0..32 {
        held.push(f.store.read_pool().acquire().await.unwrap());
    }
    let pending = f.shared.source_pending.load(Ordering::Acquire);
    ws.send(Message::Text(
        raw_request(
            json!(2),
            "note.sourceSession.read",
            owner_request(&op, &binding, 0),
            0,
        )
        .into(),
    ))
    .await
    .unwrap();
    condition(|| f.shared.source_pending.load(Ordering::Acquire) > pending).await;
    // Only the first frame-header byte arrives while actual Store work is held.
    // The same socket poll records TCP-meter growth and remains Pending.
    // This does not instrument the WebSocket header parser stage.
    let partial = f.shared.source_partial.load(Ordering::Acquire);
    ws.get_mut().write_all(&[0x81]).await.unwrap();
    ws.get_mut().flush().await.unwrap();
    condition(|| f.shared.source_partial.load(Ordering::Acquire) > partial).await;
    drop(held);
    let owner = response(&mut ws).await.unwrap();
    assert_eq!(owner["id"], 2);
    let attrs = owner["result"]["items"][0]["attributesRef"].clone();
    let next = raw_request(
        json!(3),
        "note.sourceSession.read",
        json!({"workspaceId":op.descriptor.workspace_id,"operationId":op.operation_id,"sequence":1,"request":{"kind":"metadata","ref":attrs,"maxItems":1,"maxWireBytes":8192}}),
        0,
    );
    let frame = masked_frame(1, true, next.as_bytes());
    ws.get_mut().write_all(&frame[1..]).await.unwrap();
    ws.get_mut().flush().await.unwrap();
    let result = response(&mut ws).await.unwrap();
    assert_eq!(result["id"], 3);
    assert!(result.get("error").is_none());
    drop(ws);
    assert_eq!(
        cleanup_receipt(&f, &op).await["kind"],
        "sourceSessionSettled"
    );
    f.finish().await;
}

#[tokio::test]
async fn source_lifecycle_completed_ping_during_held_store_read_revokes_and_waits() {
    let f = Fixture::new().await;
    let (binding, op) = seed(
        &f,
        "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
        62,
    )
    .await;
    let mut ws = ready(&f, Mode::Read).await;
    rpc(
        &mut ws,
        json!(1),
        "note.sourceSession.open",
        serde_json::to_value(&op).unwrap(),
        0,
    )
    .await;
    let mut held = Vec::new();
    for _ in 0..32 {
        held.push(f.store.read_pool().acquire().await.unwrap());
    }
    let pending = f.shared.source_pending.load(Ordering::Acquire);
    ws.send(Message::Text(
        raw_request(
            json!(2),
            "note.sourceSession.read",
            owner_request(&op, &binding, 0),
            0,
        )
        .into(),
    ))
    .await
    .unwrap();
    condition(|| f.shared.source_pending.load(Ordering::Acquire) > pending).await;
    ws.send(Message::Ping(vec![1].into())).await.unwrap();
    assert!(response(&mut ws).await.is_none());
    assert!(f.shared.observe.lock().unwrap().is_empty());
    assert_eq!(f.shared.root.counts().read, 2); // original held owner plus pending accept
    drop(held);
    drop(ws);
    assert_eq!(
        cleanup_receipt(&f, &op).await["kind"],
        "sourceSessionSettled"
    );
    f.finish().await;
}

#[tokio::test]
async fn source_lifecycle_guest_revocation_during_actual_pool_wait_blocks_disclosure_not_cleanup() {
    let f = Fixture::new().await;
    seed(
        &f,
        "<div data-type=\"diff-block\" data-diff-code=\"+secret\"></div>",
        63,
    )
    .await;
    let token = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let guest = PrincipalId::from_string("\u{feff}source-guest");
    f.guest(&guest.0, token).await;
    let workspace = WorkspaceId::from("ws-1");
    f.store
        .add_workspace_member(&workspace, &guest, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let principal = format!("principal:{}", guest.0);
    let binding = binding_as(&f.store, &workspace, &NoteId::from("note-1"), &principal).await;
    let hold = f
        .store
        .hold_canonical_source(&workspace.0, &principal, &binding)
        .unwrap();
    let op = operation(&f.shared.api, &binding, hold.expires_at(), 64);
    drop(hold);
    let mut ws = ready_token(&f, Mode::Read, token).await;
    rpc(
        &mut ws,
        json!(1),
        "note.sourceSession.open",
        serde_json::to_value(&op).unwrap(),
        0,
    )
    .await;
    let mut held = Vec::new();
    for _ in 0..32 {
        held.push(f.store.read_pool().acquire().await.unwrap());
    }
    let pending = f.shared.source_pending.load(Ordering::Acquire);
    ws.send(Message::Text(
        raw_request(
            json!(2),
            "note.sourceSession.read",
            owner_request(&op, &binding, 0),
            0,
        )
        .into(),
    ))
    .await
    .unwrap();
    condition(|| f.shared.source_pending.load(Ordering::Acquire) > pending).await;
    f.store
        .remove_workspace_member(&workspace, &guest)
        .await
        .unwrap();
    drop(held);
    let denied = response(&mut ws).await.unwrap();
    error_response(
        &denied,
        json!(2),
        intent_core::note_source_session::SessionError::NotFound,
    );
    drop(ws);
    let mut cleanup = ready_token(&f, Mode::Cleanup, token).await;
    let terminal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = rpc(
                &mut cleanup,
                json!(3),
                "note.sourceSession.close",
                serde_json::to_value(&op).unwrap(),
                0,
            )
            .await;
            if status["kind"] != "sourceSessionClosing" {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(terminal["kind"], "sourceSessionSettled");
    assert_eq!(
        cleanup_receipt(&f, &op).await["kind"],
        "sourceSessionUnknown"
    ); // other principal learns no history
    drop(cleanup);
    f.finish().await;
}

fn assert_auth_io_idle(f: &Fixture) {
    let observations = f.shared.auth_io.lock().unwrap();
    assert_eq!(
        observations.len(),
        1,
        "one exact final authorization interval"
    );
    let (read_before, read_after, write_before, write_after) = observations[0];
    assert_eq!(
        read_before, read_after,
        "authorization must not read arriving input"
    );
    assert_eq!(
        write_before, write_after,
        "authorization must not flush automatic output"
    );
}

#[tokio::test]
async fn source_lifecycle_final_control_auth_retains_writer_without_input_poll() {
    for cancel in [false, true] {
        let f = Fixture::new().await;
        let (_, op) = seed(
            &f,
            "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
            71,
        )
        .await;
        let mut ws = ready(&f, Mode::Read).await;
        rpc(
            &mut ws,
            json!(1),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0,
        )
        .await;
        f.shared.auth_io.lock().unwrap().clear();
        let mut held = Vec::new();
        for _ in 0..32 {
            held.push(f.store.read_pool().acquire().await.unwrap());
        }
        ws.send(Message::Text(
            raw_request(
                json!(2),
                "note.sourceSession.open",
                serde_json::to_value(&op).unwrap(),
                0,
            )
            .into(),
        ))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), f.shared.control_pending.notified())
            .await
            .unwrap();
        ws.send(Message::Ping(vec![7].into())).await.unwrap();
        if cancel {
            f.server.request_stop();
            assert!(response(&mut ws).await.is_none());
            condition(|| f.shared.root.counts().read == 1).await; // pending accept retired; original auth owner remains
            assert!(
                f.shared.auth_io.lock().unwrap().is_empty(),
                "exact auth continuation still pending"
            );
            assert!(f.shared.observe.lock().unwrap().is_empty());
        }
        drop(held);
        if !cancel {
            let result = response(&mut ws).await.unwrap();
            assert_eq!(result["id"], 2);
            assert_eq!(result["result"]["kind"], "sourceSessionAlreadyRegistered");
            assert!(response(&mut ws).await.is_none()); // Ping is observed only after owned control flush
        }
        condition(|| !f.shared.auth_io.lock().unwrap().is_empty()).await;
        assert_auth_io_idle(&f);
        drop(ws);
        if !cancel {
            assert_eq!(
                cleanup_receipt(&f, &op).await["kind"],
                "sourceSessionSettled"
            );
        }
        f.finish().await;
    }
}

#[tokio::test]
async fn source_lifecycle_final_page_auth_retains_exact_cancelled_continuation() {
    for cancel in [false, true] {
        let f = Fixture::new().await;
        let (binding, op) = seed(
            &f,
            "<div data-type=\"mermaid-block\" data-mermaid-code=\"graph TD\"></div>",
            72,
        )
        .await;
        let mut ws = ready(&f, Mode::Read).await;
        rpc(
            &mut ws,
            json!(1),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0,
        )
        .await;
        f.shared.auth_io.lock().unwrap().clear();
        let gate = Arc::new(AuthGate::default());
        *f.shared.page_auth_gate.lock().unwrap() = Some(gate.clone());
        ws.send(Message::Text(
            raw_request(
                json!(2),
                "note.sourceSession.read",
                owner_request(&op, &binding, 0),
                0,
            )
            .into(),
        ))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), gate.reached.notified())
            .await
            .unwrap();
        // The explicit test barrier is before credential revalidation; source read already returned.
        let mut held = Vec::new();
        for _ in 0..32 {
            held.push(f.store.read_pool().acquire().await.unwrap());
        }
        gate.release.notify_one();
        tokio::time::timeout(
            Duration::from_secs(5),
            f.shared.page_auth_pending.notified(),
        )
        .await
        .unwrap();
        ws.send(Message::Ping(vec![8].into())).await.unwrap();
        if cancel {
            f.server.request_stop();
            assert!(response(&mut ws).await.is_none());
            condition(|| f.shared.root.counts().read == 1).await;
            assert!(f.shared.auth_io.lock().unwrap().is_empty());
            assert!(f.shared.observe.lock().unwrap().is_empty());
        }
        drop(held);
        if !cancel {
            let result = response(&mut ws).await.unwrap();
            assert_eq!(result["id"], 2);
            assert_eq!(result["result"]["items"][0]["nodeType"], "mermaidBlock");
            assert!(result.get("error").is_none());
            assert!(response(&mut ws).await.is_none());
        }
        condition(|| !f.shared.auth_io.lock().unwrap().is_empty()).await;
        assert_auth_io_idle(&f);
        drop(ws);
        if !cancel {
            assert_eq!(
                cleanup_receipt(&f, &op).await["kind"],
                "sourceSessionSettled"
            );
        }
        f.finish().await;
    }
}

#[tokio::test]
async fn source_lifecycle_recovery_source_validation_rejects_mutation_and_recreation() {
    for recreate in [false, true] {
        let f = Fixture::new().await;
        let (binding, op) = seed(
            &f,
            "<div data-type=\"diff-block\" data-diff-code=\"+original\"></div>",
            81,
        )
        .await;
        let mut ws = ready(&f, Mode::Read).await;
        rpc(
            &mut ws,
            json!(1),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0,
        )
        .await;
        let gate = Arc::new(AuthGate::default());
        *f.shared.page_auth_gate.lock().unwrap() = Some(gate.clone());
        ws.send(Message::Text(
            raw_request(
                json!(2),
                "note.sourceSession.read",
                owner_request(&op, &binding, 0),
                0,
            )
            .into(),
        ))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), gate.reached.notified())
            .await
            .unwrap();
        let workspace = WorkspaceId::from("ws-1");
        let note = NoteId::from("note-1");
        let mut value = f.store.get_note(&workspace, &note).await.unwrap();
        if recreate {
            f.store.delete_note(&workspace, &note).await.unwrap();
            f.store.insert_note(&value).await.unwrap();
        } else {
            value.content =
                "<div data-type=\"diff-block\" data-diff-code=\"+replacement\"></div>".into();
            f.store.update_note(&value).await.unwrap();
        }
        gate.release.notify_one();
        error_response(
            &response(&mut ws).await.unwrap(),
            json!(2),
            intent_core::note_source_session::SessionError::Stale,
        );
        drop(ws);
        assert_eq!(
            cleanup_receipt(&f, &op).await["kind"],
            "sourceSessionSettled"
        );
        f.finish().await;
    }
}

#[tokio::test]
async fn source_lifecycle_recovery_discarded_replies_reconcile_only_by_original_operation() {
    for stage in ["open", "read", "close"] {
        let f = Fixture::new().await;
        let (binding, op) = seed(
            &f,
            "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
            82,
        )
        .await;
        let mut original = ready(&f, Mode::Read).await;
        original
            .send(Message::Text(
                raw_request(
                    json!(1),
                    "note.sourceSession.open",
                    serde_json::to_value(&op).unwrap(),
                    0,
                )
                .into(),
            ))
            .await
            .unwrap();
        // Explicit application-level response loss injection after actual WSS receipt;
        // this is not a dropped network packet or proof of remote consumption.
        let opened = response(&mut original).await.unwrap();
        assert_eq!(opened["id"], 1);
        assert_eq!(opened["result"]["kind"], "sourceSessionOpened");
        drop(opened);
        if stage != "open" {
            original
                .send(Message::Text(
                    raw_request(
                        json!(2),
                        "note.sourceSession.read",
                        owner_request(&op, &binding, 0),
                        0,
                    )
                    .into(),
                ))
                .await
                .unwrap();
            let page = response(&mut original).await.unwrap();
            assert_eq!(page["id"], 2);
            assert!(page.get("result").is_some());
            drop(page);
        }
        if stage == "close" {
            original
                .send(Message::Text(
                    raw_request(
                        json!(3),
                        "note.sourceSession.close",
                        serde_json::to_value(&op).unwrap(),
                        0,
                    )
                    .into(),
                ))
                .await
                .unwrap();
            let closing = response(&mut original).await.unwrap();
            assert_eq!(closing["id"], 3);
            assert_eq!(closing["result"]["kind"], "sourceSessionClosing");
            drop(closing);
            assert!(response(&mut original).await.is_none());
        }
        let mut replacement = ready(&f, Mode::Read).await;
        let duplicate = rpc(
            &mut replacement,
            json!(4),
            "note.sourceSession.open",
            serde_json::to_value(&op).unwrap(),
            0,
        )
        .await;
        assert_eq!(duplicate["kind"], "sourceSessionAlreadyRegistered");
        replacement
            .send(Message::Text(
                raw_request(
                    json!(5),
                    "note.sourceSession.read",
                    owner_request(&op, &binding, 0),
                    0,
                )
                .into(),
            ))
            .await
            .unwrap();
        error_response(
            &response(&mut replacement).await.unwrap(),
            json!(5),
            intent_core::note_source_session::SessionError::Unavailable,
        );
        drop(replacement);
        // Cleanup on a separately authenticated socket revokes the original even
        // while its idle parser or unconsumed delivery is still owned.
        let settled = cleanup_receipt(&f, &op).await;
        assert_eq!(settled["kind"], "sourceSessionSettled");
        assert_eq!(settled["reason"], "closed");
        if stage != "close" {
            assert!(response(&mut original).await.is_none());
        }
        drop(original);
        assert_eq!(cleanup_receipt(&f, &op).await, settled);
        f.finish().await;
    }
}

fn rebind_operation(mut operation: Operation, nonce: u64) -> Operation {
    operation.descriptor.nonce = format!("{nonce:032x}");
    operation.operation_id = intent_core::note_artifact::canonical::digest(
        &json!({"domain":"note.sourceSession.open.v1","descriptor":operation.descriptor})
            .to_string(),
    )
    .unwrap();
    operation
}

#[tokio::test]
async fn source_lifecycle_capacity_retains_cleanup_and_finite_cancellation_history() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let f = Fixture::new().await;
        let (_, base) = seed(
            &f,
            "<div data-type=\"diff-block\" data-diff-code=\"+x\"></div>",
            90,
        )
        .await;
        let first = rebind_operation(base.clone(), 0);
        let mut original = ready(&f, Mode::Read).await;
        rpc(
            &mut original,
            json!(1),
            "note.sourceSession.open",
            serde_json::to_value(&first).unwrap(),
            0,
        )
        .await;
        // One real live original plus239 settled histories exhaust the exact240 normal cells.
        for nonce in 1..240 {
            let op = rebind_operation(base.clone(), nonce);
            let mut ws = ready(&f, Mode::Read).await;
            rpc(
                &mut ws,
                json!(nonce),
                "note.sourceSession.open",
                serde_json::to_value(&op).unwrap(),
                0,
            )
            .await;
            drop(ws);
            assert_eq!(
                cleanup_receipt(&f, &op).await["kind"],
                "sourceSessionSettled"
            );
        }
        let over = rebind_operation(base.clone(), 1000);
        let mut ws = ready(&f, Mode::Read).await;
        ws.send(Message::Text(
            raw_request(
                json!(1000),
                "note.sourceSession.open",
                serde_json::to_value(&over).unwrap(),
                0,
            )
            .into(),
        ))
        .await
        .unwrap();
        error_response(
            &response(&mut ws).await.unwrap(),
            json!(1000),
            intent_core::note_source_session::SessionError::Capacity,
        );
        drop(ws);
        // Existing-owner cleanup requires no new history cell, including original transport retirement.
        let known = cleanup_receipt(&f, &first).await;
        assert_eq!(known["kind"], "sourceSessionSettled");
        assert_eq!(known["reason"], "closed");
        assert!(response(&mut original).await.is_none());
        drop(original);
        let mut cleanup = ready(&f, Mode::Cleanup).await;
        for nonce in 240..256 {
            let op = rebind_operation(base.clone(), nonce);
            let cancelled = rpc(
                &mut cleanup,
                json!(nonce),
                "note.sourceSession.close",
                serde_json::to_value(&op).unwrap(),
                0,
            )
            .await;
            assert_eq!(cancelled["kind"], "sourceSessionSettled");
            assert_eq!(cancelled["reason"], "cancelled");
        }
        cleanup
            .send(Message::Text(
                raw_request(
                    json!(1001),
                    "note.sourceSession.close",
                    serde_json::to_value(&over).unwrap(),
                    0,
                )
                .into(),
            ))
            .await
            .unwrap();
        error_response(
            &response(&mut cleanup).await.unwrap(),
            json!(1001),
            intent_core::note_source_session::SessionError::Capacity,
        );
        assert_eq!(
            rpc(
                &mut cleanup,
                json!(1002),
                "note.sourceSession.close",
                serde_json::to_value(&first).unwrap(),
                0
            )
            .await,
            known
        );
        drop(cleanup);
        f.finish().await;
    })
    .await
    .expect("bounded sequential real wire capacity witness");
}

#[tokio::test]
async fn source_lifecycle_capacity_unknown_incarnation_and_signed_deadline_refuse_allocation() {
    let f = Fixture::new().await;
    let (_, base) = seed(
        &f,
        "<div data-type=\"mermaid-block\" data-mermaid-code=\"graph TD\"></div>",
        91,
    )
    .await;
    let mut over = base.clone();
    over.descriptor.accept_until = (intent_core::parse_iso(&base.descriptor.accept_until).unwrap()
        + time::Duration::nanoseconds(1))
    .format(&time::format_description::well_known::Rfc3339)
    .unwrap();
    over = rebind_operation(over, 1);
    let mut expired = base.clone();
    expired.descriptor.accept_until = "2000-01-01T00:00:00Z".into();
    expired = rebind_operation(expired, 2);
    for op in [over, expired] {
        let mut ws = ready(&f, Mode::Read).await;
        ws.send(Message::Text(
            raw_request(
                json!(-1),
                "note.sourceSession.open",
                serde_json::to_value(&op).unwrap(),
                0,
            )
            .into(),
        ))
        .await
        .unwrap();
        error_response(
            &response(&mut ws).await.unwrap(),
            json!(-1),
            intent_core::note_source_session::SessionError::Expired,
        );
        drop(ws);
        assert_eq!(
            cleanup_receipt(&f, &op).await["kind"],
            "sourceSessionUnknown"
        );
    }
    let mut other = base.clone();
    other.descriptor.daemon_incarnation = "00000000-0000-4000-8000-000000000000".into();
    other = rebind_operation(other, 3);
    assert_eq!(
        cleanup_receipt(&f, &other).await["kind"],
        "sourceSessionUnknown"
    );
    // None of the refused opens allocated an owner for the original valid operation.
    let mut ws = ready(&f, Mode::Read).await;
    assert_eq!(
        rpc(
            &mut ws,
            json!(1),
            "note.sourceSession.open",
            serde_json::to_value(&base).unwrap(),
            0
        )
        .await["kind"],
        "sourceSessionOpened"
    );
    drop(ws);
    assert_eq!(
        cleanup_receipt(&f, &base).await["kind"],
        "sourceSessionSettled"
    );
    f.finish().await;
}
