//! Actual `SQLite` rollback callback controls, not a mocked Store future.
use super::*;
use crate::CanonicalSourceBinding;
use intent_core::note_artifact::request::Primitive;

struct Closed(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for Closed {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn bindings(store: &Store) -> Vec<CanonicalSourceBinding> {
    let first = page(
        store,
        json!({"kind":"source","maxSourceBytes":128,"maxWireBytes":8192}),
    )
    .await;
    let context=page(store,json!({"kind":"context","contextRef":first["contextRef"],"maxItems":128,"maxWireBytes":8192})).await;
    let mut result = Vec::new();
    for owner in context["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["nodeType"] == "diffBlock" || v["nodeType"] == "mermaidBlock")
    {
        let root = page(
            store,
            json!({"kind":"metadata","ref":owner["attributesRef"],"maxWireBytes":8192}),
        )
        .await;
        let fields = page(
            store,
            json!({"kind":"metadata","ref":root["items"][0]["childrenRef"],"maxWireBytes":8192}),
        )
        .await;
        let code = fields["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["key"] == "code")
            .unwrap();
        result.push(CanonicalSourceBinding {
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
        });
    }
    result
}

async fn binding(store: &Store) -> CanonicalSourceBinding {
    bindings(store).await.remove(0)
}

#[tokio::test]
async fn canonical_source_errors_wait_for_actual_sqlite_rollback() {
    let mut premature = Vec::new();
    for authorization in [true, false] {
        let (mut store, tmp, mut note) = setup("```diff\n+one\n```").await;
        let binding = binding(&store).await;
        note.content = "```diff\n+changed\n```".into();
        store.update_note(&note).await.unwrap();
        store.read_pool.close().await;
        store.read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&tmp.path)
                    .read_only(true),
            )
            .await
            .unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let timed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut connection = store.read_pool.acquire().await.unwrap();
            let observed = entered.clone();
            let gate = release.clone();
            let timeout = timed_out.clone();
            let retirement = Closed(closed.clone());
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_rollback_hook(move || {
                    let _retained_until_connection_close = &retirement;
                    observed.notify_one();
                    let (lock, condition) = &*gate;
                    let (_held, wait) = condition
                        .wait_timeout_while(
                            lock.lock().unwrap(),
                            std::time::Duration::from_secs(5),
                            |released| !*released,
                        )
                        .unwrap();
                    timeout.store(wait.timed_out(), Ordering::SeqCst);
                });
        }
        let owned = store.clone();
        let operation = tokio::spawn(async move {
            if authorization {
                owned
                    .authorize_canonical_source("pages", "alice", &binding)
                    .await
                    .map(|_| json!("authorized"))
            } else {
                owned.read_note_page_settled("pages","spec","alice",request(json!({"kind":"context","contextRef":binding.owner_ref,"maxWireBytes":8192})),&json!(1)).await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .expect("actual SQLite rollback hook never ran");
        // SQLite is inside the real rollback hook and cannot ACK ROLLBACK yet.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        premature.push((authorization, operation.is_finished()));
        *release.0.lock().unwrap() = true;
        release.1.notify_one();
        let result = operation.await.unwrap();
        assert!(matches!(result, Err(Error::NotePage(NotePageError::Stale))));
        assert!(
            !timed_out.load(Ordering::SeqCst),
            "rollback gate expired before explicit release"
        );
        assert!(
            closed.load(Ordering::SeqCst),
            "source connection callback remains alive after returned outcome"
        );
        let mut conn = store.read_pool.acquire().await.unwrap();
        conn.lock_handle().await.unwrap().remove_rollback_hook();
        drop(conn);
        store.close().await;
    }
    assert!(
        premature.iter().all(|(_, returned)| !returned),
        "source operations returned before actual rollback ACK: {premature:?}"
    );
}

#[tokio::test]
async fn canonical_source_hold_rejects_non_direct_owner_value_references() {
    let (store, _tmp, _note) = setup("```diff\n+one\n```").await;
    let mut value = binding(&store).await;
    value.source_ref = value.owner_ref.clone();
    assert!(
        store
            .hold_canonical_source("pages", "alice", &value)
            .is_err(),
        "native owner reference accepted as a direct code-value hold"
    );
    store.close().await;
}

#[tokio::test]
async fn canonical_source_exact_binding_rejects_same_snapshot_substitution() {
    let (store, _tmp, _) = setup("```diff\n+one\n```\n\n```diff\n+two\n```").await;
    let values = bindings(&store).await;
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].snapshot_id, values[1].snapshot_id);
    assert_ne!(values[0].source_ref, values[1].source_ref);
    for value in &values {
        store
            .authorize_canonical_source("pages", "alice", value)
            .await
            .unwrap();
    }
    let mut invalid = Vec::new();
    let mut swapped = values[0].clone();
    swapped.source_ref.clone_from(&values[1].source_ref);
    // A valid signed retention hold does not establish owner-to-code authority.
    store
        .hold_canonical_source("pages", "alice", &swapped)
        .unwrap();
    invalid.push(swapped);
    let mut primitive = values[0].clone();
    primitive.primitive = Primitive::Mermaid;
    invalid.push(primitive);
    let mut revision = values[0].clone();
    revision.source_revision.push('x');
    invalid.push(revision);
    let mut incarnation = values[0].clone();
    incarnation.scope.note_instance_id.push('x');
    invalid.push(incarnation);
    for value in invalid {
        assert!(store
            .authorize_canonical_source("pages", "alice", &value)
            .await
            .is_err());
    }
    for (workspace, principal) in [("pages", "bob"), ("foreign", "alice")] {
        assert!(store
            .authorize_canonical_source(workspace, principal, &values[0])
            .await
            .is_err());
    }
    store.close().await;
}

#[tokio::test]
async fn canonical_source_extraction_preserves_full_artifact_header_validation() {
    use intent_core::note_artifact::request::ArtifactHeader;
    let (store, _tmp, _) = setup("```diff\n+one\n```").await;
    let value = binding(&store).await;
    let header = json!({
        "scope":value.scope,"source":{"kind":"snapshot","snapshotId":value.snapshot_id,"sourceRevision":value.source_revision,"ownerRef":value.owner_ref,"sourceRef":value.source_ref},
        "primitive":"diff","profile":"test-only-unregistered","environment":{"width":800,"height":600,"theme":"light","fontRef":"test-font","fontSize":14,"devicePixelRatio":1},
        "reservation":{"payloadBytes":4096,"records":10,"indexEntries":10,"storageChargeBytes":65536}
    });
    let valid: ArtifactHeader = serde_json::from_value(header.clone()).unwrap();
    store
        .authorize_note_artifact_source("pages", "alice", &valid)
        .await
        .unwrap();
    for field in ["profile", "reservation"] {
        let mut malformed = header.clone();
        if field == "profile" {
            malformed["profile"] = json!("");
        } else {
            malformed["reservation"]["records"] = json!(0);
        }
        let malformed: ArtifactHeader = serde_json::from_value(malformed).unwrap();
        assert!(store
            .authorize_note_artifact_source("pages", "alice", &malformed)
            .await
            .is_err());
        store
            .authorize_canonical_source("pages", "alice", &value)
            .await
            .unwrap();
    }
    // This validates source authorization only, never renderer/profile admission.
    store.close().await;
}

#[tokio::test]
async fn canonical_source_settled_reads_bound_actual_sql_work_per_fragment() {
    let mut costs = Vec::new();
    for bytes in [32_768, 2_000_000] {
        let started = std::time::Instant::now();
        let (mut store, tmp, _) = setup(&format!(
            "<div data-type=\"diff-block\" data-diff-code=\"{}\"></div>",
            "x".repeat(bytes)
        ))
        .await;
        let construction_ms = started.elapsed().as_millis();
        // binding() obtains only 128 source bytes, then the indexed owner/code refs.
        let value = binding(&store).await;
        let steps = Arc::new(AtomicUsize::new(0));
        store.read_pool.close().await;
        let counter = steps.clone();
        store.read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(move |connection, _| {
                let counter = counter.clone();
                Box::pin(async move {
                    connection
                        .lock_handle()
                        .await?
                        .set_progress_handler(1, move || {
                            counter.fetch_add(1, Ordering::Relaxed);
                            true
                        });
                    Ok(())
                })
            })
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&tmp.path)
                    .read_only(true),
            )
            .await
            .unwrap();
        steps.store(0, Ordering::Relaxed);
        store
            .authorize_canonical_source("pages", "alice", &value)
            .await
            .unwrap();
        let authorize_steps = steps.swap(0, Ordering::Relaxed);
        let mut reference = value.source_ref;
        let mut total = 0;
        let mut counts = Vec::new();
        loop {
            let result = store.read_note_page_settled("pages", "spec", "alice",
                request(json!({"kind":"context","contextRef":reference,"maxItems":1,"maxWireBytes":8192})), &json!("measured-source")).await.unwrap();
            counts.push(steps.swap(0, Ordering::Relaxed));
            let item = &result["items"][0];
            let fragment = item["text"].as_str().unwrap();
            assert!(fragment.bytes().all(|byte| byte == b'x'));
            assert_eq!(item["offset"], total);
            assert!(fragment.len() <= 8192);
            total += fragment.len();
            assert!(
                json!({"jsonrpc":"2.0","id":"measured-source","result":result})
                    .to_string()
                    .len()
                    <= 8192
            );
            if item["nextRef"].is_null() {
                break;
            }
            reference = item["nextRef"].as_str().unwrap().into();
        }
        assert_eq!(total, bytes);
        let max_steps = *counts.iter().max().unwrap();
        costs.push(json!({"sourceBytes":bytes,"constructionMs":construction_ms,"authorizeVmSteps":authorize_steps,"requests":counts.len(),"firstVmSteps":counts[0],"lastVmSteps":counts.last(),"maxVmSteps":max_steps}));
        store.close().await;
    }
    assert!(
        costs[1]["maxVmSteps"].as_u64().unwrap() <= costs[0]["maxVmSteps"].as_u64().unwrap() + 200,
        "{costs:?}"
    );
    let evidence = json!({"costs":costs,"scope":"Actual indexed source authorization and sequential returned nextRef requests; fresh connection progress handler on every settled read. Connection bootstrap before after_connect and initial index construction are excluded from per-request VM counts; counts are not heap/FS/latency proof."});
    eprintln!("canonical source SQL work: {evidence}");
    if let Ok(path) = std::env::var("CANONICAL_SOURCE_COSTS") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
}
