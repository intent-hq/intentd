use super::*;
use crate::tests::setup;
use intent_core::{
    note_mutation::{NoteMutationError, NoteSplice},
    WorkspaceApi,
};
use serde_json::json;

async fn request(
    services: &Services,
    workspace: &WorkspaceId,
    note: &intent_core::NoteId,
    splices: Vec<NoteSplice>,
) -> NoteApplySplices {
    let state = services
        .store
        .read_note_page_state(workspace, note, None)
        .await
        .unwrap();
    let mut request = NoteApplySplices {
        backend_id: state["scope"]["backendId"].as_str().unwrap().into(),
        workspace_id: workspace.0.clone(),
        note_id: note.0.clone(),
        note_instance_id: state["scope"]["noteInstanceId"].as_str().unwrap().into(),
        base_revision: state["sourceRevision"].as_str().unwrap().into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        expires_at: format!("{}.000Z", &intent_core::iso_ms_from_now(60_000)[..19]),
        payload_digest: String::new(),
        splices,
    };
    request.payload_digest = request.computed_digest().unwrap();
    request
}

#[intent_test_macros::daemon_test]
async fn public_splices_exact_unicode_replay_and_atomic_receipt() {
    let (_tmp, services, workspace, note) = setup("same😀\r\nsame😀\r\n").await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 8,
            end: 12,
            text: "saved".into(),
        }],
    )
    .await;
    let receipt = services.note_apply_splices(input.clone()).await.unwrap();
    let changed = services.store.get_note(&workspace, &note).await.unwrap();
    assert_eq!(changed.content, "same😀\r\nsaved😀\r\n");
    assert_eq!(receipt["outcome"], "committed");
    let version_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_version")
        .fetch_one(services.store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        services.note_apply_splices(input.clone()).await.unwrap(),
        receipt
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_version")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        version_count
    );
    let mapping: String = sqlx::query_scalar(
        "SELECT value FROM note_operation_item WHERE kind='mapping' AND sequence=0",
    )
    .fetch_one(services.store.read_pool())
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&mapping).unwrap(),
        json!({"start":8,"end":12,"insertedLength":5})
    );
    let mut wrong = input;
    wrong.splices[0].text = "other".into();
    wrong.payload_digest = wrong.computed_digest().unwrap();
    assert!(matches!(
        services.note_apply_splices(wrong).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
}

#[intent_test_macros::daemon_test]
async fn public_splices_conversion_children_relations_and_provenance_commit_together() {
    let source="prefix😀\r\n@@@task key=a\n# A\nbody A\n@@@\n@@@task key=b dependsOn=a\n# B\nbody B\n@@@\ntail";
    let (_tmp, services, workspace, note) = setup(source).await;
    let bus = crate::EventBus::new(services.store.clone());
    let services = services.with_event_bus(bus);
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 0,
            text: "X".into(),
        }],
    )
    .await;
    let receipt = services.note_apply_splices(input.clone()).await.unwrap();
    let parent = services.store.get_note(&workspace, &note).await.unwrap();
    assert!(parent.content.starts_with("Xprefix😀\r\n"));
    assert!(parent.content.ends_with("tail"));
    assert!(!parent.content.contains("@@@task"));
    let children: Vec<_> = services
        .store
        .list_notes(&workspace)
        .await
        .unwrap()
        .into_iter()
        .filter(|n| n.parent_id.as_ref() == Some(&note))
        .collect();
    assert_eq!(children.len(), 2);
    let a = children.iter().find(|n| n.title == "A").unwrap();
    let b = children.iter().find(|n| n.title == "B").unwrap();
    assert_eq!(
        b.metadata.task.as_ref().unwrap().depends_on,
        vec![a.id.clone()]
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT converted_count FROM note_operation")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        2
    );
    let effects: Vec<String> = sqlx::query_scalar(
        "SELECT value FROM note_operation_item WHERE kind='effects' ORDER BY sequence",
    )
    .fetch_all(services.store.read_pool())
    .await
    .unwrap();
    assert!(effects.iter().any(|e| e.contains("sourceEffect")));
    let event_count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM event WHERE event_type IN ('note:created','task:created') OR json_extract(data_json,'$.triggeredBy.reason')='relations-changed'").fetch_one(services.store.read_pool()).await.unwrap();
    assert_eq!(event_count, 5);
    assert_eq!(services.note_apply_splices(input).await.unwrap(), receipt);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM event WHERE event_type IN ('note:created','task:created') OR json_extract(data_json,'$.triggeredBy.reason')='relations-changed'").fetch_one(services.store.read_pool()).await.unwrap(),event_count);
    assert_eq!(
        services.store.list_notes(&workspace).await.unwrap().len(),
        3
    );
}

#[intent_test_macros::daemon_test]
async fn public_splices_conversion_sql_failures_roll_back_children_versions_relations() {
    for trigger in [
        "CREATE TRIGGER fail_conversion BEFORE INSERT ON note WHEN NEW.parent_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'child injected'); END",
        "CREATE TRIGGER fail_conversion BEFORE INSERT ON note_version WHEN NEW.note_id != 'n1' BEGIN SELECT RAISE(ABORT,'child version injected'); END",
        "CREATE TRIGGER fail_conversion BEFORE UPDATE OF task_json ON note WHEN NEW.parent_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'relation injected'); END",
        "CREATE TRIGGER fail_conversion BEFORE INSERT ON note_version WHEN NEW.note_id = 'n1' AND (SELECT COUNT(*) FROM note_version WHERE note_id='n1') > 0 BEGIN SELECT RAISE(ABORT,'conversion version injected'); END",
    ] {
        let phantom="00000000-0000-4000-8000-000000000001";
        let source=format!("prefix<!--anchor:{phantom}:point-->\n@@@task key=a\n# A\nbody\n@@@\n@@@task key=b dependsOn=a\n# B\nbody\n@@@\n");
        let (_tmp,services,workspace,note)=setup(&source).await;
        sqlx::query(trigger).execute(services.store.write_pool()).await.unwrap();
        let input=request(&services,&workspace,&note,vec![NoteSplice{start:0,end:0,text:"X".into()}]).await;
        let receipt=services.note_apply_splices(input.clone()).await.unwrap();
        let parent=services.store.get_note(&workspace,&note).await.unwrap();
        assert_eq!(parent.content,format!("X{}",source.replace(&format!("<!--anchor:{phantom}:point-->"),"")),"{trigger}");
        assert_eq!(services.store.list_notes(&workspace).await.unwrap().len(),1);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM note_version").fetch_one(services.store.read_pool()).await.unwrap(),1);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT converted_count FROM note_operation").fetch_one(services.store.read_pool()).await.unwrap(),0);
        let effects:Vec<String>=sqlx::query_scalar("SELECT value FROM note_operation_item WHERE kind='effects'").fetch_all(services.store.read_pool()).await.unwrap();
        assert!(effects.iter().any(|e|e.contains("phantom-scrub")));
        assert!(!effects.iter().any(|e|e.contains("createdTask")||e.contains("task-marker-projection")));
        assert_eq!(services.note_apply_splices(input).await.unwrap(),receipt);
    }
}

#[intent_test_macros::daemon_test]
async fn public_splices_fatal_initial_version_failure_rolls_back_everything() {
    let (_tmp, services, workspace, note) = setup("base").await;
    sqlx::query("CREATE TRIGGER fail_initial BEFORE INSERT ON note_version BEGIN SELECT RAISE(ABORT,'version injected'); END").execute(services.store.write_pool()).await.unwrap();
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 4,
            text: "changed".into(),
        }],
    )
    .await;
    assert!(matches!(
        services.note_apply_splices(input).await,
        Err(Error::Internal(_))
    ));
    assert_eq!(
        services
            .store
            .get_note(&workspace, &note)
            .await
            .unwrap()
            .content,
        "base"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_operation")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        0
    );
}

#[intent_test_macros::daemon_test]
async fn public_splices_publish_final_live_anchors_and_preserve_legacy_point_disposition() {
    for rollback in [false, true] {
        let a = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let b = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
        let point = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
        let source=format!("😀\n@@@task\n# Child\nbody\n@@@\n<!--anchor:{a}:start-->one😀<!--anchor:{b}:start-->two<!--anchor:{a}:end-->three<!--anchor:{b}:end-->\n<!--anchor:{a}:start-->again<!--anchor:{a}:end--><!--anchor:{a}:point--><!--anchor:{point}:point-->");
        let (_tmp, services, workspace, note) = setup(&source).await;
        for id in [a, b, point] {
            let comment:Comment=serde_json::from_value(json!({"id":id,"threadId":id,"noteId":note,"type":"comment","content":"body","author":"User","authorType":"user","status":"open","createdAt":intent_core::now_iso(),"updatedAt":intent_core::now_iso()})).unwrap();
            services
                .store
                .insert_comment(&workspace, &comment)
                .await
                .unwrap();
        }
        if rollback {
            sqlx::query("CREATE TRIGGER fail_anchor_conversion BEFORE INSERT ON note WHEN NEW.parent_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'child injected'); END").execute(services.store.write_pool()).await.unwrap();
        }
        let input = request(
            &services,
            &workspace,
            &note,
            vec![NoteSplice {
                start: 0,
                end: 0,
                text: "prefix\n".into(),
            }],
        )
        .await;
        let receipt = services.note_apply_splices(input).await.unwrap();
        let final_note = services.store.get_note(&workspace, &note).await.unwrap();
        assert!(
            !final_note
                .content
                .contains(&format!("<!--anchor:{point}:point-->")),
            "point-only legacy root is orphaned and scrubbed"
        );
        assert!(
            final_note
                .content
                .contains(&format!("<!--anchor:{a}:point-->")),
            "a point occurrence on an otherwise healthy range root survives"
        );
        assert_eq!(final_note.content.contains("@@@task"), rollback);
        let epochs = services
            .store
            .note_annotation_epochs(&workspace, &note)
            .await
            .unwrap();
        assert!(epochs.anchors_ready);
        let state = services
            .store
            .read_note_page_state(&workspace, &note, None)
            .await
            .unwrap();
        assert_eq!(state["sourceRevision"], receipt["afterRevision"]);
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT comment_id,start,end FROM note_comment_anchor ORDER BY comment_id,start",
        )
        .fetch_all(services.store.read_pool())
        .await
        .unwrap();
        let mut expected = Vec::new();
        for id in [a, b] {
            let opening = format!("<!--anchor:{id}:start-->");
            let closing = format!("<!--anchor:{id}:end-->");
            for (at, _) in final_note.content.match_indices(&opening) {
                let start = at + opening.len();
                let end = start + final_note.content[start..].find(&closing).unwrap();
                expected.push((
                    id.to_string(),
                    i64::try_from(final_note.content[..start].encode_utf16().count()).unwrap(),
                    i64::try_from(final_note.content[..end].encode_utf16().count()).unwrap(),
                ));
            }
        }
        let at = final_note
            .content
            .find(&format!("<!--anchor:{a}:point-->"))
            .unwrap();
        let position = i64::try_from(final_note.content[..at].encode_utf16().count()).unwrap();
        expected.push((a.into(), position, position));
        expected.sort();
        assert_eq!(rows, expected);
        let page: intent_core::note_annotation::AnnotationReadRequest=serde_json::from_value(json!({"backendId":receipt["scope"]["backendId"],"workspaceId":workspace,"noteId":note,"noteInstanceId":receipt["scope"]["noteInstanceId"],"sourceRevision":receipt["afterRevision"],"commentRevision":state["commentRevision"],"page":{"kind":"comments","ranges":[],"anchorState":"all","maxWireBytes":4096}})).unwrap();
        let result = services
            .get_note_annotation_page(
                intent_core::note_annotation::AnnotationMethod::Comments,
                page,
                json!(1),
            )
            .await
            .unwrap();
        assert_eq!(result["items"].as_array().unwrap().len(), 3);
        assert_eq!(result["sourceRevision"], receipt["afterRevision"]);
        assert_eq!(result["commentRevision"], state["commentRevision"]);
        assert!(
            json!({"jsonrpc":"2.0","id":1,"result":result})
                .to_string()
                .len()
                <= 4096
        );
    }
}

#[tokio::test]
async fn public_splices_current_authority_controls_replay_after_deletion() {
    use intent_core::{with_caller, HostRole, Principal, PrincipalId, WorkspaceRole};
    let (_tmp, services, workspace, note) = setup("base").await;
    let principal = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    services.store.upsert_principal(&principal).await.unwrap();
    let caller = Caller::Wire {
        principal_id: principal.id.clone(),
        host_role: HostRole::Guest,
    };
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 4,
            text: "saved".into(),
        }],
    )
    .await;
    assert!(matches!(
        with_caller(caller.clone(), services.note_apply_splices(input.clone())).await,
        Err(Error::NotFound(_))
    ));
    services
        .store
        .add_workspace_member(&workspace, &principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let receipt = with_caller(caller.clone(), services.note_apply_splices(input.clone()))
        .await
        .unwrap();
    services.store.delete_note(&workspace, &note).await.unwrap();
    assert_eq!(
        with_caller(caller.clone(), services.note_apply_splices(input.clone()))
            .await
            .unwrap(),
        receipt
    );
    services
        .store
        .remove_workspace_member(&workspace, &principal.id)
        .await
        .unwrap();
    assert!(matches!(
        with_caller(caller, services.note_apply_splices(input)).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn public_splices_requires_a_caller() {
    if crate::capability::reran_unarmed("note_splice::tests::public_splices_requires_a_caller") {
        return;
    }
    let (_tmp, services, workspace, note) = setup("base").await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 4,
            text: "saved".into(),
        }],
    )
    .await;
    assert!(matches!(
        services.note_apply_splices(input).await,
        Err(Error::Forbidden(_))
    ));
}

#[intent_test_macros::daemon_test]
async fn public_splices_effect_states_replay_actual_coordinates_and_utf8_digests() {
    let phantom = "00000000-0000-4000-8000-000000000001";
    let source =
        format!("same😀\r\n<!--anchor:{phantom}:point-->\n@@@task\n# A\nbody\n@@@\nsame😀");
    let (_tmp, services, workspace, note) = setup(&source).await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 4,
            text: "same!".into(),
        }],
    )
    .await;
    services.note_apply_splices(input).await.unwrap();
    let records: Vec<String> = sqlx::query_scalar(
        "SELECT value FROM note_operation_item WHERE kind='effects' ORDER BY sequence",
    )
    .fetch_all(services.store.read_pool())
    .await
    .unwrap();
    let mut phases = std::collections::BTreeMap::<String, Vec<Value>>::new();
    for record in records {
        let item: Value = serde_json::from_str(&record).unwrap();
        if item["kind"] == "sourceEffect" {
            phases
                .entry(item["inputState"].as_str().unwrap().into())
                .or_default()
                .push(item);
        }
    }
    assert!(phases.len() >= 2);
    for (input, items) in phases {
        let chunks: Vec<String> = sqlx::query_scalar(
            "SELECT text FROM note_operation_source WHERE phase=? ORDER BY start",
        )
        .bind(&input)
        .fetch_all(services.store.read_pool())
        .await
        .unwrap();
        let original = chunks.concat();
        let output = items[0]["outputState"].as_str().unwrap();
        let chunks: Vec<String> = sqlx::query_scalar(
            "SELECT text FROM note_operation_source WHERE phase=? ORDER BY start",
        )
        .bind(output)
        .fetch_all(services.store.read_pool())
        .await
        .unwrap();
        let result = chunks.concat();
        let input16: Vec<u16> = original.encode_utf16().collect();
        let output16: Vec<u16> = result.encode_utf16().collect();
        let mut prior = 0usize;
        let mut output_prior = 0usize;
        for item in items {
            let start = usize::try_from(item["range"]["start"].as_u64().unwrap()).unwrap();
            let end = usize::try_from(item["range"]["end"].as_u64().unwrap()).unwrap();
            let inserted = usize::try_from(item["insertedLength"].as_u64().unwrap()).unwrap();
            assert_eq!(
                &input16[prior..start],
                &output16[output_prior..output_prior + start - prior]
            );
            output_prior += start - prior;
            let removed = String::from_utf16(&input16[start..end]).unwrap();
            let added =
                String::from_utf16(&output16[output_prior..output_prior + inserted]).unwrap();
            assert_eq!(
                item["beforeDigest"],
                crate::attachment_upload::sha256_hex(removed.as_bytes())
            );
            assert_eq!(
                item["afterDigest"],
                crate::attachment_upload::sha256_hex(added.as_bytes())
            );
            prior = end;
            output_prior += inserted;
        }
        assert_eq!(&input16[prior..], &output16[output_prior..]);
    }
}

#[intent_test_macros::daemon_test]
async fn public_splices_receipt_inverse_restores_canonical_result_with_bounded_reads() {
    use intent_core::note_receipt_detail::NoteOperationReceiptRead;
    let source = format!(
        "{}\n@@@task key=a\n# A\nbody😀\n@@@\n",
        "😀\r\n".repeat(5000)
    );
    let (_tmp, services, workspace, note) = setup(&source).await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 4000,
            text: "X".into(),
        }],
    )
    .await;
    let receipt = services.note_apply_splices(input.clone()).await.unwrap();
    let after = services
        .store
        .get_note(&workspace, &note)
        .await
        .unwrap()
        .content;
    let base = json!({"backendId":input.backend_id,"workspaceId":input.workspace_id,"noteId":input.note_id,
        "noteInstanceId":input.note_instance_id,"operationId":input.operation_id,"payloadDigest":input.payload_digest,
        "maxItems":1,"maxWireBytes":4096,"maxSourceBytes":1024});
    let mut inverse_request = base.clone();
    inverse_request["kind"] = json!("inverse");
    inverse_request["ref"] = receipt["inverseRef"].clone();
    let mut inverse = Vec::new();
    loop {
        let query = serde_json::from_value::<NoteOperationReceiptRead>(inverse_request.clone())
            .unwrap()
            .query()
            .unwrap();
        let page = services
            .get_note_receipt_detail(query, json!("inverse"))
            .await
            .unwrap();
        assert!(
            json!({"jsonrpc":"2.0","id":"inverse","result":page})
                .to_string()
                .len()
                <= 4096
        );
        inverse.extend(page["items"].as_array().unwrap().iter().cloned());
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        inverse_request["cursor"] = json!(next);
    }
    assert!(!inverse.is_empty());
    let mut edits = Vec::new();
    for item in inverse {
        assert_eq!(item["historyGroup"], "0");
        assert_eq!(item["inputState"], receipt["afterRevision"]);
        assert_eq!(item["outputState"], receipt["beforeRevision"]);
        let mut text_request = base.clone();
        text_request["kind"] = json!("inverseText");
        text_request["ref"] = receipt["inverseRef"].clone();
        text_request["textId"] = item["replacement"]["textId"].clone();
        text_request["offset"] = json!(0);
        let mut replacement = String::new();
        loop {
            let query = serde_json::from_value::<NoteOperationReceiptRead>(text_request.clone())
                .unwrap()
                .query()
                .unwrap();
            let page = services
                .get_note_receipt_detail(query, json!("text"))
                .await
                .unwrap();
            assert!(
                json!({"jsonrpc":"2.0","id":"text","result":page})
                    .to_string()
                    .len()
                    <= 4096
            );
            for fragment in page["items"].as_array().unwrap() {
                assert_eq!(
                    fragment["offset"].as_u64().unwrap(),
                    replacement.encode_utf16().count() as u64
                );
                let text = fragment["text"].as_str().unwrap();
                assert!(text.len() <= 1024);
                replacement.push_str(text);
            }
            let Some(next) = page["nextCursor"].as_str() else {
                break;
            };
            text_request.as_object_mut().unwrap().remove("offset");
            text_request["cursor"] = json!(next);
        }
        assert_eq!(
            item["replacement"]["sha256"],
            crate::attachment_upload::sha256_hex(replacement.as_bytes())
        );
        let mut detail = base.clone();
        detail["kind"] = json!("detail");
        detail["ref"] = item["provenanceRef"].clone();
        let query = serde_json::from_value::<NoteOperationReceiptRead>(detail)
            .unwrap()
            .query()
            .unwrap();
        let page = services
            .get_note_receipt_detail(query, json!("detail"))
            .await
            .unwrap();
        assert_eq!(page["items"][0]["type"], "object");
        edits.push((
            item["start"].as_u64().unwrap(),
            item["end"].as_u64().unwrap(),
            replacement,
        ));
    }
    fn byte_at(text: &str, offset: u64) -> usize {
        let mut units = 0;
        for (byte, ch) in text.char_indices() {
            if units == offset {
                return byte;
            }
            units += ch.len_utf16() as u64;
        }
        assert_eq!(units, offset);
        text.len()
    }
    let mut restored = after;
    for (start, end, replacement) in edits.into_iter().rev() {
        restored.replace_range(
            byte_at(&restored, start)..byte_at(&restored, end),
            &replacement,
        );
    }
    assert_eq!(restored, source);
}

#[intent_test_macros::daemon_test]
async fn public_splices_canonical_effect_details_stream_exact_escaped_unicode_fields() {
    use intent_core::note_receipt_detail::NoteOperationReceiptRead;
    let body = "line😀\"\\\r\n".repeat(600);
    let source = format!("@@@task key=a\n# A\n{body}\n@@@\n");
    let (_tmp, services, workspace, note) = setup(&source).await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: source.encode_utf16().count() as u64,
            end: source.encode_utf16().count() as u64,
            text: "tail".into(),
        }],
    )
    .await;
    let receipt = services.note_apply_splices(input.clone()).await.unwrap();
    let mut params = json!({"backendId":input.backend_id,"workspaceId":input.workspace_id,"noteId":input.note_id,
        "noteInstanceId":input.note_instance_id,"operationId":input.operation_id,"payloadDigest":input.payload_digest,
        "kind":"effects","ref":receipt["effectsRef"],"maxItems":128,"maxWireBytes":4096,"maxSourceBytes":64});
    let mut effect = None;
    loop {
        let query = serde_json::from_value::<NoteOperationReceiptRead>(params.clone())
            .unwrap()
            .query()
            .unwrap();
        let page = services
            .get_note_receipt_detail(query, json!("effect"))
            .await
            .unwrap();
        for item in page["items"].as_array().unwrap() {
            if item["kind"] == "sourceEffect" && item["reason"] == "task-conversion" {
                effect = Some(item.clone());
            }
        }
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        params["cursor"] = json!(next);
    }
    let effect = effect.unwrap();
    params.as_object_mut().unwrap().remove("cursor");
    params["kind"] = json!("detail");
    params["ref"] = effect["detailRef"].clone();
    let query = serde_json::from_value::<NoteOperationReceiptRead>(params.clone())
        .unwrap()
        .query()
        .unwrap();
    let root = services
        .get_note_receipt_detail(query, json!("root"))
        .await
        .unwrap();
    params["ref"] = root["items"][0]["childrenRef"].clone();
    let query = serde_json::from_value::<NoteOperationReceiptRead>(params.clone())
        .unwrap()
        .query()
        .unwrap();
    let fields = services
        .get_note_receipt_detail(query, json!("fields"))
        .await
        .unwrap();
    let removed = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["key"] == "removed")
        .unwrap();
    assert!(removed.get("value").is_none());
    params["ref"] = removed["valueRef"].clone();
    let mut text = String::new();
    loop {
        let query = serde_json::from_value::<NoteOperationReceiptRead>(params.clone())
            .unwrap()
            .query()
            .unwrap();
        let page = services
            .get_note_receipt_detail(query, json!("scalar"))
            .await
            .unwrap();
        assert!(
            json!({"jsonrpc":"2.0","id":"scalar","result":page})
                .to_string()
                .len()
                <= 4096
        );
        let fragment = &page["items"][0];
        assert_eq!(
            fragment["offset"].as_u64().unwrap(),
            text.encode_utf16().count() as u64
        );
        let part = fragment["text"].as_str().unwrap();
        assert!(part.len() <= 64);
        text.push_str(part);
        assert!(page["nextCursor"].is_null());
        let Some(next) = fragment["nextRef"].as_str() else {
            break;
        };
        params["ref"] = json!(next);
    }
    assert!(text.contains(&body));
    assert_eq!(
        crate::attachment_upload::sha256_hex(text.as_bytes()),
        effect["beforeDigest"].as_str().unwrap()
    );
}

#[intent_test_macros::daemon_test]
async fn public_splices_reject_numbered_replacements_atomically_without_scanning_untouched_source()
{
    let (_tmp, services, workspace, note) = setup("left right").await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![
            NoteSplice {
                start: 0,
                end: 4,
                text: "valid".into(),
            },
            NoteSplice {
                start: 5,
                end: 10,
                text: "   1 | # title\n   2 | body".into(),
            },
        ],
    )
    .await;
    assert!(matches!(
        services.note_apply_splices(input).await,
        Err(Error::InvalidParams(_))
    ));
    assert_eq!(
        services
            .store
            .get_note(&workspace, &note)
            .await
            .unwrap()
            .content,
        "left right"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_version")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_operation")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        0
    );
    let old = "   1 | historical\n   2 | text";
    let (_tmp, services, workspace, note) = setup(old).await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 0,
            text: "repair\n".into(),
        }],
    )
    .await;
    services.note_apply_splices(input).await.unwrap();
    assert_eq!(
        services
            .store
            .get_note(&workspace, &note)
            .await
            .unwrap()
            .content,
        format!("repair\n{old}")
    );
}

#[intent_test_macros::daemon_test]
async fn public_splices_numbered_guard_preserves_historical_exact_replay() {
    let (_tmp, services, workspace, note) = setup("base").await;
    let input = request(
        &services,
        &workspace,
        &note,
        vec![NoteSplice {
            start: 0,
            end: 4,
            text: "   1 | historical\n   2 | receipt".into(),
        }],
    )
    .await;
    let author = crate::resolve_note_version_author(&services.store, None).await;
    let NoteMutationAdmission::Write(mut write) = services
        .store
        .begin_note_mutation("daemon", input.clone(), &intent_core::now_iso())
        .await
        .unwrap()
    else {
        panic!("new operation")
    };
    write
        .persist_source(&author, &intent_core::now_iso())
        .await
        .unwrap();
    let receipt = write.commit().await.unwrap();
    assert_eq!(services.note_apply_splices(input).await.unwrap(), receipt);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_version")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        1
    );
}
