use super::*;

#[tokio::test]
async fn indexed_plain_markdown_live_fixture_exposes_canonical_text_maps() {
    // Exact source used by the real-daemon 600000-byte browser regression.
    let header = "# Size boundary note\n\nUnicode café 漢字 🙂 and canonical-search-needle.\n\n";
    let row = "Bounded source line: café 漢字 🙂 remains exact.\n";
    let remaining = 600_000 - header.len();
    let source = format!(
        "{header}{}{}",
        row.repeat(remaining / row.len()),
        "x".repeat(remaining % row.len())
    );
    assert_eq!(source.len(), 600_000);
    let (store, _tmp, _) = setup(&source).await;
    let first = page(
        &store,
        json!({"kind":"source","at":0,"maxSourceBytes":1024,"maxWireBytes":8192,"maxItems":64}),
    )
    .await;
    let context = page(&store, json!({"kind":"context","contextRef":first["contextRef"],"maxWireBytes":8192,"maxItems":64})).await;
    let owners: Vec<_> = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["construct"] == "markdownBlock")
        .collect();
    assert!(
        owners.len() >= 3,
        "ordinary heading and both paragraphs need canonical map owners"
    );
    for owner in owners {
        assert!(owner["nativeRef"].is_string());
        assert!(owner["sourceMapRef"].is_string());
        let maps = page(&store, json!({"kind":"context","contextRef":owner["sourceMapRef"],"maxWireBytes":8192,"maxItems":64})).await;
        assert!(maps["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["kind"] == "sourceMap" && item["textRef"].is_string()));
    }
}

async fn canonical_window_text(
    store: &Store,
    source_request: Value,
) -> (Value, String, Vec<Value>) {
    let mut calls = Vec::new();
    let source = record_source_window_closure(store, source_request, &mut calls).await;
    let mut maps = std::collections::BTreeMap::new();
    let mut natives = std::collections::BTreeMap::new();
    for call in &calls {
        for item in call["response"]["items"].as_array().into_iter().flatten() {
            if item["kind"] == "nativeNode" {
                natives.insert(item["id"].to_string(), item.clone());
            }
            if item["kind"] == "sourceMap" && item["textRef"].is_string() {
                maps.insert(item["id"].to_string(), item.clone());
            }
        }
    }
    let mut texts = std::collections::BTreeMap::new();
    for map in maps.values() {
        let text = page(
            store,
            json!({"kind":"context","contextRef":map["textRef"],"maxWireBytes":8192}),
        )
        .await;
        let value = text["items"][0]["text"].as_str().unwrap();
        let key = (
            map["sourceRange"]["start"].as_u64().unwrap(),
            map["textNodeId"].to_string(),
            map["renderedRange"]["start"].as_u64().unwrap(),
        );
        assert!(
            texts.insert(key, value.to_owned()).is_none(),
            "duplicate canonical text coverage"
        );
        assert_eq!(
            value.encode_utf16().count() as u64,
            map["renderedRange"]["end"].as_u64().unwrap()
                - map["renderedRange"]["start"].as_u64().unwrap()
        );
    }
    (
        source,
        texts.into_values().collect(),
        natives.into_values().collect(),
    )
}

async fn metadata_string_fields(store: &Store, reference: &Value) -> Vec<(String, String)> {
    let mut pending = vec![reference.clone()];
    let mut fields = Vec::new();
    while let Some(reference) = pending.pop() {
        let attrs = page(
            store,
            json!({"kind":"metadata","ref":reference,"maxWireBytes":8192}),
        )
        .await;
        assert!(
            attrs["nextCursor"].is_null(),
            "tiny fixture attributes fit one page"
        );
        for item in attrs["items"].as_array().unwrap() {
            if item["childrenRef"].is_string() {
                pending.push(item["childrenRef"].clone());
            }
            if item["valueRef"].is_string() {
                let text = page(
                    store,
                    json!({"kind":"context","contextRef":item["valueRef"]}),
                )
                .await;
                fields.push((
                    item["key"].as_str().unwrap_or("").into(),
                    text["items"][0]["text"].as_str().unwrap().into(),
                ));
            }
        }
    }
    fields
}

#[tokio::test]
async fn indexed_plain_markdown_maps_preserve_heading_marks_and_nested_text() {
    for (raw, expected) in [
        (
            "## Héading 🙂\n\nPlain **bold** and *italic* &amp; \\*.",
            "Héading 🙂Plain bold and italic & *.",
        ),
        (
            "> quoted **bold**\n>\n> - outer\n>   - inner\n> - tail",
            "quoted boldouterinnertail",
        ),
        ("- first\n\n- second\n\n  paragraph", "firstsecondparagraph"),
        (
            "| Head | Other |\n| --- | --- |\n| café | 漢字 🙂 |",
            "HeadOthercafé漢字 🙂",
        ),
        ("[label](https://example.test) and `code`", "label and code"),
    ] {
        let (store, _tmp, _) = setup(raw).await;
        let (_, text, nodes) =
            canonical_window_text(&store, json!({"kind":"source","maxWireBytes":8192})).await;
        assert_eq!(text, expected, "source {raw}");
        let mut marks = Vec::new();
        for node in nodes.iter().filter(|node| node["marksRef"].is_string()) {
            marks.extend(metadata_string_fields(&store, &node["marksRef"]).await);
        }
        for (syntax, mark) in [
            ("**", "bold"),
            ("*italic*", "italic"),
            ("[label]", "link"),
            ("`code`", "code"),
        ] {
            if raw.contains(syntax) {
                assert!(
                    marks.contains(&("type".into(), mark.into())),
                    "missing canonical {mark} mark: {raw}"
                );
            }
        }
        if raw.starts_with("##") {
            let heading = nodes
                .iter()
                .find(|n| n["nodeType"] == "heading")
                .expect("canonical heading");
            let attrs = page(
                &store,
                json!({"kind":"metadata","ref":heading["attributesRef"]}),
            )
            .await;
            let fields = page(
                &store,
                json!({"kind":"metadata","ref":attrs["items"][0]["childrenRef"]}),
            )
            .await;
            assert!(fields["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field["key"] == "level" && field["value"] == 2));
        }
    }
}

#[tokio::test]
async fn indexed_plain_markdown_maps_rebuild_old_profile_and_expire_on_save() {
    let (store, tmp, mut note) = setup("## Before\n\nUnicode café 🙂").await;
    let author = intent_core::NoteVersionAuthor {
        id: "alice".into(),
        name: "Alice".into(),
        author_type: "user".into(),
    };
    let version = store
        .append_note_version(&note, &author, "2026-10-06T00:00:00Z", note.rev)
        .await
        .unwrap();
    let history = store
        .list_note_versions(&note.workspace_id, &note.id)
        .await
        .unwrap();
    let snapshot = store
        .get_note_version(&note.workspace_id, &note.id, version)
        .await
        .unwrap();
    let old = page(&store, json!({"kind":"source"})).await;
    let raw_revision = old["sourceRevision"].clone();
    // Model a database created by the prior profile: its current raw note was
    // indexed, but canonical Markdown map/native collections did not exist.
    sqlx::query("DELETE FROM note_page_entry WHERE json_extract(value,'$.kind') IN ('nativeNode','sourceMap') OR json_extract(value,'$.construct')='markdownBlock'")
        .execute(store.write_pool()).await.unwrap();
    sqlx::query("UPDATE note_page_head SET profile_revision='before-ordinary-markdown-maps'")
        .execute(store.write_pool())
        .await
        .unwrap();
    store.close().await;
    drop(store);
    let reopened = Store::open_for_daemon(&tmp.path).await.unwrap();
    assert_eq!(
        reopened
            .list_note_versions(&note.workspace_id, &note.id)
            .await
            .unwrap(),
        history
    );
    assert_eq!(
        reopened
            .get_note_version(&note.workspace_id, &note.id, version)
            .await
            .unwrap(),
        snapshot
    );
    let (current, text, _) =
        canonical_window_text(&reopened, json!({"kind":"source","maxWireBytes":8192})).await;
    assert_eq!(text, "BeforeUnicode café 🙂");
    assert_eq!(current["sourceRevision"], raw_revision);
    assert_eq!(current["scope"], old["scope"]);
    assert_eq!(current["text"], old["text"]);
    assert_error(
        reopened
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"context","contextRef":old["contextRef"]})),
                &json!(1),
            )
            .await,
        NotePageError::Expired,
    );
    note.content = "## After\n\n**Saved** 漢字".into();
    reopened.update_note(&note).await.unwrap();
    assert_error(
        reopened
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"context","contextRef":current["contextRef"]})),
                &json!(1),
            )
            .await,
        NotePageError::Stale,
    );
    let (saved, text, _) =
        canonical_window_text(&reopened, json!({"kind":"source","maxWireBytes":8192})).await;
    assert_eq!(text, "AfterSaved 漢字");
    assert_ne!(saved["sourceRevision"], raw_revision);
}

#[tokio::test]
async fn indexed_plain_markdown_maps_keep_exact_unicode_page_seams() {
    let raw = "## Héading 🙂\n\nPlain **bold café 漢字** and *italic* &amp; \\*.";
    let (store, _tmp, _) = setup(raw).await;
    let mut request = json!({"kind":"source","maxSourceBytes":17,"maxWireBytes":8192});
    let mut text = String::new();
    let mut snapshot = None;
    loop {
        let (source, part, _) = canonical_window_text(&store, request.clone()).await;
        if let Some(expected) = &snapshot {
            assert_eq!(expected, &source["snapshotId"]);
        } else {
            snapshot = Some(source["snapshotId"].clone());
        }
        text.push_str(&part);
        if source["nextCursor"].is_null() {
            break;
        }
        request["cursor"] = source["nextCursor"].clone();
    }
    assert_eq!(text, "Héading 🙂Plain bold café 漢字 and italic & *.");
}

#[tokio::test]
async fn indexed_plain_markdown_maps_keep_code_only_and_atom_owners() {
    for (raw, expected, atom) in [
        ("`only code`", "only code", None),
        ("before\nafter", "beforeafter", Some("hardBreak")),
        ("![alt](https://example.test/image.png)", "", Some("image")),
        (
            "- ![alt](https://example.test/image.png)",
            "",
            Some("image"),
        ),
        (
            "| ![alt](https://example.test/image.png) |\n| --- |",
            "",
            Some("image"),
        ),
        ("## ", "", Some("heading")),
    ] {
        let (store, _tmp, _) = setup(raw).await;
        let (_, text, nodes) =
            canonical_window_text(&store, json!({"kind":"source","maxWireBytes":8192})).await;
        assert_eq!(text, expected, "source {raw}");
        if let Some(atom) = atom {
            if atom == "image" {
                let image = nodes
                    .iter()
                    .find(|node| node["nodeType"] == "image")
                    .expect("reachable image node");
                assert_eq!(image["nodeClass"], "atom");
                let fields = metadata_string_fields(&store, &image["attributesRef"]).await;
                assert!(fields.contains(&("src".into(), "https://example.test/image.png".into())));
                assert!(fields.contains(&("alt".into(), "alt".into())));
            }
            assert!(
                nodes.iter().any(|node| node["nodeType"] == atom),
                "missing {atom}: {raw}"
            );
        }
    }
}

#[tokio::test]
async fn indexed_plain_markdown_far_seek_keeps_sql_work_bounded() {
    let mut costs = Vec::new();
    for length in [32_768, 2_000_000] {
        let raw = format!("# Heading\n\n{} **TARGET** café 🙂", "x".repeat(length));
        let at = raw[..raw.find("TARGET").unwrap()].encode_utf16().count();
        let (mut store, tmp, _) = setup(&raw).await;
        store.read_pool.close().await;
        store.read_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&tmp.path)
                    .read_only(true),
            )
            .await
            .unwrap()
            .into();
        let source = page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":6,"maxWireBytes":8192}),
        )
        .await;
        let context = page(
            &store,
            json!({"kind":"context","contextRef":source["contextRef"],"maxItems":128}),
        )
        .await;
        let owner = context["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["construct"] == "markdownBlock")
            .unwrap();
        let request =
            json!({"kind":"context","contextRef":owner["sourceMapRef"],"maxWireBytes":8192});
        page(&store, request.clone()).await;
        let count = Arc::new(AtomicUsize::new(0));
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            let copy = count.clone();
            conn.lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    copy.fetch_add(1, Ordering::Relaxed);
                    true
                });
        }
        let maps = page(&store, request).await;
        let cost = count.load(Ordering::Relaxed);
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle().await.unwrap().remove_progress_handler();
        }
        assert!(cost > 0 && cost < 500, "bounded actual SQLite work: {cost}");
        costs.push(cost);
        let visible: Vec<_> = maps["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|map| map["textRef"].is_string())
            .collect();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0]["sourceRange"], source["range"]);
        let text = page(
            &store,
            json!({"kind":"context","contextRef":visible[0]["textRef"]}),
        )
        .await;
        assert_eq!(text["items"][0]["text"], "TARGET");
    }
    assert!(
        costs[1] <= costs[0] + 30,
        "seek work grew with unloaded prefix: {costs:?}"
    );
}
