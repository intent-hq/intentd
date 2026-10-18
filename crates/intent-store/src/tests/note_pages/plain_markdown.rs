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

async fn context_field(store: &Store, boundary: &Value, field: &str) -> String {
    let details = page(
        store,
        json!({"kind":"context","contextRef":boundary["detailRef"]}),
    )
    .await;
    let entry = details["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["field"] == field)
        .unwrap();
    let mut reference = entry["nextRef"].clone();
    let mut value = entry["text"].as_str().unwrap().to_owned();
    while !reference.is_null() {
        let chunk = page(store, json!({"kind":"context","contextRef":reference})).await;
        value.push_str(chunk["items"][0]["text"].as_str().unwrap());
        reference = chunk["items"][0]["nextRef"].clone();
    }
    value
}

#[tokio::test]
async fn indexed_task_list_marker_outside_paragraph_range_keeps_source() {
    for source in [
        "- [ ] [First task](intent://local/task/first)\n\n- [ ] [Second task](intent://local/task/second)\n",
        "- [x] **Café 😀**\n\n- [ ] 終了\n",
        "- [ ] first\n- [x] second\n",
        "- [ ] outer\n\n  - [x] nested 😀\n\n  - [ ] last\n",
        "- [ ]\n\n- [x]\n",
    ] {
        let (store, _tmp, _) = setup(source).await;
        let read = page(&store, json!({"kind":"source","at":0})).await;
        assert_eq!(read["text"], source);
        let context = page(&store, json!({"kind":"context","contextRef":read["contextRef"]})).await;
        let items = context["items"].as_array().unwrap();
        let markers: Vec<_> = items.iter().filter(|item| item["role"] == "taskMarker").collect();
        assert_eq!(markers.len(), source.matches("[ ]").count() + source.matches("[x]").count());
        let units: Vec<_> = source.encode_utf16().collect();
        for marker in markers {
            let start = usize::try_from(marker["sourceRange"]["start"].as_u64().unwrap()).unwrap();
            let end = usize::try_from(marker["sourceRange"]["end"].as_u64().unwrap()).unwrap();
            let literal = String::from_utf16(&units[start..end]).unwrap();
            assert!(literal == "[ ]" || literal == "[x]");
            let owner = page(&store, json!({"kind":"context","contextRef":marker["parentRef"]})).await;
            let owner = &owner["items"][0];
            assert_eq!(owner["construct"], "listItem");
            // Every specimen uses a dash and one space before the marker.
            // Exact identity rejects accidentally assigning a nested marker
            // to an outer item whose range merely contains it.
            assert_eq!(owner["sourceRange"]["start"].as_u64().unwrap(), (start - 2) as u64);
            assert!(owner["sourceRange"]["end"].as_u64().unwrap() >= end as u64);
            assert_eq!(context_field(&store, owner, "openingSource").await, "- ");
        }
        for paragraph in items.iter().filter(|item| item["construct"] == "paragraph") {
            assert_eq!(context_field(&store, paragraph, "openingSource").await, "");
            let closing = context_field(&store, paragraph, "closingSource").await;
            assert!(closing.is_empty() || closing == "\n", "unexpected paragraph suffix: {closing:?}");
        }
    }
}

#[tokio::test]
async fn indexed_markdown_source_maps_cover_block_separators() {
    for raw in [
        "# Blank Spec control\n\nPlain Unicode café 漢字 🙂 remains exact.\n",
        "\n\n# Héading 🙂\n\nParagraph\n\n",
        "\r\n# Héading\r\n\r\nParagraph\r\n\r\n",
        "\n \n\t\n",
    ] {
        let (store, _tmp, _) = setup(raw).await;
        for (at, max_bytes) in [(0, 32), (21, 32), (21, 4)] {
            if at >= raw.encode_utf16().count() {
                continue;
            }
            let mut calls = Vec::new();
            let source = record_source_window_closure(
                &store,
                json!({"kind":"source","at":at,"maxSourceBytes":max_bytes,"maxWireBytes":8192}),
                &mut calls,
            )
            .await;
            let items: Vec<_> = calls
                .iter()
                .flat_map(|call| call["response"]["items"].as_array().into_iter().flatten())
                .collect();
            for owner in items
                .iter()
                .filter(|item| item["construct"] == "markdownDocument")
            {
                assert_eq!(
                    owner["sourceRange"],
                    json!({"start":0,"end":raw.encode_utf16().count()})
                );
                assert_eq!(owner["entryPath"], "markdown");
                let native = page(
                    &store,
                    json!({"kind":"context","contextRef":owner["nativeRef"]}),
                )
                .await;
                let root = &native["items"][0];
                assert_eq!(root["nodeType"], "doc");
                assert_eq!(root["nodeClass"], "container");
                assert!(root["parentRef"].is_null());
                assert_eq!(root["childIndex"], 0);
                assert_eq!(root["provenance"], "implicit");
                assert_eq!(root["sourceRange"], json!({"start":0,"end":0}));
                assert_eq!(root["attributesRef"], owner["attributesRef"]);
                assert_eq!(root["profile"], owner["profile"]);
                assert_eq!(root["profileVersion"], owner["profileVersion"]);
                for map in items.iter().filter(|item| item["kind"] == "sourceMap") {
                    let resolved = page(
                        &store,
                        json!({"kind":"context","contextRef":map["ownerRef"]}),
                    )
                    .await;
                    if resolved["items"][0]["id"] != owner["id"] {
                        continue;
                    }
                    assert_eq!(map["mapping"], "omitted");
                    assert_eq!(map["renderedRange"], json!({"start":0,"end":0}));
                    for key in ["textRef", "textNodeRef", "textNodeId"] {
                        assert!(map[key].is_null());
                    }
                    assert!(
                        map["sourceRange"]["start"].as_u64().unwrap()
                            >= source["range"]["start"].as_u64().unwrap()
                    );
                    assert!(
                        map["sourceRange"]["end"].as_u64().unwrap()
                            <= source["range"]["end"].as_u64().unwrap()
                    );
                }
            }
            let mut ranges: Vec<_> = calls
                .iter()
                .flat_map(|call| call["response"]["items"].as_array().into_iter().flatten())
                .filter(|item| item["kind"] == "sourceMap")
                .map(|item| {
                    (
                        item["sourceRange"]["start"].as_u64().unwrap(),
                        item["sourceRange"]["end"].as_u64().unwrap(),
                    )
                })
                .collect();
            ranges.sort_unstable();
            ranges.dedup();
            let mut covered = source["range"]["start"].as_u64().unwrap();
            for (start, end) in &ranges {
                assert!(
                    *start <= covered,
                    "unmapped separator at {covered}; source={raw:?}, window={}, ranges={ranges:?}",
                    source["range"]
                );
                covered = covered.max(*end);
            }
            assert!(
                covered >= source["range"]["end"].as_u64().unwrap(),
                "unmapped tail; source={raw:?}, ranges={ranges:?}"
            );
        }
    }
}

#[tokio::test]
async fn indexed_markdown_source_maps_cover_container_separators() {
    let mut failures = Vec::new();
    for raw in [
        "- [ ] [First task](intent://local/task/first)\n\n- [ ] [Second task](intent://local/task/second)\n",
        "- outer\n\n  - [ ] nested café 🙂\n\n  - next\n",
        "> first café 🙂\n>\n> second\n\n> third\n",
    ] {
        let (store, _tmp, _) = setup(raw).await;
        let mut calls = Vec::new();
        let source = record_source_window_closure(
            &store,
            json!({"kind":"source","maxSourceBytes":4096,"maxWireBytes":8192}),
            &mut calls,
        ).await;
        assert_eq!(source["text"], raw);
        let mut ranges: Vec<_> = calls.iter()
            .flat_map(|call| call["response"]["items"].as_array().into_iter().flatten())
            .filter(|item| item["kind"] == "sourceMap")
            .map(|item| (item["sourceRange"]["start"].as_u64().unwrap(), item["sourceRange"]["end"].as_u64().unwrap()))
            .collect();
        ranges.sort_unstable();
        ranges.dedup();
        let mut covered = 0;
        let mut holes = Vec::new();
        for (start, end) in &ranges {
            if *start > covered { holes.push((covered, *start)); }
            covered = covered.max(*end);
        }
        let end = source["range"]["end"].as_u64().unwrap();
        if covered < end { holes.push((covered, end)); }
        if !holes.is_empty() { failures.push(format!("source={raw:?}; holes={holes:?}; maps={ranges:?}")); }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn indexed_markdown_source_maps_admit_empty_document_only_at_empty_source() {
    for raw in ["", "# Heading\n\nParagraph\n"] {
        let (store, _tmp, _) = setup(raw).await;
        let mut calls = Vec::new();
        let end = raw.encode_utf16().count();
        let source = record_source_window_closure(
            &store,
            json!({"kind":"source","at":end,"maxSourceBytes":32,"maxWireBytes":8192}),
            &mut calls,
        )
        .await;
        assert_eq!(source["range"], json!({"start":end,"end":end}));
        let items: Vec<_> = calls
            .iter()
            .flat_map(|call| call["response"]["items"].as_array().into_iter().flatten())
            .collect();
        let owners: Vec<_> = items
            .iter()
            .filter(|item| {
                item["construct"] == "markdownDocument" && item.get("sourceMapRef").is_some()
            })
            .collect();
        if raw.is_empty() {
            assert_eq!(
                owners.len(),
                1,
                "empty source must expose the document root"
            );
            let owner = owners[0];
            assert_eq!(owner["sourceRange"], json!({"start":0,"end":0}));
            let maps = page(
                &store,
                json!({"kind":"context","contextRef":owner["sourceMapRef"]}),
            )
            .await;
            assert!(maps["items"].as_array().unwrap().is_empty());
            let native = page(
                &store,
                json!({"kind":"context","contextRef":owner["nativeRef"]}),
            )
            .await;
            assert_eq!(native["items"][0]["nodeType"], "doc");
            assert_eq!(
                native["items"][0]["sourceRange"],
                json!({"start":0,"end":0})
            );
        } else {
            assert!(
                owners.is_empty(),
                "nonempty EOF must not admit the document owner"
            );
        }
    }
}

#[tokio::test]
async fn indexed_markdown_source_maps_do_not_admit_root_between_gaps() {
    let raw = "\n\n# Heading\n\nParagraph\n\n";
    let (store, _tmp, _) = setup(raw).await;
    let mut calls = Vec::new();
    let source = record_source_window_closure(
        &store,
        json!({"kind":"source","at":5,"maxSourceBytes":4,"maxWireBytes":8192}),
        &mut calls,
    )
    .await;
    assert_eq!(source["text"], "eadi");
    assert!(
        !calls
            .iter()
            .flat_map(|call| call["response"]["items"].as_array().into_iter().flatten())
            .any(|item| item["construct"] == "markdownDocument"),
        "gap-free heading window must not admit the document owner"
    );
}

#[tokio::test]
async fn indexed_markdown_source_maps_seek_document_gaps_with_bounded_sql() {
    let mut costs = Vec::new();
    for blocks in [512, 4096] {
        let raw = "# Head\n\nabcdef\n\n".repeat(blocks);
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
        let at = raw.len() - 1;
        let source = page(
            &store,
            json!({"kind":"source","at":at,"maxSourceBytes":4,"maxWireBytes":4096}),
        )
        .await;
        assert_eq!(source["text"], "\n");
        let counter = Arc::new(AtomicUsize::new(0));
        let count = counter.clone();
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle()
                .await
                .unwrap()
                .set_progress_handler(1, move || {
                    count.fetch_add(1, Ordering::Relaxed);
                    true
                });
        }
        let mut req = json!({"kind":"context","contextRef":source["contextRef"],"maxItems":1,"maxWireBytes":4096});
        let mut items = Vec::new();
        loop {
            let context = page(&store, req.clone()).await;
            assert!(context["items"].as_array().unwrap().len() <= 1);
            assert!(serde_json::to_vec(&context).unwrap().len() <= 4096);
            items.extend(context["items"].as_array().unwrap().iter().cloned());
            if context["nextCursor"].is_null() {
                break;
            }
            req["cursor"] = context["nextCursor"].clone();
        }
        let cost = counter.load(Ordering::Relaxed);
        {
            let mut conn = store.read_pool.acquire().await.unwrap();
            conn.lock_handle().await.unwrap().remove_progress_handler();
        }
        costs.push(cost);
        let owners: Vec<_> = items
            .iter()
            .filter(|item| item["construct"] == "markdownDocument")
            .collect();
        assert_eq!(owners.len(), 1);
        let maps = page(&store,json!({"kind":"context","contextRef":owners[0]["sourceMapRef"],"maxItems":1,"maxWireBytes":4096})).await;
        assert_eq!(maps["items"].as_array().unwrap().len(), 1);
        assert_eq!(maps["items"][0]["sourceRange"], source["range"]);
        assert_eq!(maps["items"][0]["mapping"], "omitted");
        let piece:i64=sqlx::query_scalar("SELECT start FROM note_page_piece WHERE workspace_id='pages' AND note_id='spec' AND start<=? ORDER BY start DESC LIMIT 1")
            .bind(i64::try_from(at).unwrap()).fetch_one(store.read_pool()).await.unwrap();
        let sql = crate::note_page_repo::CONTEXT_WINDOW_SQL;
        let start = i64::try_from(at).unwrap();
        let end = start + 1;
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let plan: Vec<String> = sqlx::query(&explain)
            .bind("pages")
            .bind("spec")
            .bind(format!("c:{piece}"))
            .bind(0)
            .bind(end)
            .bind(start)
            .bind(start)
            .bind(end)
            .bind(start)
            .bind(end)
            .bind(2)
            .fetch_all(store.read_pool())
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>(3))
            .collect();
        assert!(
            plan.iter()
                .any(|line| line.contains("SEARCH gap") && line.contains("note_page_map_end")),
            "{plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|line| line.contains("SCAN gap") || line.contains("TEMP B-TREE")),
            "{plan:?}"
        );
        let zero = sqlx::query(sql)
            .bind("pages")
            .bind("spec")
            .bind(format!("c:{piece}"))
            .bind(0)
            .bind(start)
            .bind(start)
            .bind(start)
            .bind(start)
            .bind(start)
            .bind(start)
            .bind(128)
            .fetch_all(store.read_pool())
            .await
            .unwrap();
        assert!(!zero.iter().any(|row| serde_json::from_str::<Value>(
            &row.get::<String, _>("value")
        )
        .unwrap()["construct"]
            == "markdownDocument"));
        eprintln!(
            "DOCUMENT_ADMISSION_QUERY bytes={} cold_context_vm_steps={cost} plan={plan:?}",
            raw.len()
        );
        assert!(cost > 0 && cost < 20000, "unexpected SQL work {cost}");
    }
    assert!(
        costs[1] <= costs[0] + 100,
        "admission work grew with unloaded prefix: {costs:?}"
    );
}

#[tokio::test]
async fn indexed_markdown_tasks_match_full_editor_native_state() {
    fn expected_shape(node: &Value) -> Value {
        let mut value = json!({"type":node["type"]});
        if node["type"] == "taskItem" {
            value["attrs"] = node["attrs"].clone();
        }
        if let Some(children) = node["content"].as_array() {
            value["content"] = children.iter().map(expected_shape).collect();
        }
        value
    }
    fn text(node: &Value) -> String {
        node["text"].as_str().map_or_else(
            || {
                node["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(text)
                    .collect()
            },
            str::to_owned,
        )
    }
    fn actual_shape(
        id: &str,
        nodes: &std::collections::BTreeMap<String, Value>,
        children: &std::collections::BTreeMap<String, Vec<(u64, String)>>,
    ) -> Value {
        let mut value = nodes[id].clone();
        if let Some(children_here) = children.get(id) {
            value["content"] = children_here
                .iter()
                .map(|(_, child)| actual_shape(child, nodes, children))
                .collect();
        }
        value
    }
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/note_task_native.json")).unwrap();
    let mut failures = Vec::new();
    for fixture in fixtures {
        let raw = fixture["source"].as_str().unwrap();
        let (store, _tmp, _) = setup(raw).await;
        let (_, rendered, natives) =
            canonical_window_text(&store, json!({"kind":"source","maxWireBytes":8192})).await;
        assert_eq!(rendered, text(&fixture["native"]));
        let mut nodes = std::collections::BTreeMap::new();
        let mut children = std::collections::BTreeMap::<String, Vec<(u64, String)>>::new();
        let mut root = None;
        for native in natives {
            let id = native["id"].as_str().unwrap().to_owned();
            let mut value = json!({"type":native["nodeType"]});
            if native["nodeType"] == "taskItem" {
                let attrroot = page(
                    &store,
                    json!({"kind":"metadata","ref":native["attributesRef"]}),
                )
                .await;
                let attrs = page(
                    &store,
                    json!({"kind":"metadata","ref":attrroot["items"][0]["childrenRef"]}),
                )
                .await;
                let mut object = serde_json::Map::new();
                for field in attrs["items"].as_array().unwrap() {
                    let val = if field["valueRef"].is_string() {
                        let fragment = page(
                            &store,
                            json!({"kind":"context","contextRef":field["valueRef"]}),
                        )
                        .await;
                        fragment["items"][0]["text"].clone()
                    } else {
                        field["value"].clone()
                    };
                    object.insert(field["key"].as_str().unwrap().into(), val);
                }
                value["attrs"] = Value::Object(object);
            }
            if native["parentRef"].is_string() {
                let parent = page(
                    &store,
                    json!({"kind":"context","contextRef":native["parentRef"]}),
                )
                .await;
                children
                    .entry(parent["items"][0]["id"].as_str().unwrap().into())
                    .or_default()
                    .push((native["childIndex"].as_u64().unwrap(), id.clone()));
            } else {
                root = Some(id.clone());
            }
            nodes.insert(id, value);
        }
        for children in children.values_mut() {
            children.sort();
        }
        let actual = actual_shape(&root.unwrap(), &nodes, &children);
        let expected = expected_shape(&fixture["native"]);
        if actual != expected {
            failures.push(json!({"name":fixture["name"],"actual":actual,"expected":expected}));
        }
    }
    assert!(
        failures.is_empty(),
        "{}",
        serde_json::to_string_pretty(&failures).unwrap()
    );
}

#[tokio::test]
async fn indexed_markdown_tasks_do_not_reclassify_ordinary_ancestors() {
    for (source, outer_kind, task_count) in [
        ("3. first\n4. second\n", "orderedList", 0),
        (
            "- ordinary parent\n  - [x] nested task\n- ordinary sibling\n",
            "bulletList",
            1,
        ),
        (
            "3. ordinary parent\n   - [x] nested task\n4. ordinary sibling\n",
            "orderedList",
            1,
        ),
    ] {
        let (store, _tmp, _) = setup(source).await;
        let (_, _, natives) =
            canonical_window_text(&store, json!({"kind":"source","maxWireBytes":8192})).await;
        assert_eq!(
            natives
                .iter()
                .filter(|n| n["nodeType"] == "taskItem")
                .count(),
            task_count
        );
        assert_eq!(
            natives
                .iter()
                .filter(|n| n["nodeType"] == "taskList")
                .count(),
            task_count
        );
        assert_eq!(
            natives
                .iter()
                .filter(|n| n["nodeType"] == "listItem")
                .count(),
            2
        );
        let outer = natives
            .iter()
            .find(|n| n["nodeType"] == outer_kind)
            .unwrap();
        let root = page(
            &store,
            json!({"kind":"context","contextRef":outer["parentRef"]}),
        )
        .await;
        assert_eq!(root["items"][0]["nodeType"], "doc");
        if outer_kind == "orderedList" {
            let root = page(
                &store,
                json!({"kind":"metadata","ref":outer["attributesRef"]}),
            )
            .await;
            let attrs = page(
                &store,
                json!({"kind":"metadata","ref":root["items"][0]["childrenRef"]}),
            )
            .await;
            assert_eq!(
                attrs["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|a| a["key"] == "start")
                    .unwrap()["value"],
                3
            );
        }
        if task_count > 0 {
            let nested = natives
                .iter()
                .find(|n| n["nodeType"] == "taskList")
                .unwrap();
            let parent = page(
                &store,
                json!({"kind":"context","contextRef":nested["parentRef"]}),
            )
            .await;
            assert_eq!(parent["items"][0]["nodeType"], "listItem");
        }
    }
}

async fn task_metadata_value(store: &Store, entry: &Value) -> Value {
    if entry["type"] == "string" {
        let part = page(
            store,
            json!({"kind":"context","contextRef":entry["valueRef"]}),
        )
        .await;
        assert!(part["items"][0]["nextRef"].is_null());
        return part["items"][0]["text"].clone();
    }
    if entry["childrenRef"].is_string() {
        let children = page(store, json!({"kind":"metadata","ref":entry["childrenRef"]})).await;
        assert!(children["nextCursor"].is_null());
        if entry["type"] == "array" {
            let mut values = Vec::new();
            for child in children["items"].as_array().unwrap() {
                assert_eq!(child["index"], values.len());
                values.push(Box::pin(task_metadata_value(store, child)).await);
            }
            return Value::Array(values);
        }
        let mut values = serde_json::Map::new();
        for child in children["items"].as_array().unwrap() {
            values.insert(
                child["key"].as_str().unwrap().into(),
                Box::pin(task_metadata_value(store, child)).await,
            );
        }
        return Value::Object(values);
    }
    entry["value"].clone()
}

#[tokio::test]
async fn indexed_markdown_tasks_preserve_public_links_and_split_provenance() {
    fn linked_leaves<'a>(node: &'a Value, result: &mut Vec<&'a Value>) {
        if node["marks"].is_array() {
            result.push(node);
        }
        for child in node["content"].as_array().into_iter().flatten() {
            linked_leaves(child, result);
        }
    }
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/note_task_native.json")).unwrap();
    for fixture in fixtures {
        let raw = fixture["source"].as_str().unwrap();
        let (store, _tmp, _) = setup(raw).await;
        let (_, _, natives) =
            canonical_window_text(&store, json!({"kind":"source","maxWireBytes":8192})).await;
        let mut expected_leaves = Vec::new();
        linked_leaves(&fixture["native"], &mut expected_leaves);
        let marked: Vec<_> = natives
            .iter()
            .filter(|n| n["marksRef"].is_string())
            .collect();
        assert_eq!(marked.len(), expected_leaves.len());
        for expected in expected_leaves {
            let label = expected["text"].as_str().unwrap();
            let start = raw.find(label).unwrap();
            let range = json!({"start":raw[..start].encode_utf16().count(),"end":raw[..start+label.len()].encode_utf16().count()});
            let native = marked
                .iter()
                .find(|n| n["sourceRange"] == range)
                .expect("exact link label provenance");
            assert_eq!(native["provenance"], "explicit");
            assert_eq!(native["childIndex"], 0);
            let root = page(&store, json!({"kind":"metadata","ref":native["marksRef"]})).await;
            assert_eq!(
                task_metadata_value(&store, &root["items"][0]).await,
                expected["marks"],
                "{} link {label}",
                fixture["name"]
            );
        }
        // Exact parser envelopes for these retained ASCII specimens. Split lists
        // own only their contiguous member items, never adjacent plain/task groups.
        let expected = match fixture["name"].as_str().unwrap() {
            "loose" => json!([
                ["taskList", 0, 95, 0],
                ["taskItem", 0, 47, 0],
                ["taskItem", 47, 95, 1]
            ]),
            "tight" => json!([
                ["taskList", 0, 94, 0],
                ["taskItem", 0, 46, 0],
                ["taskItem", 46, 94, 1]
            ]),
            "mixed" => json!([
                ["taskList", 0, 46, 0],
                ["taskItem", 0, 46, 0],
                ["bulletList", 46, 65, 1],
                ["listItem", 46, 65, 0],
                ["taskList", 65, 113, 2],
                ["taskItem", 65, 113, 0]
            ]),
            "nested" => json!([
                ["taskList", 0, 96, 0],
                ["taskItem", 0, 96, 0],
                ["taskList", 48, 96, 1],
                ["taskItem", 48, 96, 0],
                ["bulletList", 96, 115, 1],
                ["listItem", 96, 115, 0]
            ]),
            other => panic!("unknown oracle {other}"),
        };
        let mut actual: Vec<Value> = natives
            .iter()
            .filter(|n| {
                matches!(
                    n["nodeType"].as_str(),
                    Some("taskList" | "taskItem" | "bulletList" | "listItem")
                )
            })
            .map(|n| {
                assert_eq!(n["provenance"], "explicit");
                json!([
                    n["nodeType"],
                    n["sourceRange"]["start"],
                    n["sourceRange"]["end"],
                    n["childIndex"]
                ])
            })
            .collect();
        let mut expected = expected.as_array().unwrap().clone();
        actual.sort_by_key(Value::to_string);
        expected.sort_by_key(Value::to_string);
        assert_eq!(
            actual, expected,
            "{} exact split list/item provenance",
            fixture["name"]
        );
    }
}

#[tokio::test]
async fn indexed_markdown_tasks_cover_all_oracle_source_ranges() {
    let mut fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/note_task_native.json")).unwrap();
    for (name, source) in [
        ("ordinary-tight", "- [First](https://example.test)\n- second\n"),
        ("unicode-inline", "- [ ] **Café 🙂** &amp; [linked](https://example.test) `a b`\n- [x] 終了\n"),
        ("image-inline", "- [ ] before ![alt](image.png) after\n- plain\n"),
        ("nested-block-boundary", "- [ ] [outer](https://outer.test)\n  - [x] **inner 🙂**\n- ordinary\n"),
        ("multiple-paragraphs", "- [ ] [first](https://first.test)\n\n  second **paragraph**\n\n  - nested [link](https://nested.test)\n"),
    ] { fixtures.push(json!({"name":name,"source":source})); }
    let mut failures = Vec::new();
    for fixture in fixtures {
        let raw = fixture["source"].as_str().unwrap();
        let (store, _tmp, _) = setup(raw).await;
        let mut calls = Vec::new();
        let source = record_source_window_closure(
            &store,
            json!({"kind":"source","maxWireBytes":8192}),
            &mut calls,
        )
        .await;
        assert_eq!(source["text"], raw);
        let items: Vec<_> = calls
            .iter()
            .flat_map(|c| c["response"]["items"].as_array().into_iter().flatten())
            .collect();
        let mut ranges: Vec<_> = items
            .iter()
            .filter(|i| i["kind"] == "sourceMap")
            .map(|i| {
                (
                    i["sourceRange"]["start"].as_u64().unwrap(),
                    i["sourceRange"]["end"].as_u64().unwrap(),
                )
            })
            .collect();
        ranges.sort_unstable();
        ranges.dedup();
        let mut covered = 0;
        let mut gaps = Vec::new();
        for (start, end) in &ranges {
            if *start > covered {
                gaps.push((covered, *start));
            }
            covered = covered.max(*end);
        }
        let end = raw.encode_utf16().count() as u64;
        if covered < end {
            gaps.push((covered, end));
        }
        let mut mismatched_owners = Vec::new();
        for owner in items.iter().filter(|i| i["construct"] == "markdownBlock") {
            let native = page(
                &store,
                json!({"kind":"context","contextRef":owner["nativeRef"]}),
            )
            .await;
            let node = &native["items"][0];
            if node["provenance"] == "repaired" {
                let receipt = page(
                    &store,
                    json!({"kind":"context","contextRef":node["sourcePiecesRef"]}),
                )
                .await;
                assert!(receipt["nextCursor"].is_null());
                let pieces = receipt["items"].as_array().unwrap();
                assert!(!pieces.is_empty());
                assert_eq!(
                    pieces.first().unwrap()["sourceRange"]["start"],
                    node["sourceRange"]["start"]
                );
                assert_eq!(
                    pieces.last().unwrap()["sourceRange"]["end"],
                    node["sourceRange"]["end"]
                );
                let units: Vec<_> = raw.encode_utf16().collect();
                for piece in pieces {
                    assert_eq!(piece["role"], "body");
                    let from =
                        usize::try_from(piece["sourceRange"]["start"].as_u64().unwrap()).unwrap();
                    let to =
                        usize::try_from(piece["sourceRange"]["end"].as_u64().unwrap()).unwrap();
                    assert!(from < to);
                    assert!(
                        String::from_utf16(&units[from..to]).is_ok(),
                        "scalar-safe receipt"
                    );
                    let target = page(
                        &store,
                        json!({"kind":"context","contextRef":piece["nodeRef"]}),
                    )
                    .await;
                    assert_eq!(target["items"][0]["id"], node["id"]);
                }
                for pair in pieces.windows(2) {
                    assert!(
                        pair[0]["sourceRange"]["end"].as_u64().unwrap()
                            <= pair[1]["sourceRange"]["start"].as_u64().unwrap()
                    );
                }
            }
            if owner["sourceRange"] != node["sourceRange"] {
                mismatched_owners
                    .push(json!({"owner":owner["sourceRange"],"native":native["items"][0]}));
            }
        }
        if !gaps.is_empty() || !mismatched_owners.is_empty() {
            failures.push(json!({"name":fixture["name"],"source":raw,"gaps":gaps,"ranges":ranges,"mismatchedOwners":mismatched_owners}));
        }
    }
    assert!(
        failures.is_empty(),
        "{}",
        serde_json::to_string_pretty(&failures).unwrap()
    );
}

#[tokio::test]
async fn indexed_markdown_tasks_expose_exact_inline_pieces_in_items_and_cells() {
    let inline = "[Café 🙂](https://example.test) and `x y` &amp;";
    for raw in [
        format!("- [ ] {inline}\n- plain\n"),
        format!("|Head|\n|---|\n|{inline}|\n"),
    ] {
        let (store, _tmp, _) = setup(&raw).await;
        let (_, _, natives) =
            canonical_window_text(&store, json!({"kind":"source","maxWireBytes":8192})).await;
        let start_byte = raw.find(inline).unwrap();
        let start = raw[..start_byte].encode_utf16().count();
        let end = start + inline.encode_utf16().count();
        let paragraph = natives
            .iter()
            .find(|n| {
                n["nodeType"] == "paragraph" && n["sourceRange"] == json!({"start":start,"end":end})
            })
            .expect("same exact inline group envelope");
        assert_eq!(paragraph["provenance"], "repaired");
        let parent = page(
            &store,
            json!({"kind":"context","contextRef":paragraph["parentRef"]}),
        )
        .await;
        assert_eq!(
            parent["items"][0]["nodeType"],
            if raw.starts_with('-') {
                "taskItem"
            } else {
                "tableCell"
            }
        );
        let mut request = json!({"kind":"context","contextRef":paragraph["sourcePiecesRef"],"maxItems":1,"maxWireBytes":4096});
        let mut actual = Vec::new();
        let mut ids = std::collections::BTreeSet::new();
        let units: Vec<_> = raw.encode_utf16().collect();
        loop {
            let got = page(&store, request).await;
            assert_eq!(got["items"].as_array().unwrap().len(), 1);
            assert!(
                serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":got}))
                    .unwrap()
                    .len()
                    <= 4096
            );
            let piece = &got["items"][0];
            assert!(ids.insert(piece["id"].clone().to_string()));
            assert_eq!(piece["role"], "body");
            let target = page(
                &store,
                json!({"kind":"context","contextRef":piece["nodeRef"]}),
            )
            .await;
            assert_eq!(target["items"][0]["id"], paragraph["id"]);
            let from = usize::try_from(piece["sourceRange"]["start"].as_u64().unwrap()).unwrap();
            let to = usize::try_from(piece["sourceRange"]["end"].as_u64().unwrap()).unwrap();
            actual.push((from, to, String::from_utf16(&units[from..to]).unwrap()));
            if got["nextCursor"].is_null() {
                break;
            }
            request = json!({"kind":"context","contextRef":paragraph["sourcePiecesRef"],"cursor":got["nextCursor"],"maxItems":1,"maxWireBytes":4096});
        }
        let mut at = start;
        let expected: Vec<_> = [
            "[Café 🙂](https://example.test)",
            " and ",
            "`x y`",
            " ",
            "&amp;",
        ]
        .into_iter()
        .map(|piece| {
            let from = at;
            at += piece.encode_utf16().count();
            (from, at, piece.to_owned())
        })
        .collect();
        assert_eq!(at, end);
        assert_eq!(actual, expected, "exact parser child slices in {raw:?}");
        // A small window beginning at either the link syntax or the Unicode
        // label still obtains complete local maps without loading prior pages.
        for at in [start, start + 1, start + 6] {
            let mut calls = Vec::new();
            let source = record_source_window_closure(
                &store,
                json!({"kind":"source","at":at,"maxSourceBytes":4,"maxWireBytes":8192}),
                &mut calls,
            )
            .await;
            let mut ranges: Vec<_> = calls
                .iter()
                .flat_map(|c| c["response"]["items"].as_array().into_iter().flatten())
                .filter(|i| i["kind"] == "sourceMap")
                .map(|i| {
                    (
                        i["sourceRange"]["start"].as_u64().unwrap(),
                        i["sourceRange"]["end"].as_u64().unwrap(),
                    )
                })
                .collect();
            ranges.sort_unstable();
            ranges.dedup();
            let mut covered = source["range"]["start"].as_u64().unwrap();
            for (from, to) in ranges {
                assert!(from <= covered, "gap in clipped paragraph maps");
                covered = covered.max(to);
            }
            assert_eq!(covered, source["range"]["end"].as_u64().unwrap());
        }
    }
}
