//! `agent.listUserMessages` default paging and the per-page byte budget
//! (PROTOCOL §5.5): every read is a bounded page with a `nextToken`, and
//! walking the cursor returns every user row exactly once.

use super::*;

/// Whole JSON-RPC frame bytes of a result, as the transport writes it.
fn frame_len(result: &serde_json::Value) -> usize {
    serde_json::to_vec(&json!({ "jsonrpc": "2.0", "id": 1, "result": result }))
        .expect("serialize")
        .len()
}

fn page_ids(page: &serde_json::Value) -> Vec<String> {
    page["items"]
        .as_array()
        .expect("items")
        .iter()
        .map(|item| item["id"].as_str().expect("id").to_string())
        .collect()
}

/// Walk every page from the newest (no token) and return the ids
/// oldest→newest plus the number of pages read.
async fn walk(
    svc: &Services,
    id: &AgentId,
    preview_chars: Option<i64>,
    limit: Option<i64>,
) -> (Vec<String>, usize) {
    let mut pages: Vec<Vec<String>> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = svc
            .agent_list_user_messages_page_op(id.clone(), None, preview_chars, limit, token)
            .await
            .expect("page");
        assert!(
            frame_len(&page) <= 512_000,
            "frame {} bytes",
            frame_len(&page)
        );
        assert_eq!(page["total"], json!(page_ids(&page).len()));
        pages.push(page_ids(&page));
        match page["nextToken"].as_str() {
            Some(next) => token = Some(next.to_string()),
            None => {
                assert!(page["nextToken"].is_null(), "nextToken always present");
                break;
            }
        }
        assert!(pages.len() < 10_000, "walk terminates");
    }
    let count = pages.len();
    (pages.into_iter().rev().flatten().collect(), count)
}

async fn append_user(svc: &Services, id: &AgentId, text: &str) -> String {
    svc.store()
        .append_agent_message(
            id,
            "user",
            &json!([{ "type": "text", "text": text }]),
            &now_iso(),
        )
        .await
        .expect("append user")
        .id
}

/// A call with no paging params serves a bounded first page (the newest
/// 100 rows) with a `nextToken`; following it returns every row once.
#[tokio::test]
async fn list_user_messages_pages_by_default() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "UserPages").await;
    let mut expected = Vec::new();
    for n in 0..250 {
        expected.push(append_user(&svc, &id, &format!("question {n}")).await);
        svc.store()
            .append_agent_message(
                &id,
                "assistant",
                &json!([{ "type": "text", "text": "a" }]),
                &now_iso(),
            )
            .await
            .expect("append assistant");
    }

    let first = svc
        .agent_list_user_messages_page_op(id.clone(), Some(ws.clone()), None, None, None)
        .await
        .expect("default page");
    assert_eq!(
        page_ids(&first),
        expected[150..].to_vec(),
        "newest 100, oldest→newest"
    );
    assert_eq!(first["total"], 100);
    assert!(
        first["nextToken"].is_string(),
        "older rows remain: {}",
        first["nextToken"]
    );

    let (all, pages) = walk(&svc, &id, None, None).await;
    assert_eq!(
        all, expected,
        "the walk returns every user row exactly once"
    );
    assert_eq!(pages, 3);

    let clamped = svc
        .agent_list_user_messages_page_op(id.clone(), None, None, Some(10_000), None)
        .await
        .expect("max page");
    assert_eq!(
        page_ids(&clamped),
        expected,
        "limit clamps to 500, all 250 fit"
    );
    assert!(clamped["nextToken"].is_null());
}

/// Escape-heavy max-length previews overflow the byte budget well before
/// the 500-row limit: the page stops at the budget, its token resumes at
/// the first row left out, and the frame stays under 512,000 bytes.
#[tokio::test]
async fn list_user_messages_trims_pages_to_the_byte_budget() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "UserPageBytes").await;
    let heavy = "\u{1}".repeat(2_000);
    let mut expected = Vec::new();
    for _ in 0..120 {
        expected.push(append_user(&svc, &id, &heavy).await);
    }

    let first = svc
        .agent_list_user_messages_page_op(id.clone(), None, Some(2_000), Some(500), None)
        .await
        .expect("page");
    let served = page_ids(&first);
    assert!(
        served.len() < 120,
        "the byte budget cut the page: {}",
        served.len()
    );
    assert!(!served.is_empty());
    assert_eq!(
        served,
        expected[120 - served.len()..].to_vec(),
        "newest rows kept"
    );
    assert!(
        frame_len(&first) <= 512_000,
        "frame {} bytes",
        frame_len(&first)
    );
    assert!(
        serde_json::to_vec(&first["items"]).expect("items").len() <= 480_000,
        "items within the page budget"
    );
    assert!(first["nextToken"].is_string());

    let (all, pages) = walk(&svc, &id, Some(2_000), Some(500)).await;
    assert_eq!(
        all, expected,
        "no row is skipped or repeated across byte cuts"
    );
    assert!(pages >= 2);
}

/// One row whose projected metadata alone exceeds the budget still serves
/// (alone) with the oversized field marked omitted, never silently cut, and
/// the walk continues past it.
#[tokio::test]
async fn list_user_messages_marks_a_single_oversized_row() {
    let (_t, svc, ws) = setup().await;
    let id = create_agent(&svc, &ws, "UserPageHuge").await;
    let older = append_user(&svc, &id, "older").await;
    let source = "s".repeat(600_000);
    let huge = svc
        .store()
        .append_agent_message_with_metadata(
            &id,
            "user",
            &json!([{ "type": "text", "text": "huge" }]),
            Some(&json!({ "source": source })),
            &now_iso(),
        )
        .await
        .expect("append huge")
        .id;

    let first = svc
        .agent_list_user_messages_page_op(id.clone(), None, None, None, None)
        .await
        .expect("page");
    assert_eq!(
        page_ids(&first),
        vec![huge.clone()],
        "oversized row serves alone"
    );
    let item = &first["items"][0];
    assert_eq!(item["preview"], "huge");
    assert!(item.get("metadata").is_none());
    assert_eq!(item["metadataOmitted"], true);
    assert!(item["metadataBytes"].as_u64().is_some_and(|n| n > 600_000));
    assert!(
        frame_len(&first) <= 512_000,
        "frame {} bytes",
        frame_len(&first)
    );

    let (all, _) = walk(&svc, &id, None, None).await;
    assert_eq!(all, vec![older, huge]);
}
