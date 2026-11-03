use super::*;

fn item(i: usize, body: usize) -> Value {
    json!({ "number": i, "body": "x".repeat(body) })
}

fn request(limit: Option<i64>, token: Option<&str>) -> PageRequest {
    PageRequest::new(limit, token)
}

fn token_str(v: &Value) -> &str {
    v.as_str().expect("string token")
}

#[test]
fn page_limit_defaults_to_50_and_caps_at_100() {
    assert_eq!(clamp_page_limit(None), 50);
    assert_eq!(clamp_page_limit(Some(0)), 1);
    assert_eq!(clamp_page_limit(Some(-3)), 1);
    assert_eq!(clamp_page_limit(Some(100)), 100);
    assert_eq!(clamp_page_limit(Some(1_000)), 100);
    assert_eq!(clamp_page_limit(Some(100_000)), 100);
}

#[test]
fn page_that_fits_is_served_whole_with_the_upstream_cursor() {
    let req = request(Some(10), None);
    let items: Vec<_> = (0..10).map(|i| item(i, 100)).collect();
    let (served, next) = budget_page(items.clone(), &req, Some("2"), 64 * 1024);
    assert_eq!(served, items);
    assert_eq!(
        decode_page_token(Some(token_str(&next))).cursor.as_deref(),
        Some("2")
    );
    let (_, last) = budget_page(items, &req, None, 64 * 1024);
    assert_eq!(last, Value::Null);
}

#[test]
fn old_style_tokens_still_decode() {
    let old = crate::github_ops::encode_next_token("7");
    assert_eq!(
        decode_page_token(Some(&old)),
        PageToken {
            cursor: Some("7".into()),
            skip: 0,
            limit: None
        }
    );
    assert_eq!(decode_page_token(Some("%%%")), PageToken::default());
    assert_eq!(decode_page_token(None), PageToken::default());
}

#[test]
fn over_budget_page_is_cut_and_resumes_inside_the_same_upstream_page() {
    let upstream: Vec<_> = (0..20).map(|i| item(i, 1_000)).collect();
    let budget = 5_500;
    let first = request(Some(20), Some(&crate::github_ops::encode_next_token("3")));
    let (page1, next1) = budget_page(upstream.clone(), &first, Some("4"), budget);
    assert!(!page1.is_empty() && page1.len() < 20);
    assert!(json_len(&Value::Array(page1.clone())) <= budget);
    let resume = decode_page_token(Some(token_str(&next1)));
    assert_eq!(
        resume,
        PageToken {
            cursor: Some("3".into()),
            skip: page1.len(),
            limit: Some(20)
        }
    );

    // Follow the tokens to the end: every item exactly once, in order, then
    // the upstream's own next cursor.
    let mut seen = page1;
    let mut next = next1;
    loop {
        let req = request(Some(5), Some(token_str(&next)));
        assert_eq!(req.limit, 20, "a resume token's page size wins");
        assert_eq!(req.cursor().as_deref(), Some("3"));
        let (page, n) = budget_page(upstream.clone(), &req, Some("4"), budget);
        assert!(json_len(&Value::Array(page.clone())) <= budget);
        seen.extend(page);
        let decoded = decode_page_token(Some(token_str(&n)));
        if decoded.skip == 0 {
            assert_eq!(decoded.cursor.as_deref(), Some("4"));
            break;
        }
        next = n;
    }
    assert_eq!(seen, upstream);
}

#[test]
fn first_page_cut_resumes_with_a_null_cursor() {
    let upstream: Vec<_> = (0..4).map(|i| item(i, 1_000)).collect();
    let req = request(None, None);
    let (page, next) = budget_page(upstream, &req, None, 2_100);
    assert_eq!(page.len(), 2);
    assert_eq!(
        decode_page_token(Some(token_str(&next))),
        PageToken {
            cursor: None,
            skip: 2,
            limit: Some(50)
        }
    );
}

#[test]
fn single_oversized_item_is_shrunk_and_marked() {
    let big = json!({
        "id": "RT1",
        "comments": [
            { "id": "c1", "body": "é".repeat(40_000) },
            { "id": "c2", "body": "y".repeat(90_000) },
        ],
    });
    let req = request(None, None);
    let (page, next) = budget_page(vec![big, item(2, 10)], &req, None, 20_000);
    assert_eq!(page.len(), 1, "the oversized item is served alone");
    assert!(json_len(&Value::Array(page.clone())) <= 20_000);
    assert_eq!(page[0][CONTENT_TRUNCATED_KEY], json!(true));
    assert_eq!(page[0]["id"], json!("RT1"), "short fields are kept");
    let body = page[0]["comments"][0]["body"].as_str().unwrap();
    assert!(!body.is_empty() && body.chars().all(|c| c == 'é'));
    assert_eq!(decode_page_token(Some(token_str(&next))).skip, 1);
}

#[test]
fn resume_past_the_end_serves_nothing_and_moves_on() {
    let req = PageRequest {
        token: PageToken {
            cursor: Some("3".into()),
            skip: 9,
            limit: Some(5),
        },
        limit: 5,
    };
    let (page, next) = budget_page((0..5).map(|i| item(i, 1)).collect(), &req, Some("4"), 1_000);
    assert!(page.is_empty());
    assert_eq!(
        decode_page_token(Some(token_str(&next))).cursor.as_deref(),
        Some("4")
    );
}
