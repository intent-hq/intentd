use super::*;

#[test]
fn clamp_limit_defaults_when_absent() {
    assert_eq!(clamp_limit(None), DEFAULT_PAGE_LIMIT);
    assert_eq!(DEFAULT_PAGE_LIMIT, 50);
}

#[test]
fn clamp_limit_caps_over_max() {
    assert_eq!(clamp_limit(Some(201)), MAX_PAGE_LIMIT);
    assert_eq!(clamp_limit(Some(10_000)), MAX_PAGE_LIMIT);
    assert_eq!(MAX_PAGE_LIMIT, 200);
}

#[test]
fn clamp_limit_floors_zero_and_negative() {
    assert_eq!(clamp_limit(Some(0)), 1);
    assert_eq!(clamp_limit(Some(-5)), 1);
}

#[test]
fn clamp_limit_passes_through_in_range() {
    assert_eq!(clamp_limit(Some(1)), 1);
    assert_eq!(clamp_limit(Some(50)), 50);
    assert_eq!(clamp_limit(Some(200)), 200);
}

#[test]
fn token_round_trips_through_opaque_base64() {
    let cursor = json!({ "b": 7 });
    let token = encode_token(&cursor);
    // Opaque: not a bare number, and base64-decodes back to the cursor.
    assert!(token.parse::<u64>().is_err());
    assert_eq!(decode_token(&token), Some(cursor));
}

#[test]
fn malformed_token_decodes_to_none() {
    assert_eq!(decode_token("not valid base64!!!"), None);
    assert_eq!(decode_token(""), None);
}

#[test]
fn first_page_returns_newest_items_newest_first() {
    let source: Vec<i32> = (0..10).collect(); // oldest..newest
    let page = paginate_slice(&source, Some(3), None);
    assert_eq!(page.items, vec![9, 8, 7]);
    assert!(page.next_token.is_some());
}

#[test]
fn paging_walks_to_exhaustion() {
    let source: Vec<i32> = (0..7).collect();
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = paginate_slice(&source, Some(3), token.as_deref());
        seen.extend(page.items.clone());
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(seen, vec![6, 5, 4, 3, 2, 1, 0]);
}

#[test]
fn last_page_has_no_next_token() {
    let source: Vec<i32> = (0..4).collect();
    // Page 1 -> [3,2], page 2 -> [1,0] (exhausted).
    let p1 = paginate_slice(&source, Some(2), None);
    assert_eq!(p1.items, vec![3, 2]);
    let p2 = paginate_slice(&source, Some(2), p1.next_token.as_deref());
    assert_eq!(p2.items, vec![1, 0]);
    assert_eq!(p2.next_token, None);
}

#[test]
fn continuation_is_stable_across_appends() {
    // Page 1 over the initial collection.
    let initial: Vec<i32> = (0..6).collect(); // 0..5
    let p1 = paginate_slice(&initial, Some(2), None);
    assert_eq!(p1.items, vec![5, 4]);
    let token = p1.next_token.clone().expect("more pages");

    // New items are appended at the newest end between page fetches.
    let mut grown = initial.clone();
    grown.extend([6, 7, 8]); // 0..8

    // Following the original token still returns the same older items,
    // unaffected by the appends (append-stable / Q13).
    let p2 = paginate_slice(&grown, Some(2), Some(&token));
    assert_eq!(p2.items, vec![3, 2]);
    let p3 = paginate_slice(&grown, Some(2), p2.next_token.as_deref());
    assert_eq!(p3.items, vec![1, 0]);
    assert_eq!(p3.next_token, None);
}

#[test]
fn window_preserves_chronological_order() {
    let source: Vec<i32> = (0..10).collect();
    let win = page_window(source.len(), Some(3), None);
    assert_eq!((win.start, win.end), (7, 10));
    assert_eq!(&source[win.start..win.end], &[7, 8, 9]);
    assert!(win.next_token.is_some());
}

#[test]
fn offset_token_round_trips_opaque() {
    let token = offset_token(50);
    assert!(token.parse::<u64>().is_err());
    assert_eq!(parse_offset(Some(&token)), 50);
}

#[test]
fn offset_token_defaults_to_zero() {
    assert_eq!(parse_offset(None), 0);
    assert_eq!(parse_offset(Some("garbage!!!")), 0);
    // A boundary token (`b`) is not a valid offset token (`o`).
    let boundary = encode_token(&json!({ "b": 9 }));
    assert_eq!(parse_offset(Some(&boundary)), 0);
}

#[test]
fn stale_token_past_end_is_clamped() {
    let source: Vec<i32> = (0..3).collect();
    let token = encode_token(&json!({ "b": 99 }));
    let page = paginate_slice(&source, Some(2), Some(&token));
    assert_eq!(page.items, vec![2, 1]);
}
