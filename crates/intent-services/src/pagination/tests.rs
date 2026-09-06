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
    assert_eq!(backward_page_boundary(&token), Some(7));
}

#[test]
fn malformed_token_decodes_to_none() {
    assert_eq!(decode_token("not valid base64!!!"), None);
    assert_eq!(decode_token(""), None);
    assert_eq!(backward_page_boundary("not valid base64!!!"), None);
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

#[test]
fn paginate_text_lines_returns_newest_lines_first_with_token() {
    let text = "l0\nl1\nl2\nl3\nl4\nl5";
    let env = paginate_text_lines(text, Some(3), None);
    assert_eq!(
        env["items"],
        json!(["l5", "l4", "l3"]),
        "first page must be newest-first"
    );
    let token = env["nextToken"].as_str().expect("more pages remain");
    let env2 = paginate_text_lines(text, Some(3), Some(token));
    assert_eq!(env2["items"], json!(["l2", "l1", "l0"]));
    // Older page exhausted → no further token.
    assert_eq!(env2["nextToken"], json!(null));
}

#[test]
fn paginate_text_lines_trims_trailing_blank_lines() {
    // Trailing blank/whitespace-only lines are dropped before paging so a
    // terminal scrollback's final newline doesn't surface as an empty item.
    let text = "alpha\nbeta\n   \n\n";
    let env = paginate_text_lines(text, Some(50), None);
    assert_eq!(env["items"], json!(["beta", "alpha"]));
    assert_eq!(env["nextToken"], json!(null));
}

#[test]
fn paginate_text_lines_empty_string_yields_empty_page() {
    let env = paginate_text_lines("", Some(10), None);
    assert_eq!(env["items"], json!([]));
    assert_eq!(env["nextToken"], json!(null));
}

#[test]
fn page_window_zero_length_source_emits_empty_window() {
    // First page over an empty source: start == end == 0, no continuation token.
    let win = page_window(0, Some(10), None);
    assert_eq!((win.start, win.end), (0, 0));
    assert!(win.next_token.is_none());
}

#[test]
fn page_window_around_centers_target_mid_history() {
    // Target 10 of 0..20 with limit 4: half the budget older ([8,9]), the
    // rest at/after the target ([10,11]); both directions continue.
    let win = page_window_around(20, Some(4), 10);
    assert_eq!((win.start, win.end), (8, 12));
    assert!(win.next_token.is_some(), "older rows remain");
    assert!(win.prev_token.is_some(), "newer rows remain");
}

#[test]
fn page_window_around_clamps_at_oldest_edge() {
    // Target near the oldest end: window pins to the start but stays full.
    let win = page_window_around(20, Some(6), 1);
    assert_eq!((win.start, win.end), (0, 6));
    assert!(win.next_token.is_none(), "nothing older than index 0");
    assert!(win.prev_token.is_some());
}

#[test]
fn page_window_around_clamps_at_newest_edge() {
    // Target near the live tail: window pins to the end but stays full, and
    // no prev token is minted once the newest row is inside the page.
    let win = page_window_around(20, Some(6), 19);
    assert_eq!((win.start, win.end), (14, 20));
    assert!(win.next_token.is_some());
    assert!(win.prev_token.is_none(), "newest row already in page");
}

#[test]
fn page_window_around_small_history_fits_one_page() {
    let win = page_window_around(3, Some(10), 1);
    assert_eq!((win.start, win.end), (0, 3));
    assert!(win.next_token.is_none());
    assert!(win.prev_token.is_none());
}

#[test]
fn seek_next_token_resolves_through_standard_backward_paging() {
    // The seek page's nextToken is an ordinary backward cursor: feeding it to
    // page_window continues into strictly older rows.
    let seek = page_window_around(20, Some(4), 10);
    let older = page_window(20, Some(4), seek.next_token.as_deref());
    assert_eq!((older.start, older.end), (4, 8));
}

#[test]
fn forward_page_window_walks_newer_to_the_live_tail() {
    // Following prevToken pages toward newest; at the tail no prev remains.
    let seek = page_window_around(10, Some(4), 4);
    assert_eq!((seek.start, seek.end), (2, 6));
    let fwd = forward_page_window(10, Some(4), seek.prev_token.as_deref().unwrap())
        .expect("forward cursor");
    assert_eq!((fwd.start, fwd.end), (6, 10));
    assert!(fwd.prev_token.is_none(), "reached the newest row");
    assert!(
        fwd.next_token.is_some(),
        "backward continuation still minted"
    );
}

#[test]
fn forward_page_window_is_append_stable() {
    // The forward cursor indexes from the oldest end, so appends at the
    // newest end never shift the rows a minted token resolves to (Q13).
    let seek = page_window_around(10, Some(4), 4);
    let token = seek.prev_token.clone().unwrap();
    let fwd = forward_page_window(15, Some(4), &token).expect("forward cursor");
    assert_eq!((fwd.start, fwd.end), (6, 10));
    assert!(fwd.prev_token.is_some(), "appended rows are newer pages");
}

#[test]
fn forward_page_window_rejects_backward_and_malformed_tokens() {
    // Backward (`b`) and garbage tokens are not forward cursors: callers fall
    // through to the legacy page_window contract.
    let backward = encode_token(&json!({ "b": 6 }));
    assert!(forward_page_window(10, Some(4), &backward).is_none());
    assert!(forward_page_window(10, Some(4), "garbage!!!").is_none());
}

#[test]
fn forward_page_window_clamps_stale_cursor_past_end() {
    // A forward cursor minted against a longer (since-pruned) history
    // degrades to an empty tail page rather than panicking.
    let token = encode_token(&json!({ "f": 99 }));
    let fwd = forward_page_window(5, Some(4), &token).expect("forward cursor");
    assert_eq!((fwd.start, fwd.end), (5, 5));
    assert!(fwd.prev_token.is_none());
}

#[test]
fn budget_page_newest_keeps_newest_suffix() {
    // Anchor Newest: admit newest-first until the budget stops an older row.
    let sizes = [100, 100, 100, 100];
    assert_eq!(budget_page(&sizes, BudgetAnchor::Newest, 250), (2, 4));
    // Everything fits: whole page kept.
    assert_eq!(budget_page(&sizes, BudgetAnchor::Newest, 400), (0, 4));
    // The anchor (newest) row always serves, even alone over budget.
    assert_eq!(budget_page(&sizes, BudgetAnchor::Newest, 50), (3, 4));
    assert_eq!(budget_page(&[], BudgetAnchor::Newest, 50), (0, 0));
}

#[test]
fn budget_page_oldest_keeps_oldest_prefix() {
    let sizes = [100, 100, 100, 100];
    assert_eq!(budget_page(&sizes, BudgetAnchor::Oldest, 250), (0, 2));
    assert_eq!(budget_page(&sizes, BudgetAnchor::Oldest, 400), (0, 4));
    assert_eq!(budget_page(&sizes, BudgetAnchor::Oldest, 50), (0, 1));
}

#[test]
fn budget_page_target_grows_outward_and_always_keeps_target() {
    let sizes = [100, 100, 100, 100, 100];
    // Target in the middle, room for two neighbors (older admitted first).
    assert_eq!(budget_page(&sizes, BudgetAnchor::Target(2), 320), (1, 4));
    // Only the target fits.
    assert_eq!(budget_page(&sizes, BudgetAnchor::Target(2), 100), (2, 3));
    // A lone over-budget target still serves (never an empty page).
    assert_eq!(budget_page(&sizes, BudgetAnchor::Target(2), 10), (2, 3));
    // Each direction stops independently: a huge older neighbor doesn't
    // block newer admission.
    let uneven = [1000, 10, 10, 10];
    assert_eq!(budget_page(&uneven, BudgetAnchor::Target(1), 40), (1, 4));
    // Position clamps into the slice.
    assert_eq!(budget_page(&sizes, BudgetAnchor::Target(99), 100), (4, 5));
}

#[test]
fn budget_page_reminted_tokens_resume_at_first_excluded_row() {
    // A budget trim on a backward page re-mints `{"b": start+lo}`; following
    // it through page_window resumes exactly at the first excluded (older)
    // row — no gaps, no duplicates.
    let sizes = [100, 100, 100, 100];
    let (lo, _hi) = budget_page(&sizes, BudgetAnchor::Newest, 250);
    let token = remint_backward_token(lo).expect("older rows remain");
    let next = page_window(4, Some(4), Some(&token));
    assert_eq!((next.start, next.end), (0, 2), "resumes at first excluded");
    assert!(
        remint_backward_token(0).is_none(),
        "no token at oldest edge"
    );
    // Forward re-mint: `{"f": end}` resumes at the first excluded newer row.
    let fwd_token = remint_forward_token(2, 4).expect("newer rows remain");
    let fwd = forward_page_window(4, Some(4), &fwd_token).expect("forward");
    assert_eq!((fwd.start, fwd.end), (2, 4));
    assert!(
        remint_forward_token(4, 4).is_none(),
        "no token at newest edge"
    );
}
