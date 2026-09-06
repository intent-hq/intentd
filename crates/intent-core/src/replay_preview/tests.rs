//! Unit tests for the shared replay-preview truncation helpers.

use super::{
    retruncate_replay_preview, truncate_marked, truncate_middle_content, MARKER_FIXED_CHARS,
    TRUNCATION_MARKER_RESERVE_CHARS,
};

#[test]
fn marker_fixed_chars_matches_rendered_marker() {
    let rendered = super::marker(7);
    assert_eq!(rendered, "\n... [7 characters truncated] ...\n");
    assert_eq!(rendered.chars().count(), MARKER_FIXED_CHARS + 1);
    assert_eq!(TRUNCATION_MARKER_RESERVE_CHARS, 60);
}

#[test]
fn under_cap_text_passes_through_unmarked() {
    let text = "x".repeat(4000);
    assert_eq!(truncate_middle_content(&text, 4000), text);
    assert_eq!(truncate_marked(&text, 4000), (text.clone(), None));
}

#[test]
fn over_cap_text_keeps_equal_head_and_tail_around_marker() {
    // 4000 cap, 60 reserved → 1970-char halves; 5000 - 3940 = 1060 omitted.
    let text = format!("{}{}", "h".repeat(2500), "t".repeat(2500));
    let (out, original) = truncate_marked(&text, 4000);
    assert_eq!(original, Some(5000));
    assert_eq!(
        out,
        format!(
            "{}\n... [1060 characters truncated] ...\n{}",
            "h".repeat(1970),
            "t".repeat(1970)
        )
    );
    assert_eq!(out, truncate_middle_content(&text, 4000));
}

#[test]
fn counts_chars_not_bytes() {
    let multibyte = "é".repeat(5000);
    assert_eq!(multibyte.len(), 10_000);
    let (out, original) = truncate_marked(&multibyte, 4000);
    assert_eq!(original, Some(5000));
    assert!(out.contains("\n... [1060 characters truncated] ...\n"));
    assert_eq!(out.chars().count(), 1970 * 2 + MARKER_FIXED_CHARS + 4);
}

#[test]
fn tiny_cap_without_marker_room_keeps_head_only() {
    let text = "abcdefghij";
    assert_eq!(truncate_middle_content(text, 5), "abcde");
    assert_eq!(truncate_marked(text, 5), ("abcde".to_string(), Some(10)));
}

fn body(len: usize) -> String {
    (0..len)
        .map(|i| char::from(b'a' + u8::try_from(i % 26).unwrap()))
        .collect()
}

#[test]
fn preview_at_same_cap_renders_byte_identically_to_full_body() {
    let full = body(9_137);
    let (preview, original) = truncate_marked(&full, 4000);
    assert_eq!(
        retruncate_replay_preview(&preview, original.unwrap(), 4000),
        truncate_marked(&full, 4000)
    );
}

#[test]
fn preview_at_larger_cap_is_retruncated_to_the_smaller_cap() {
    let full = body(20_001);
    let (preview, original) = truncate_marked(&full, 12_000);
    for cap in [500usize, 4000, 4001, 11_999] {
        assert_eq!(
            retruncate_replay_preview(&preview, original.unwrap(), cap),
            truncate_marked(&full, cap),
            "cap {cap}"
        );
    }
}

#[test]
fn preview_at_smaller_cap_is_never_expanded() {
    let full = body(20_000);
    let (preview, original) = truncate_marked(&full, 4000);
    // Cap grew but stays under the body: keep the stored preview, still marked.
    assert_eq!(
        retruncate_replay_preview(&preview, 20_000, 8000),
        (preview.clone(), original)
    );
    // Cap grew past the whole body: still cannot expand, still marked.
    assert_eq!(
        retruncate_replay_preview(&preview, 20_000, 100_000),
        (preview.clone(), Some(20_000))
    );
}

#[test]
fn preview_holding_the_whole_body_follows_the_full_body_path() {
    let full = body(3000);
    let (preview, original) = truncate_marked(&full, 4000);
    assert_eq!(original, None);
    assert_eq!(
        retruncate_replay_preview(&preview, 3000, 4000),
        (full.clone(), None)
    );
    assert_eq!(
        retruncate_replay_preview(&preview, 3000, 1000),
        truncate_marked(&full, 1000)
    );
}

#[test]
fn preview_with_a_marker_lookalike_in_its_body_still_matches_the_real_marker() {
    let lookalike = "\n... [12 characters truncated] ...\n";
    let full = format!("{lookalike}{}{lookalike}", body(12_000));
    let (preview, original) = truncate_marked(&full, 8000);
    assert_eq!(
        retruncate_replay_preview(&preview, original.unwrap(), 4000),
        truncate_marked(&full, 4000)
    );
}

#[test]
fn unparseable_preview_passes_through_marked() {
    let garbage = "not a middle-truncated preview";
    assert_eq!(
        retruncate_replay_preview(garbage, 50_000, 4000),
        (garbage.to_string(), Some(50_000))
    );
}
