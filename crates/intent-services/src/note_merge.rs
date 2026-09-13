//! Pure three-way character merge for `note.setContent`.
//!
//! Given *base* (the writer's baseline), *current* (the stored text) and
//! *incoming* (the writer's new text), the writer's intent `diff(base →
//! incoming)` is applied onto *current*. Hunks are computed per Unicode scalar
//! value with [`similar::capture_diff_slices`] (Myers), so a merge can never
//! split a surrogate pair or drop a code point.
//!
//! Rules:
//! - Non-overlapping hunks from either side apply.
//! - Hunks from both sides that overlap the same base span form one
//!   conflicting cluster: the result carries the *current* variant of that
//!   span immediately followed by the *incoming* variant. Nothing is dropped
//!   and no markers are inserted.
//! - Identical edits on both sides apply once and are not a conflict.
//! - Pure insertions at the same base offset both apply, current's first.

use std::ops::Range;

use similar::{capture_diff_slices, Algorithm, DiffOp};

/// Result of [`three_way_merge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeOutcome {
    /// Merged text.
    pub text: String,
    /// Number of base spans where both writers made different edits (each
    /// rendered as current-variant followed by incoming-variant).
    pub conflicting_spans: usize,
    /// Byte range in `text` of each conflicting span's rendering (the
    /// concatenated current and incoming variants), in order; one entry per
    /// counted conflict.
    pub conflict_ranges: Vec<Range<usize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    Current,
    Incoming,
}

/// One change relative to `base`: base chars `start..end` become
/// `replacement`. A pure insertion has `start == end`.
#[derive(Debug)]
struct Hunk {
    start: usize,
    end: usize,
    side: Side,
    replacement: Vec<char>,
}

fn hunks(base: &[char], other: &[char], side: Side) -> Vec<Hunk> {
    capture_diff_slices(Algorithm::Myers, base, other)
        .into_iter()
        .filter_map(|op| match op {
            DiffOp::Equal { .. } => None,
            DiffOp::Delete {
                old_index, old_len, ..
            } => Some(Hunk {
                start: old_index,
                end: old_index + old_len,
                side,
                replacement: Vec::new(),
            }),
            DiffOp::Insert {
                old_index,
                new_index,
                new_len,
            } => Some(Hunk {
                start: old_index,
                end: old_index,
                side,
                replacement: other[new_index..new_index + new_len].to_vec(),
            }),
            DiffOp::Replace {
                old_index,
                old_len,
                new_index,
                new_len,
            } => Some(Hunk {
                start: old_index,
                end: old_index + old_len,
                side,
                replacement: other[new_index..new_index + new_len].to_vec(),
            }),
        })
        .collect()
}

/// Apply one side's hunks of a cluster to `base[start..end]`.
fn variant(cluster: &[Hunk], side: Side, base: &[char], start: usize, end: usize) -> Vec<char> {
    let mut out = Vec::new();
    let mut cursor = start;
    for h in cluster.iter().filter(|h| h.side == side) {
        out.extend_from_slice(&base[cursor..h.start]);
        out.extend_from_slice(&h.replacement);
        cursor = h.end;
    }
    out.extend_from_slice(&base[cursor..end]);
    out
}

/// Merge `incoming`'s edits (relative to `base`) onto `current`.
pub(crate) fn three_way_merge(base: &str, current: &str, incoming: &str) -> MergeOutcome {
    if base == current || current == incoming {
        return MergeOutcome {
            text: incoming.to_string(),
            conflicting_spans: 0,
            conflict_ranges: Vec::new(),
        };
    }
    if base == incoming {
        return MergeOutcome {
            text: current.to_string(),
            conflicting_spans: 0,
            conflict_ranges: Vec::new(),
        };
    }

    let base: Vec<char> = base.chars().collect();
    let current: Vec<char> = current.chars().collect();
    let incoming: Vec<char> = incoming.chars().collect();

    let mut all = hunks(&base, &current, Side::Current);
    all.extend(hunks(&base, &incoming, Side::Incoming));
    all.sort_by_key(|h| (h.start, h.end, h.side));

    let mut out: Vec<char> = Vec::with_capacity(current.len().max(incoming.len()));
    let mut conflicting_spans = 0;
    // char ranges in `out`; mapped to byte ranges once the text is built
    let mut conflict_chars: Vec<Range<usize>> = Vec::new();
    let mut copied = 0; // base chars emitted so far
    let mut i = 0;
    while i < all.len() {
        let start = all[i].start;
        let mut end = all[i].end;
        let mut j = i + 1;
        while j < all.len() {
            let h = &all[j];
            let overlaps = h.start < end;
            // A same-side hunk touching the cluster end is separated from its
            // predecessor only by an Equal run the other side rewrote; keep
            // that writer's text contiguous.
            let continues = h.start == end && all[i..j].iter().any(|c| c.side == h.side);
            // Pure insertions at one offset are compared so an identical
            // insertion on both sides applies once.
            let same_offset_insertion = h.start == start && h.end == start && end == start;
            if !(overlaps || continues || same_offset_insertion) {
                break;
            }
            end = end.max(h.end);
            j += 1;
        }
        let cluster = &all[i..j];

        out.extend_from_slice(&base[copied..start]);
        let has_cur = cluster.iter().any(|h| h.side == Side::Current);
        let has_inc = cluster.iter().any(|h| h.side == Side::Incoming);
        match (has_cur, has_inc) {
            (true, true) => {
                let cur = variant(cluster, Side::Current, &base, start, end);
                let inc = variant(cluster, Side::Incoming, &base, start, end);
                let rendered_at = out.len();
                out.extend_from_slice(&cur);
                if cur != inc {
                    out.extend_from_slice(&inc);
                    // Distinct insertions at one offset both apply in order;
                    // only a rewritten base span counts as a conflict.
                    if end > start {
                        conflicting_spans += 1;
                        conflict_chars.push(rendered_at..out.len());
                    }
                }
            }
            (true, false) => out.extend(variant(cluster, Side::Current, &base, start, end)),
            (false, _) => out.extend(variant(cluster, Side::Incoming, &base, start, end)),
        }
        copied = end;
        i = j;
    }
    out.extend_from_slice(&base[copied..]);

    let conflict_ranges = if conflict_chars.is_empty() {
        Vec::new()
    } else {
        // byte offset of every char boundary in `out`, plus the end
        let mut byte_at = Vec::with_capacity(out.len() + 1);
        let mut bytes = 0;
        for c in &out {
            byte_at.push(bytes);
            bytes += c.len_utf8();
        }
        byte_at.push(bytes);
        conflict_chars
            .into_iter()
            .map(|r| byte_at[r.start]..byte_at[r.end])
            .collect()
    };

    MergeOutcome {
        text: out.into_iter().collect(),
        conflicting_spans,
        conflict_ranges,
    }
}

/// Whitespace `note_ops::match_task_line` skips around a bullet.
fn is_js_space_char(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{000b}' | '\u{000c}')
}

/// Checkbox characters `note_ops::match_task_line` accepts inside `[ ]`.
fn is_marker_char(c: char) -> bool {
    matches!(c, ' ' | 'x' | 'X' | '/')
}

/// Collapse checkbox markers a conflicting [`three_way_merge`] left with both
/// variants concatenated (`- [x ] …`, `- [x/] …`) back to one valid marker.
/// The kept character is the first one — the *current* side's, since a
/// conflicting span renders current-variant then incoming-variant. Only a
/// marker whose bracket run overlaps one of `conflicts` (the merge's
/// `conflict_ranges`, byte ranges in `merged`) is repaired, so text neither
/// writer contended — including such a shape sitting in a code block — is
/// left byte-identical, as are lines whose marker already parses, non-bullet
/// lines and brackets holding anything but marker characters. Returns the
/// text and the number of repaired lines.
pub(crate) fn repair_checkbox_markers(merged: &str, conflicts: &[Range<usize>]) -> (String, usize) {
    if conflicts.is_empty() {
        return (merged.to_string(), 0);
    }
    let mut repaired = 0;
    let mut out = String::with_capacity(merged.len());
    let mut line_start = 0;
    for (i, line) in merged.split('\n').enumerate() {
        if i > 0 {
            out.push('\n');
        }
        match collapse_marker(line) {
            Some((fixed, run)) => {
                let run = line_start + run.start..line_start + run.end;
                if conflicts
                    .iter()
                    .any(|c| c.start < run.end && run.start < c.end)
                {
                    repaired += 1;
                    out.push_str(&fixed);
                } else {
                    out.push_str(line);
                }
            }
            None => out.push_str(line),
        }
        line_start += line.len() + 1;
    }
    (out, repaired)
}

/// `line` with a `[` + two-or-more marker chars + `]` bullet marker reduced
/// to its first char, plus the byte range of that bracket run in `line`;
/// `None` when the line needs no repair.
fn collapse_marker(line: &str) -> Option<(String, Range<usize>)> {
    let mut it = line.char_indices().peekable();
    while matches!(it.peek(), Some((_, c)) if is_js_space_char(*c)) {
        it.next();
    }
    match it.next() {
        Some((_, '-' | '*')) => {}
        _ => return None,
    }
    while matches!(it.peek(), Some((_, c)) if is_js_space_char(*c)) {
        it.next();
    }
    let (box_start, '[') = it.next()? else {
        return None;
    };
    let (_, first) = it.next()?;
    if !is_marker_char(first) {
        return None;
    }
    let mut markers = 1;
    let close = loop {
        match it.next()? {
            (idx, ']') => break idx,
            (_, c) if is_marker_char(c) => markers += 1,
            _ => return None,
        }
    };
    if markers < 2 {
        return None;
    }
    let fixed = format!("{}[{first}]{}", &line[..box_start], &line[close + 1..]);
    Some((fixed, box_start..close + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge(base: &str, current: &str, incoming: &str) -> MergeOutcome {
        three_way_merge(base, current, incoming)
    }

    fn clean(base: &str, current: &str, incoming: &str, expected: &str) {
        let out = merge(base, current, incoming);
        assert_eq!(out.text, expected);
        assert_eq!(out.conflicting_spans, 0, "unexpected conflict in {out:?}");
    }

    fn conflict(base: &str, current: &str, incoming: &str, expected: &str) {
        let out = merge(base, current, incoming);
        assert_eq!(out.text, expected);
        assert_eq!(out.conflicting_spans, 1, "expected one conflict in {out:?}");
    }

    // --- fast paths ---------------------------------------------------------

    #[test]
    fn base_equals_current_returns_incoming() {
        clean("abc", "abc", "xyz", "xyz");
    }

    #[test]
    fn base_equals_incoming_returns_current() {
        clean("abc", "xyz", "abc", "xyz");
    }

    #[test]
    fn current_equals_incoming_returns_current() {
        clean("abc", "xyz", "xyz", "xyz");
    }

    // --- empty base -----------------------------------------------------------

    #[test]
    fn empty_base_concatenates_current_then_incoming() {
        clean("", "abc", "xyz", "abcxyz");
        clean("", "", "x", "x");
        clean("", "x", "", "x");
        clean("", "🦀", "😀", "🦀😀");
    }

    // --- disjoint edits -------------------------------------------------------

    #[test]
    fn disjoint_edits_in_one_paragraph() {
        clean(
            "The quick brown fox jumps over the lazy dog.",
            "The slow brown fox jumps over the lazy dog.",
            "The quick brown fox jumps over the sleepy dog.",
            "The slow brown fox jumps over the sleepy dog.",
        );
    }

    #[test]
    fn disjoint_edits_with_emoji_at_boundaries() {
        clean(
            "😀 quick 🦊 jumps over 🐶 lazy 🎉",
            "😀 slow 🦊 jumps over 🐶 lazy 🎉",
            "😀 quick 🦊 jumps over 🐶 sleepy 🎉",
            "😀 slow 🦊 jumps over 🐶 sleepy 🎉",
        );
        clean("a🦊b", "a🦀🦊b", "a🦊🐶b", "a🦀🦊🐶b");
    }

    #[test]
    fn same_offset_insertions_apply_current_first() {
        clean("ab", "aXb", "aYb", "aXYb");
        clean("😀🎉", "😀🦀🎉", "😀🐶🎉", "😀🦀🐶🎉");
    }

    // --- edit adjacent to an insertion ----------------------------------------

    #[test]
    fn edit_adjacent_to_insertion_both_sides() {
        // insertion right after the other writer's replaced span
        clean("abc def", "xyz def", "abc! def", "xyz! def");
        clean("abc def", "abc! def", "xyz def", "xyz! def");
        // insertion right before the other writer's replaced span
        clean("abc def", "abc ghi", "abc !def", "abc !ghi");
        clean("abc def", "abc !def", "abc ghi", "abc !ghi");
    }

    #[test]
    fn edit_adjacent_to_insertion_with_emoji() {
        clean("🦊🦊🦊 def", "🐶🐶🐶 def", "🦊🦊🦊🎉 def", "🐶🐶🐶🎉 def");
        clean("🦊🦊🦊 def", "🦊🦊🦊🎉 def", "🐶🐶🐶 def", "🐶🐶🐶🎉 def");
        clean("abc 🦊🦊", "abc 🐶🐶", "abc 🎉🦊🦊", "abc 🎉🐶🐶");
        clean("abc 🦊🦊", "abc 🎉🦊🦊", "abc 🐶🐶", "abc 🎉🐶🐶");
        // insertions flanking one emoji are disjoint, not a conflict
        clean("x 🦊 y", "x cat🦊 y", "x 🦊dog y", "x cat🦊dog y");
    }

    // --- same-span conflict ---------------------------------------------------

    #[test]
    fn same_span_conflict_keeps_current_then_incoming() {
        conflict(
            "one cat three",
            "one dog three",
            "one fox three",
            "one dogfox three",
        );
        // Myers aligns the shared 'o' of two/four; the writer's text stays contiguous.
        conflict(
            "one two three",
            "one four three",
            "one five three",
            "one fourfive three",
        );
        conflict("W", "A", "B", "AB");
    }

    #[test]
    fn same_span_conflict_with_emoji() {
        conflict(
            "one 🦊🦊 three",
            "one 🐶🐶 three",
            "one 🦀🦀 three",
            "one 🐶🐶🦀🦀 three",
        );
        conflict("🦊", "🐶", "🦀", "🐶🦀");
        // Myers keeps the shared 🐶; current's trailing 🎉 stays with its writer's text.
        conflict(
            "one 🦊🐶 three",
            "one 🦀🐶🎉 three",
            "one 🍎🍎 three",
            "one 🦀🐶🎉🍎🍎 three",
        );
    }

    // --- deletion spanning the other writer's insertion -----------------------

    #[test]
    fn deletion_spanning_insertion_keeps_insertion() {
        let base = "keep this whole sentence here";
        let deleted = "keep here";
        let inserted = "keep this entire whole sentence here";
        conflict(base, deleted, inserted, inserted);
        conflict(base, inserted, deleted, inserted);
    }

    #[test]
    fn deletion_spanning_insertion_with_emoji() {
        let base = "🎉 keep 🦊🦊🦊 here 🎉";
        let deleted = "🎉 keep here 🎉";
        let inserted = "🎉 keep 🦊🐶🦊🦊 here 🎉";
        conflict(base, deleted, inserted, inserted);
        conflict(base, inserted, deleted, inserted);
    }

    // --- identical edits ------------------------------------------------------

    #[test]
    fn identical_edit_on_both_sides_applies_once() {
        clean(
            "alpha beta gamma",
            "alpha BETA gamma delta",
            "ALPHA BETA gamma",
            "ALPHA BETA gamma delta",
        );
        clean("abc", "aXc!", "aXc", "aXc!");
    }

    #[test]
    fn identical_insertion_on_both_sides_applies_once() {
        clean("abc", "aXbc!", "aXbc", "aXbc!");
        clean("abc", "aXbc", "aXbc!", "aXbc!");
        clean("abc", "!aXbc", "aXbc?", "!aXbc?");
        clean("a c", "a new c", "a new c.", "a new c.");
    }

    #[test]
    fn identical_insertion_with_emoji() {
        clean("a🦊c", "a🐶🦊c🎉", "a🐶🦊c", "a🐶🦊c🎉");
        clean("a🦊c", "a🐶🦊c", "a🐶🦊c🎉", "a🐶🦊c🎉");
        clean("😀🎉", "😀🦀🎉", "😀🦀🎉!", "😀🦀🎉!");
    }

    #[test]
    fn identical_edit_with_emoji() {
        clean(
            "alpha 🦊 gamma",
            "alpha 🐶 gamma 🎉",
            "😀 alpha 🐶 gamma",
            "😀 alpha 🐶 gamma 🎉",
        );
        clean("a🦊c", "a🐶c🎉", "a🐶c", "a🐶c🎉");
    }

    // --- structure ------------------------------------------------------------

    #[test]
    fn multiple_conflicts_are_counted_separately() {
        let out = merge("aaa bbb ccc", "xxx bbb yyy", "zzz bbb www");
        assert_eq!(out.text, "xxxzzz bbb yyywww");
        assert_eq!(out.conflicting_spans, 2);
    }

    #[test]
    fn nothing_else_changes_around_a_conflict() {
        let base = "para one.\n\npara two has W in it.\n\npara three.";
        let out = merge(
            base,
            "para one.\n\npara two has X in it.\n\npara three.",
            "para one.\n\npara two has Y in it.\n\npara three.",
        );
        assert_eq!(
            out.text,
            "para one.\n\npara two has XY in it.\n\npara three."
        );
        assert_eq!(out.conflicting_spans, 1);
    }

    // --- conflict ranges --------------------------------------------------------

    #[test]
    fn conflict_ranges_cover_each_rendered_conflict() {
        let out = merge("one cat three", "one dog three", "one fox three");
        assert_eq!(out.conflict_ranges, vec![4..10]);
        assert_eq!(&out.text[4..10], "dogfox");

        let out = merge("aaa bbb ccc", "xxx bbb yyy", "zzz bbb www");
        assert_eq!(out.conflict_ranges, vec![0..6, 11..17]);
        assert_eq!(&out.text[0..6], "xxxzzz");
        assert_eq!(&out.text[11..17], "yyywww");

        // byte ranges, not char ranges
        let out = merge("one 🦊🦊 three", "one 🐶🐶 three", "one 🦀🦀 three");
        assert_eq!(out.conflict_ranges, vec![4..20]);
        assert_eq!(&out.text[4..20], "🐶🐶🦀🦀");

        // clean merges, fast paths and same-offset insertions report none
        assert!(merge("abc", "abc", "xyz").conflict_ranges.is_empty());
        assert!(merge("ab", "aXb", "aYb").conflict_ranges.is_empty());
        assert!(merge("abc def", "xyz def", "abc! def")
            .conflict_ranges
            .is_empty());
    }

    // --- checkbox marker repair -----------------------------------------------

    use crate::note_ops::{parse_tasks, set_linked_checkbox};

    fn repair(out: &MergeOutcome) -> (String, usize) {
        repair_checkbox_markers(&out.text, &out.conflict_ranges)
    }

    /// Regression for intent-hq/intent#4930: both writers changed the single
    /// character inside `[ ]`, the merger concatenated the variants
    /// (current first) and the line stopped parsing as a task.
    #[test]
    fn regression_intent_4930_conflicting_marker_collapses_to_current_side() {
        let out = merge("- [ ] t", "- [x] t", "- [/] t");
        assert_eq!(out.text, "- [x/] t", "current variant is emitted first");
        assert_eq!(out.conflicting_spans, 1);
        assert_eq!(out.conflict_ranges, vec![3..5]);
        assert!(
            parse_tasks(&out.text).is_empty(),
            "malformed marker must not parse"
        );

        let (repaired, count) = repair(&out);
        assert_eq!(repaired, "- [x] t");
        assert_eq!(count, 1);
        let rows = parse_tasks(&repaired);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "done");
        assert_eq!(rows[0].text, "t");

        // The issue's exact shape: in-progress base, materialized `[x]`, stale `[ ]`.
        let out = merge("- [/] t", "- [x] t", "- [ ] t");
        assert_eq!(out.text, "- [x ] t");
        assert_eq!(repair(&out), ("- [x] t".to_string(), 1));
    }

    #[test]
    fn repaired_linked_task_line_stays_linked_and_materializable() {
        let base = "- [/] [T](intent://local/task/abc)";
        let out = merge(
            base,
            "- [x] [T](intent://local/task/abc)",
            "- [ ] [T](intent://local/task/abc)",
        );
        assert_eq!(out.text, "- [x ] [T](intent://local/task/abc)");
        assert_eq!(out.conflicting_spans, 1);
        assert!(set_linked_checkbox(&out.text, "abc", "[/]").is_none());

        let (repaired, count) = repair(&out);
        assert_eq!(repaired, "- [x] [T](intent://local/task/abc)");
        assert_eq!(count, 1);
        let rows = parse_tasks(&repaired);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].task_note_id.as_deref(), Some("abc"));
        assert_eq!(
            set_linked_checkbox(&repaired, "abc", "[/]").as_deref(),
            Some("- [/] [T](intent://local/task/abc)")
        );
    }

    #[test]
    fn agreeing_marker_with_conflict_elsewhere_on_the_line_is_untouched() {
        let out = merge("- [ ] cat", "- [x] dog", "- [x] fox");
        assert_eq!(out.text, "- [x] dogfox");
        assert_eq!(out.conflicting_spans, 1);
        assert_eq!(repair(&out), (out.text.clone(), 0));
    }

    #[test]
    fn non_conflicting_merge_is_never_altered() {
        let out = merge("- [ ] t", "- [x] t", "- [ ] t!");
        assert_eq!(out.text, "- [x] t!");
        assert_eq!(out.conflicting_spans, 0);
        assert_eq!(repair(&out), (out.text.clone(), 0));

        // Identical marker edits apply once and leave nothing to repair.
        let out = merge("- [ ] t", "- [x] t", "- [x] t");
        assert_eq!(out.text, "- [x] t");
        assert_eq!(repair(&out), (out.text.clone(), 0));
    }

    /// A conflict in prose must not license rewriting a marker-shaped line
    /// neither writer touched — a fenced code block or indented code.
    #[test]
    fn uncontended_code_lines_survive_a_conflict_elsewhere() {
        for code in [
            "```\n- [x ] unchanged code\n```",
            "~~~\n- [x ] unchanged code\n~~~",
            "    - [x ] unchanged code",
        ] {
            let base = format!("cat\n\n{code}\n");
            let current = format!("dog\n\n{code}\n");
            let incoming = format!("fox\n\n{code}\n");
            let out = merge(&base, &current, &incoming);
            assert_eq!(out.text, format!("dogfox\n\n{code}\n"));
            assert_eq!(out.conflicting_spans, 1);
            assert_eq!(out.conflict_ranges, vec![0..6]);
            assert_eq!(repair(&out), (out.text.clone(), 0), "{code:?}");
        }
    }

    #[test]
    fn only_markers_inside_a_conflict_range_are_repaired() {
        // A real marker conflict on one line, an uncontended malformed marker on another.
        let out = merge(
            "- [ ] one\n- [x ] two",
            "- [x] one\n- [x ] two",
            "- [/] one\n- [x ] two",
        );
        assert_eq!(out.text, "- [x/] one\n- [x ] two");
        assert_eq!(out.conflict_ranges, vec![3..5]);
        assert_eq!(repair(&out), ("- [x] one\n- [x ] two".to_string(), 1));

        // Two marker conflicts on separate lines, indented and star bullets.
        let out = merge(
            "# H\n  * [ ] one\n- [/] two\nend",
            "# H\n  * [/] one\n- [ ] two\nend",
            "# H\n  * [x] one\n- [x] two\nend",
        );
        assert_eq!(out.text, "# H\n  * [/x] one\n- [ x] two\nend");
        assert_eq!(out.conflict_ranges.len(), 2, "{out:?}");
        let (repaired, count) = repair(&out);
        assert_eq!(repaired, "# H\n  * [/] one\n- [ ] two\nend");
        assert_eq!(count, 2);
        assert_eq!(parse_tasks(&repaired).len(), 2);
    }

    #[test]
    fn repair_without_conflicts_is_identity() {
        let text = "- [ ] a\n* [x] b\n  - [/] c\n- [X] d\nsee [x ] in prose\n- [x ] stale\n";
        assert_eq!(repair_checkbox_markers(text, &[]), (text.to_string(), 0));
        assert_eq!(repair_checkbox_markers("", &[]), (String::new(), 0));
    }

    #[test]
    fn repair_shapes_within_a_covering_range() {
        let text = "# H\n  * [/ ] one\n- [ x] two\n- [xX/] three\nsee [x ] prose\n- [xy] t\nend";
        let all: Vec<Range<usize>> = std::iter::once(0..text.len()).collect();
        let (repaired, count) = repair_checkbox_markers(text, &all);
        assert_eq!(
            repaired,
            "# H\n  * [/] one\n- [ ] two\n- [x] three\nsee [x ] prose\n- [xy] t\nend"
        );
        assert_eq!(count, 3);
        // A range touching only the text after the bracket run does not qualify.
        let after_run: Vec<Range<usize>> = std::iter::once(6..8).collect();
        assert_eq!(
            repair_checkbox_markers("- [x ] t", &after_run),
            ("- [x ] t".to_string(), 0)
        );
        // Partial overlap with the bracket run qualifies: the last marker
        // character (`4..5`) and the closing bracket (`5..6`) both repair.
        let last_marker_char: Vec<Range<usize>> = std::iter::once(4..5).collect();
        assert_eq!(
            repair_checkbox_markers("- [x ] t", &last_marker_char),
            ("- [x] t".to_string(), 1)
        );
        let closing_bracket: Vec<Range<usize>> = std::iter::once(5..6).collect();
        assert_eq!(
            repair_checkbox_markers("- [x ] t", &closing_bracket),
            ("- [x] t".to_string(), 1)
        );
    }
}
