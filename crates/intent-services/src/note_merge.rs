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

use similar::{capture_diff_slices, Algorithm, DiffOp};

/// Result of [`three_way_merge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeOutcome {
    /// Merged text.
    pub text: String,
    /// Number of base spans where both writers made different edits (each
    /// rendered as current-variant followed by incoming-variant).
    pub conflicting_spans: usize,
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
        };
    }
    if base == incoming {
        return MergeOutcome {
            text: current.to_string(),
            conflicting_spans: 0,
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
                out.extend_from_slice(&cur);
                if cur != inc {
                    out.extend_from_slice(&inc);
                    // Distinct insertions at one offset both apply in order;
                    // only a rewritten base span counts as a conflict.
                    if end > start {
                        conflicting_spans += 1;
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

    MergeOutcome {
        text: out.into_iter().collect(),
        conflicting_spans,
    }
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
}
