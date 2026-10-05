//! Actual parser/anchor edit coordinates for composing mutation receipts.
//! Each phase addresses its own input. No text diff chooses among repeated text.
use intent_core::note_mutation::NoteSplice;
use intent_core::Comment;

/// A canonical transformation and the edits which produced it. This is an
/// internal source-planning value, not an RPC result or a serialized receipt.
#[derive(Clone, Debug)]
pub struct CanonicalSourceChange {
    pub content: String,
    pub phases: Vec<Vec<NoteSplice>>,
}

impl CanonicalSourceChange {
    pub(crate) fn unchanged(content: String) -> Self {
        Self {
            content,
            phases: Vec::new(),
        }
    }
}

/// Builders consume trusted parser byte ranges in order and count intervening
/// UTF-16 once. The caller's existing parser determines replacement identity.
pub(super) struct SourceEditBuilder<'a> {
    source: &'a str,
    cursor: usize,
    units: u64,
    content: String,
    edits: Vec<NoteSplice>,
}

impl<'a> SourceEditBuilder<'a> {
    pub(super) fn new(source: &'a str) -> Self {
        Self {
            source,
            cursor: 0,
            units: 0,
            content: String::new(),
            edits: Vec::new(),
        }
    }

    pub(super) fn replace(&mut self, start: usize, end: usize, text: &str) {
        // These ranges come from char-safe parser/match_indices positions, not
        // client offsets. Slicing also guards accidental parser range regressions.
        let untouched = &self.source[self.cursor..start];
        let removed = &self.source[start..end];
        self.content.push_str(untouched);
        self.units += untouched.encode_utf16().count() as u64;
        let start_units = self.units;
        self.units += removed.encode_utf16().count() as u64;
        self.edits.push(NoteSplice {
            start: start_units,
            end: self.units,
            text: text.into(),
        });
        self.content.push_str(text);
        self.cursor = end;
    }

    pub(super) fn finish(mut self) -> CanonicalSourceChange {
        self.content.push_str(&self.source[self.cursor..]);
        CanonicalSourceChange {
            content: self.content,
            phases: if self.edits.is_empty() {
                Vec::new()
            } else {
                vec![self.edits]
            },
        }
    }
}

/// Record the existing all-occurrences projection, including a matching literal
/// outside a parsed task fence. Empty patterns are never canonical markers.
pub(crate) fn replace_all(source: &str, pattern: &str, replacement: &str) -> CanonicalSourceChange {
    assert!(!pattern.is_empty(), "canonical marker cannot be empty");
    let mut edits = SourceEditBuilder::new(source);
    for (start, _) in source.match_indices(pattern) {
        edits.replace(start, start + pattern.len(), replacement);
    }
    edits.finish()
}

/// One ordered canonical phase, with ranges in its own input source.
#[derive(Debug)]
pub struct CanonicalPhase {
    pub reason: &'static str,
    pub edits: Vec<NoteSplice>,
}

/// Pure anchor plan shared by legacy writes and the transactional partial path.
/// The latter must read comments and persist these orphan flips in its writer
/// transaction. This value alone performs no storage or event publication.
#[derive(Debug)]
pub struct AnchorSourcePlan {
    pub content: String,
    pub phases: Vec<CanonicalPhase>,
    pub orphaned: Vec<String>,
}

impl AnchorSourcePlan {
    fn apply(&mut self, change: CanonicalSourceChange, reason: &'static str) {
        self.content = change.content;
        self.phases.extend(
            change
                .phases
                .into_iter()
                .map(|edits| CanonicalPhase { reason, edits }),
        );
    }
}

/// Plan the same recovery/scrubbing order as a legacy note write, retaining
/// actual edit addresses even when markers or surrounding text repeat.
#[must_use]
pub fn plan_anchor_changes(source: &str, comments: &[Comment]) -> AnchorSourcePlan {
    let mut plan = AnchorSourcePlan {
        content: source.into(),
        phases: Vec::new(),
        orphaned: Vec::new(),
    };
    let mut live_ids = crate::live_comment_ids(comments);
    for comment in comments {
        if comment.parent_id.is_some() || comment.is_orphaned == Some(true) {
            continue;
        }
        match super::classify_anchor_state(&plan.content, &comment.id) {
            super::AnchorState::Healthy => continue,
            super::AnchorState::Missing => {}
            super::AnchorState::PartialStartOnly | super::AnchorState::PartialEndOnly => {
                match super::recover_partial_anchor(
                    &plan.content,
                    &comment.id,
                    comment.anchor_before.as_deref(),
                    comment.anchor_after.as_deref(),
                ) {
                    super::RecoveryOutcome::Recovered(change) => {
                        plan.apply(change, "anchor-repair");
                        continue;
                    }
                    super::RecoveryOutcome::Failed(reason) => {
                        tracing::debug!(comment_id = %comment.id, reason, "partial-anchor recovery failed; orphaning comment");
                        let change = super::remove_anchor_markers(&plan.content, &comment.id);
                        plan.apply(change, "anchor-repair");
                    }
                }
            }
            super::AnchorState::Degenerate => {
                let change = super::remove_anchor_markers(&plan.content, &comment.id);
                plan.apply(change, "anchor-repair");
            }
        }
        live_ids.remove(&comment.id);
        plan.orphaned.push(comment.id.clone());
    }
    let scrub = super::scrub_phantom_anchor_markers(&plan.content, &live_ids);
    plan.apply(scrub, "phantom-scrub");
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::note_ops;
    use intent_core::note_mutation::NoteSourceHistory;
    use std::collections::HashSet;

    fn replay(source: &str, change: &CanonicalSourceChange) -> NoteSourceHistory {
        let mut history = NoteSourceHistory::new(source.into());
        for phase in &change.phases {
            history.apply_phase(phase).unwrap();
        }
        assert_eq!(history.source(), change.content);
        history
    }

    #[test]
    fn marker_removal_records_sequential_joined_marker_semantics() {
        let source = "🙂<!--an<!--anchor:c1:start-->chor:c1:end-->tail";
        let changed = note_ops::remove_anchor_markers(source, "c1");
        assert_eq!(changed.content, "🙂tail");
        assert_eq!(changed.phases.len(), 2);
        let history = replay(source, &changed);
        let mapping = history.mapping();
        assert_eq!(mapping.len(), 1);
        assert_eq!(mapping[0].start, 2);
        assert_eq!(mapping[0].end, source.encode_utf16().count() as u64 - 4);
        assert_eq!(mapping[0].inserted_length, 0);
    }

    #[test]
    fn task_parser_records_distant_unicode_ranges_without_matching_repeated_text() {
        let prefix = "same🙂\r\ne\u{301}\r\n";
        let first = "@@@task\n# same\nbody\n@@@";
        let invalid = "@@@task\n\n@@@";
        let source = format!("{prefix}{first}\r\nsame🙂\r\n{invalid}\r\n{first}tail");
        let parsed = note_ops::extract_task_blocks(&source);
        assert_eq!(parsed.tasks.len(), 2);
        assert_eq!(parsed.source_change.phases[0].len(), 3);
        let history = replay(&source, &parsed.source_change);
        assert_eq!(
            history.mapping()[0].start,
            prefix.encode_utf16().count() as u64
        );
        assert_eq!(parsed.source_change.content, format!("{prefix}<!-- task-block-placeholder-0 -->\r\nsame🙂\r\n<!-- invalid-task-block-removed -->\r\n<!-- task-block-placeholder-1 -->tail"));
    }

    #[test]
    fn task_marker_projection_records_all_legacy_matches_even_outside_fence() {
        let literal = "<!-- task-block-placeholder-0 -->";
        let source = format!("{literal}🙂@@@task\n# title\nbody\n@@@tail");
        let parsed = note_ops::extract_task_blocks(&source);
        let mut history = replay(&source, &parsed.source_change);
        let projected = replace_all(history.source(), literal, "linked");
        assert_eq!(projected.phases[0].len(), 2);
        for edits in &projected.phases {
            history.apply_phase(edits).unwrap();
        }
        assert_eq!(history.source(), "linked🙂linkedtail");
        assert_eq!(history.mapping().len(), 2);
    }

    #[test]
    fn phantom_scrub_preserves_live_markers_lookalikes_and_non_effect_bytes() {
        let live = "11111111-2222-3333-4444-555555555555";
        let phantom = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let source = format!("🙂<!--anchor:{{id}}:start--><!--anchor:{live}:start-->e\u{301}\r\n<!--anchor:{phantom}:point--><!--anchor:{live}:end-->");
        let change = note_ops::scrub_phantom_anchor_markers(&source, &HashSet::from([live.into()]));
        assert_eq!(change.phases[0].len(), 1);
        replay(&source, &change);
        assert_eq!(
            change.content,
            source.replace(&format!("<!--anchor:{phantom}:point-->"), "")
        );
    }

    #[test]
    fn partial_anchor_recovery_records_original_neighbor_address() {
        let source = "same🙂 pre <!--anchor:c1:start-->target post target post";
        let note_ops::RecoveryOutcome::Recovered(change) =
            note_ops::recover_partial_anchor(source, "c1", Some("pre "), Some(" post"))
        else {
            panic!("expected recovery")
        };
        let expected = source.find(" post").unwrap();
        assert_eq!(
            change.phases[0][0].start,
            source[..expected].encode_utf16().count() as u64
        );
        assert_eq!(change.phases[0][0].start, change.phases[0][0].end);
        replay(source, &change);
        assert!(change
            .content
            .ends_with("target<!--anchor:c1:end--> post target post"));
    }

    #[test]
    fn anchor_plan_keeps_orphan_flips_and_ordered_source_footprints_together() {
        let mut comment: Comment = serde_json::from_value(serde_json::json!({
            "id":"c1","threadId":"c1","type":"comment","content":"body",
            "author":"User","authorType":"user","status":"open",
            "createdAt":"2026-10-05T00:00:00.000Z","updatedAt":"2026-10-05T00:00:00.000Z"
        }))
        .unwrap();
        comment.anchor_after = Some(" post".into());
        let mut missing = comment.clone();
        missing.id = "missing".into();
        let phantom = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let source = format!("🙂<!--anchor:c1:start-->target post<!--anchor:{phantom}:point-->");
        let plan = plan_anchor_changes(&source, &[comment, missing]);
        assert_eq!(plan.orphaned, ["missing"]);
        assert_eq!(
            plan.phases
                .iter()
                .map(|phase| phase.reason)
                .collect::<Vec<_>>(),
            ["anchor-repair", "phantom-scrub"]
        );
        let mut history = NoteSourceHistory::new(source);
        for phase in &plan.phases {
            history.apply_phase(&phase.edits).unwrap();
        }
        assert_eq!(history.source(), plan.content);
        assert_eq!(
            plan.content,
            "🙂<!--anchor:c1:start-->target<!--anchor:c1:end--> post"
        );
    }
}
