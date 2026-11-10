//! Note content write paths: version provenance, the versioned content
//! persist, the `note.setContent` three-way merge resolution, the
//! truncated-read write guard (`baseContentBytes`), the shared
//! read-merge-persist loop with its edge-whitespace trim policy, and the
//! comment re-anchor pass every content write runs before it persists.
//!
//! Kept out of `lib.rs` so `scripts/changed-tests.sh` can route a change here
//! to the note end-to-end suites (`.config/e2e-test-map.txt`, intentd#3204).

use std::collections::HashSet;

use intent_core::events::NOTE_UPDATED;
use intent_core::{
    now_iso, AgentId, BoxFuture, Comment, Note, NoteId, NoteSetContentResult, NoteVersionAuthor,
    WorkspaceId,
};
use intent_store::{NoteWriteLock, Store};

use crate::{
    fetch_note_peer, note_change_event, note_merge, note_ops, publish_event, Error, Result,
    Services,
};

impl Services {
    /// Take `note_id`'s content-write lane; see [`Store::lock_note_content`]
    /// for the hold rules. In this crate, drop it before any follow-up that
    /// may write a note (auto-conversion, task-status redirects,
    /// materialization — which takes the lane itself).
    pub(crate) async fn lock_note_content(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> NoteWriteLock {
        self.store.lock_note_content(workspace_id, note_id).await
    }

    /// `note.setContent` body. `trim_edges` is set for agent and tool writes
    /// (`set_note_content`) and clear for client saves (`set_note_content_guarded`),
    /// so the editor keeps blank paragraphs typed at the note's edges.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn write_set_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
        expected_version: Option<i64>,
        base_content_bytes: Option<u64>,
        caller_agent_id: Option<AgentId>,
        trim_edges: bool,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let _mutation = self.workspace_mutations.enter(&workspace_id)?;
            self.require_member(&workspace_id).await?;
            // Guard before the merge so a rejected write touches neither the
            // store nor the merge state.
            note_ops::reject_numbered_read_presentation(&content)?;
            let author = resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
            let write_lane = self.lock_note_content(&workspace_id, &note_id).await;
            let MergedContentWrite {
                note,
                old_content,
                content: clean,
                now,
                rev,
            } = persist_merged_content(
                &store,
                &write_lane,
                ContentWrite {
                    seed: None,
                    incoming: &content,
                    expected_version,
                    policy: ContentWritePolicy::SetContent {
                        confirm_replacement,
                        trim_edges,
                    },
                    author: &author,
                    op: "note.setContent",
                    base_content_bytes,
                },
            )
            .await?;
            drop(write_lane);
            let previous_title = note.title.clone();
            services
                .schedule_line_attribution_recompute(&note.workspace_id.clone(), &note.id.clone());
            let outcome = services
                .auto_convert_task_blocks_after_write(
                    &note.workspace_id,
                    &note.id,
                    &clean,
                    caller_agent_id.as_ref(),
                )
                .await;
            let (final_content, final_updated_at, final_rev) = match outcome.refetched_note {
                Some(n) => (n.content, n.updated_at, n.rev),
                None => (clean, now, rev),
            };
            publish_event(
                bus.as_ref(),
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            // A spec-body write can move taskStats (link-gated rollup);
            // non-spec notes skip the probe (§6.5).
            services
                .maybe_emit_display_status_for_spec_write(&note.workspace_id, &note.id)
                .await;
            Ok(NoteSetContentResult {
                ok: true,
                title: note.title,
                note_id: note.id,
                previous_title: Some(previous_title),
                updated_at: final_updated_at,
                old_content: Some(old_content),
                new_content: final_content,
                converted_count: outcome.converted_count,
                created_task_note_ids: outcome.created_note_ids,
                created_tasks: outcome.created_tasks,
                warnings: outcome.warnings,
                rev: final_rev,
            })
        })
    }
}

/// Content a new note persists: `note.create` trims edge whitespace like
/// every agent and tool write.
pub(crate) fn new_note_content(content: Option<String>) -> String {
    note_ops::trim_note_edges(content.unwrap_or_default())
}

/// Author stamp for daemon-internal note-version writes (the workspace-seed
/// spec note snapshot, linked-checkbox
/// materialization). All user- and agent-originated writes resolve the
/// author from the caller instead (see [`resolve_note_version_author`]).
pub(crate) fn system_version_author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    }
}

/// FE/user-originated note-version author. Reference parity with
/// `notes.service.ts` (`{ id: 'user', name: 'User', type: 'user' }`) — used
/// when a note write carries no agent caller.
pub(crate) fn user_version_author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "user".to_string(),
        name: "User".to_string(),
        author_type: "user".to_string(),
    }
}

/// Resolve the version author for a note write. `Some(agent_id)` → an
/// agent-typed author with the caller's session `name` (best-effort; falls
/// back to the id when the session lookup fails). `None` → the user author.
/// Mirrors `notes.service.ts` provenance (`currentActor?.type === 'agent'`
/// path) so `note.listVersions`/`note.getVersion` carry the acting agent.
pub(crate) async fn resolve_note_version_author(
    store: &Store,
    caller_agent_id: Option<&AgentId>,
) -> NoteVersionAuthor {
    match caller_agent_id {
        Some(agent_id) => {
            // Name-only lookup avoids pulling the full message log on every
            // version stamp; the id string is used as the display fallback
            // when the session row is missing.
            let name = store
                .get_agent_session_name(agent_id)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| agent_id.0.clone());
            NoteVersionAuthor {
                id: agent_id.0.clone(),
                name,
                author_type: "agent".to_string(),
            }
        }
        None => user_version_author(),
    }
}

/// Insert a new note row and its initial version snapshot (at `note.rev`,
/// stamped with `author` and the note's `updated_at`) in one store transaction
/// ([`Store::insert_note_with_version`]): the fresh row is never visible while
/// its rev has no recoverable base, so a writer that reads the new note and
/// later sends that rev as a stale base merges from the initial content
/// instead of degrading to last-writer-wins (PROTOCOL §5.2 version-history
/// extensions). Every note insert goes through here. Returns the version
/// number.
pub(crate) async fn persist_new_note(
    store: &Store,
    note: &Note,
    author: &NoteVersionAuthor,
) -> Result<i64> {
    store
        .insert_note_with_version(note, author, &note.updated_at)
        .await
}

/// Persist a note content write and its version snapshot in one store
/// transaction ([`Store::update_note_with_version`]): the row's new `rev` and
/// the snapshot that makes it a recoverable merge base become visible
/// together, so no concurrent writer can read the new rev and resolve it to
/// the previous content, and a snapshot can never land out of rev order.
/// Every persisted content *update* goes through here; inserts commit row and
/// snapshot together via [`persist_new_note`]. Gated on `expected_version`
/// when `Some` (`Conflict` on a mismatch, like
/// [`Store::update_note_versioned`]). Returns the post-write `rev`.
pub(crate) async fn persist_note_content(
    store: &Store,
    lock: &NoteWriteLock,
    note: &Note,
    expected_version: Option<i64>,
    author: &NoteVersionAuthor,
) -> Result<i64> {
    store
        .update_note_with_version(lock, note, expected_version, author, &note.updated_at)
        .await
        .map(|(rev, _)| rev)
}

/// [`persist_note_content`] for a write that also creates child notes, in one
/// store transaction ([`Store::update_note_with_version_and_children`]): the
/// children exist only if the parent's content write and its version snapshot
/// land (the `task.convertBlocks` fence → link rewrite). Gated on
/// `expected_version` when `Some`. Returns the post-write `rev`.
pub(crate) async fn persist_note_content_with_children(
    store: &Store,
    lock: &NoteWriteLock,
    note: &Note,
    expected_version: Option<i64>,
    children: &[Note],
    author: &NoteVersionAuthor,
) -> Result<i64> {
    store
        .update_note_with_version_and_children(
            lock,
            note,
            expected_version,
            children,
            author,
            &note.updated_at,
        )
        .await
        .map(|(rev, _)| rev)
}

/// [`persist_note_content`] without a version gate that returns the new
/// version number instead of the `rev` (`note.restoreVersion` reports which
/// snapshot the restore created).
pub(crate) async fn persist_note_content_new_version(
    store: &Store,
    lock: &NoteWriteLock,
    note: &Note,
    author: &NoteVersionAuthor,
) -> Result<i64> {
    store
        .update_note_with_version(lock, note, None, author, &note.updated_at)
        .await
        .map(|(_, version)| version)
}

/// [`persist_new_note`] with the daemon-internal system author, for
/// noninteractive note inserts that live outside this crate (the `intentd`
/// importers). Imported notes are loaded by clients like any other, so their
/// initial `rev` must be a recoverable base too — the row and its snapshot
/// commit together.
///
/// # Errors
///
/// Returns `Error::Internal` if the insert or the version append fails.
pub async fn persist_system_new_note(store: &Store, note: &Note) -> Result<i64> {
    persist_new_note(store, note, &system_version_author()).await
}

/// Bounded read-merge-persist attempts for `note.setContent`: on a store
/// `Conflict` (another versioned write landed between the fetch and the
/// persist) the loop re-fetches and re-merges against the new current; the
/// last attempt's `Conflict` propagates unchanged.
pub(crate) const SET_CONTENT_MAX_ATTEMPTS: usize = 5;

/// Text `note.setContent` persists for one attempt, plus the baseline the
/// reduction guard is measured against.
struct SetContentMerge {
    /// Content to clean and persist.
    text: String,
    /// The writer's base when `expected_version` was stale and its snapshot
    /// was recoverable; `None` on the exact / LWW paths (guard measures
    /// current → incoming).
    base: Option<String>,
    outcome: &'static str,
    conflicting_spans: usize,
    /// Checkbox lines whose conflicting marker was collapsed back to one
    /// valid marker after the merge (intent-hq/intent#4930).
    repaired_markers: usize,
}

/// Resolve what a `note.setContent` write persists on top of the stored
/// `current` row. An absent or matching `expected_version` is the exact path
/// (`incoming` replaces the current text). A stale `expected_version` (below
/// `current.rev`) recovers the writer's base via
/// [`Store::get_note_version_content_by_rev`] and applies the writer's intent
/// onto the current text with [`note_merge::three_way_merge`]; when no
/// snapshot survives for that rev the write degrades to honest
/// last-writer-wins. Only revs that could have been read merge: an
/// `expected_version` above `current.rev` was never served by this note, so
/// it is the plain optimistic-concurrency mismatch (`Error::Conflict`,
/// `-32005`) rather than a base to merge from.
async fn merge_set_content(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    current: &Note,
    incoming: &str,
    expected_version: Option<i64>,
) -> Result<SetContentMerge> {
    let stale = match expected_version {
        Some(v) if v > current.rev => {
            let current = serde_json::to_value(current)
                .map_err(|e| Error::Internal(format!("encode current note failed: {e}")))?;
            return Err(Error::Conflict { current });
        }
        Some(v) if v < current.rev => v,
        _ => {
            return Ok(SetContentMerge {
                text: incoming.to_string(),
                base: None,
                outcome: "exact",
                conflicting_spans: 0,
                repaired_markers: 0,
            })
        }
    };
    let current = current.content.as_str();
    match store
        .get_note_version_content_by_rev(workspace_id, note_id, stale)
        .await?
    {
        Some(base) => {
            let merged = note_merge::three_way_merge(&base, current, incoming);
            // Only a conflicting span can concatenate two marker variants;
            // clean merges persist the merged text verbatim, and the repair
            // is scoped to the spans that actually conflicted.
            let (text, repaired_markers) = if merged.conflicting_spans > 0 {
                note_merge::repair_checkbox_markers(&merged.text, &merged.conflict_ranges)
            } else {
                (merged.text, 0)
            };
            Ok(SetContentMerge {
                text,
                base: Some(base),
                outcome: "merged",
                conflicting_spans: merged.conflicting_spans,
                repaired_markers,
            })
        }
        None => Ok(SetContentMerge {
            text: incoming.to_string(),
            base: None,
            outcome: "lww-no-base",
            conflicting_spans: 0,
            repaired_markers: 0,
        }),
    }
}

/// `note.setContent` reduction guard: reject an unconfirmed write whose
/// `incoming` text is more than 50 % shorter than `baseline` (the writer's
/// base when known, else the stored current).
fn check_set_content_reduction(
    baseline: &str,
    incoming: &str,
    confirm_replacement: bool,
) -> Result<()> {
    if baseline.is_empty() || confirm_replacement {
        return Ok(());
    }
    // Note sizes are far below 2^53 (loss-free in f64); the rounded
    // percentage is in [0, 100] so the float→int cast is exact.
    #[expect(clippy::cast_precision_loss)]
    let old_len = baseline.chars().count() as f64;
    #[expect(clippy::cast_precision_loss)]
    let new_len = incoming.chars().count() as f64;
    let reduction = (old_len - new_len) / old_len * 100.0;
    if reduction > 50.0 {
        // The rounded percentage is in (50, 100]: exact in i64.
        #[expect(clippy::cast_possible_truncation)]
        let reduction_pct = reduction.round() as i64;
        return Err(Error::Internal(format!(
            "⚠️ CONTENT REDUCTION DETECTED: Your new content ({} chars) is {}% shorter than the existing content ({} chars).\n\nThis will REPLACE the entire note. If you intended to:\n- ADD content: Use note.add instead\n- EDIT a section: Use note.edit instead\n- PROCEED with replacement: Call note.setContent again with confirmReplacement=true",
            incoming.chars().count(),
            reduction_pct,
            baseline.chars().count()
        )));
    }
    Ok(())
}

/// Truncated-read write guard: compare the writer's `baseContentBytes` with
/// the UTF-8 size of the base `merge` resolved — the stored `current`
/// content on the exact path, the retained snapshot on the merge path. The
/// last-writer-wins path has no known base, so a guarded write is refused
/// there rather than overwriting text the writer never saw.
fn check_content_base(expected: u64, merge: &SetContentMerge, current: &str) -> Result<()> {
    let resolved = match merge.outcome {
        "exact" => Some(current.len()),
        "merged" => merge.base.as_deref().map(str::len),
        _ => None,
    }
    .map(|len| u64::try_from(len).unwrap_or(u64::MAX));
    if resolved == Some(expected) {
        return Ok(());
    }
    Err(Error::ContentBaseMismatch {
        base_content_bytes: expected,
        resolved_base_bytes: resolved,
    })
}

/// How [`persist_merged_content`] turns one attempt's merged text into the
/// text it persists.
#[derive(Clone, Copy)]
pub(crate) enum ContentWritePolicy {
    /// `note.setContent`: the set-content normalizer runs once on the
    /// writer's text before any merge, then each attempt applies the
    /// reduction guard (measured against the writer's base when known, else
    /// the stored current) and validates the merged text before persisting.
    /// `trim_edges` trims the merged text (agent and tool writes, not editor
    /// saves).
    SetContent {
        confirm_replacement: bool,
        trim_edges: bool,
    },
    /// `note.add` / `note.edit` / `note.editLines`: the surgical transform
    /// already ran against the content the caller read; the merged text
    /// persists with its edge whitespace trimmed.
    Surgical,
    /// `task.update` / `task.updateStatus`: like `Surgical`, but the edge
    /// whitespace is kept. Callers address checkboxes by line number from an
    /// earlier read, so a trim would shift their next write onto another line.
    LineStable,
}

impl ContentWritePolicy {
    fn trims_edges(self) -> bool {
        match self {
            Self::SetContent { trim_edges, .. } => trim_edges,
            Self::Surgical => true,
            Self::LineStable => false,
        }
    }
}

/// One persisted content write from [`persist_merged_content`].
pub(crate) struct MergedContentWrite {
    /// The note as persisted (`content` / `updated_at` are the written
    /// values; `rev` is the pre-write rev the last attempt read — use `rev`).
    pub(crate) note: Note,
    /// The stored content the final attempt replaced.
    pub(crate) old_content: String,
    /// The persisted content (post-clean, post-reanchor).
    pub(crate) content: String,
    /// `updated_at` stamped on the write.
    pub(crate) now: String,
    /// Post-write rev.
    pub(crate) rev: i64,
}

/// One client content write for [`persist_merged_content`].
pub(crate) struct ContentWrite<'a> {
    /// A row the caller already fetched: attempt 1 reuses it instead of
    /// re-reading.
    pub(crate) seed: Option<Note>,
    /// The text the writer wants persisted (for surgical ops, the transform's
    /// result against the `seed` content).
    pub(crate) incoming: &'a str,
    /// The rev the writer's `incoming` is based on (`None`: unconditional);
    /// surgical callers pass the rev of their `seed` read.
    pub(crate) expected_version: Option<i64>,
    pub(crate) policy: ContentWritePolicy,
    pub(crate) author: &'a NoteVersionAuthor,
    /// Method label for the merge trace.
    pub(crate) op: &'static str,
    /// The writer's `baseContentBytes` (truncated-read write guard): when
    /// `Some`, every attempt refuses the write with
    /// [`Error::ContentBaseMismatch`] unless it equals the UTF-8 size of the
    /// base that attempt resolves (the stored content on the exact path, the
    /// merge snapshot on the stale path; an unknown base always refuses, so
    /// a guarded write never degrades to last-writer-wins). Without
    /// `expected_version` a retry after a lost rev gate is refused too: the
    /// concurrent text was never the writer's base. `None` (surgical ops,
    /// agents, MCP) is unguarded.
    pub(crate) base_content_bytes: Option<u64>,
}

/// Read-merge-persist loop every client content write runs through
/// (`note.setContent` and the surgical `note.add` / `note.edit` /
/// `note.editLines`): each attempt fetches the current row, resolves what to
/// persist with [`merge_set_content`] (`incoming` verbatim when
/// `expected_version` is absent or matches the current rev; when it is below
/// the current rev, the writer's intent three-way-merged onto the current text
/// from the snapshot at `expected_version`, degrading to last-writer-wins when
/// no snapshot survives; when it is above the current rev, `Conflict` without
/// a write), applies `policy`, runs comment-anchor recovery, and persists
/// gated on the rev it read via [`persist_note_content`] — so a write that
/// lands in between is merged into on the next attempt rather than
/// overwritten. The last attempt's `Conflict` propagates unchanged.
///
/// The `SetContent` cleaner is split around the merge: normalization
/// (quote strip, JSON-value extraction) runs on `incoming` once, before any
/// merge, so the merge (and the checkbox repair) sees the shape that will
/// persist — a quoted payload no longer hides a bullet behind its quote —
/// while current text that legitimately starts with a quote is not stripped
/// by someone else's write. Validation (empty / truncated) runs on the
/// merged text of each attempt, since a zero-conflict merge of two partial
/// deletions can empty a note neither side emptied.
pub(crate) async fn persist_merged_content(
    store: &Store,
    lock: &NoteWriteLock,
    write: ContentWrite<'_>,
) -> Result<MergedContentWrite> {
    let workspace_id = lock.workspace_id();
    let note_id = lock.note_id();
    let ContentWrite {
        mut seed,
        incoming,
        expected_version,
        policy,
        author,
        op,
        base_content_bytes,
    } = write;
    let normalized;
    let merge_input = match policy {
        ContentWritePolicy::SetContent { .. } => {
            normalized = note_ops::normalize_set_content(incoming);
            normalized.as_str()
        }
        ContentWritePolicy::Surgical | ContentWritePolicy::LineStable => incoming,
    };
    let mut attempt = 0;
    loop {
        attempt += 1;
        let mut note = match seed.take() {
            Some(note) => note,
            None => fetch_note_peer(store, workspace_id, note_id).await?,
        };
        let old_content = note.content.clone();
        let current_rev = note.rev;
        if let (Some(expected), None, true) = (base_content_bytes, expected_version, attempt > 1) {
            // A guarded write without `expectedVersion` checked its base
            // against the row the previous attempt read; a write that landed
            // since is text the writer never saw, so it is refused rather
            // than retried over (the caller holds no rev to merge from).
            return Err(Error::ContentBaseMismatch {
                base_content_bytes: expected,
                resolved_base_bytes: Some(u64::try_from(old_content.len()).unwrap_or(u64::MAX)),
            });
        }
        let merge = merge_set_content(
            store,
            workspace_id,
            note_id,
            &note,
            merge_input,
            expected_version,
        )
        .await?;
        if let Some(expected) = base_content_bytes {
            check_content_base(expected, &merge, &old_content)?;
        }
        let text = if policy.trims_edges() {
            note_ops::trim_note_edges(merge.text)
        } else {
            merge.text
        };
        if let ContentWritePolicy::SetContent {
            confirm_replacement,
            ..
        } = policy
        {
            check_set_content_reduction(
                merge.base.as_deref().unwrap_or(&old_content),
                incoming,
                confirm_replacement,
            )?;
            note_ops::validate_set_content(&text)?;
        }
        tracing::debug!(
            note = %note_id.0,
            op,
            attempt,
            current_rev,
            expected_version,
            outcome = merge.outcome,
            conflicting_spans = merge.conflicting_spans,
            repaired_markers = merge.repaired_markers,
            "note content merge"
        );
        let mut plan = reanchor_note_comments(store, workspace_id, note_id, text).await?;
        let content = std::mem::take(&mut plan.content);
        note.content = content.clone();
        let now = now_iso();
        note.updated_at = now.clone();
        match persist_note_content(store, lock, &note, Some(current_rev), author).await {
            Ok(rev) => {
                plan.apply_orphaned(store, workspace_id).await?;
                return Ok(MergedContentWrite {
                    note,
                    old_content,
                    content,
                    now,
                    rev,
                });
            }
            Err(Error::Conflict { .. }) if attempt < SET_CONTENT_MAX_ATTEMPTS => {}
            Err(e) => return Err(e),
        }
    }
}

/// Comment anchor recovery pass, called by every note-content mutation before
/// the new content is persisted. Reference `NotesService.updateNote` calls
/// `recoverAllPartialAnchors` on the incoming markdown and marks unrecoverable
/// comments `isOrphaned: true`; we mirror that here using the anchor context
/// captured at `comment.add` time (`anchor_before` / `anchor_after`) instead of
/// version history, since the pass runs before the new version is stored.
///
/// Returns a [`ReanchorPlan`]: the possibly-rewritten markdown plus the set
/// of comments whose `is_orphaned` flag needs to be flipped to `true`.
/// Already-orphaned comments are left as-is. The plan does **no** store
/// writes — callers first persist the note-content change via an atomic
/// versioned write (`persist_note_content` / the `*_with_version` store
/// helpers) and then call [`ReanchorPlan::apply_orphaned`] so a failed note write
/// cannot leave comment rows flagged orphaned while the persisted markdown
/// still contains the original anchors.
///
/// The lookup is scoped to `workspace_id` because `note_id` (e.g. the
/// well-known `spec` id) is not globally unique — see
/// `comment_repo::list_comments_in_workspace`.
pub(crate) async fn reanchor_note_comments(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    content: String,
) -> Result<ReanchorPlan> {
    // Propagate the list_comments error: a failed lookup must not leave the
    // note content persisted with broken anchors while the comment row still
    // claims to be healthy. Callers already treat `reanchor_note_comments` as
    // fallible and roll back the surrounding mutation.
    let comments = store
        .list_comments_in_workspace(workspace_id, note_id)
        .await?;
    // Ids that legitimately own markers after this pass; comments flipped to
    // orphaned below are removed so the final scrub treats their markers as
    // debris too.
    let mut live_ids = live_comment_ids(&comments);
    let mut current = content;
    let mut orphaned: Vec<String> = Vec::new();
    for comment in &comments {
        // Only root-level anchored comments carry markers; replies inherit the
        // parent's anchor and never inject their own into the note body.
        if comment.parent_id.is_some() {
            continue;
        }
        if comment.is_orphaned == Some(true) {
            continue;
        }
        let state = note_ops::classify_anchor_state(&current, &comment.id);
        match state {
            note_ops::AnchorState::Healthy => {}
            note_ops::AnchorState::Missing => {
                live_ids.remove(&comment.id);
                orphaned.push(comment.id.clone());
            }
            note_ops::AnchorState::Degenerate => {
                live_ids.remove(&comment.id);
                current = note_ops::remove_anchor_markers(&current, &comment.id);
                orphaned.push(comment.id.clone());
            }
            note_ops::AnchorState::PartialStartOnly | note_ops::AnchorState::PartialEndOnly => {
                let outcome = note_ops::recover_partial_anchor(
                    &current,
                    &comment.id,
                    comment.anchor_before.as_deref(),
                    comment.anchor_after.as_deref(),
                );
                match outcome {
                    note_ops::RecoveryOutcome::Recovered(new_md) => {
                        current = new_md;
                    }
                    note_ops::RecoveryOutcome::Failed(reason) => {
                        tracing::debug!(
                            comment_id = %comment.id,
                            reason,
                            "partial-anchor recovery failed; orphaning comment"
                        );
                        live_ids.remove(&comment.id);
                        current = note_ops::remove_anchor_markers(&current, &comment.id);
                        orphaned.push(comment.id.clone());
                    }
                }
            }
        }
    }
    // Phantom scrub (Round 15): UUID-format markers whose id has no live
    // comment row — an id with no row at all, or markers left behind by a
    // row already flagged orphaned — are debris and would otherwise survive
    // every mutation. Non-UUID marker-lookalikes (documentation literals)
    // are user content and are never touched.
    current = note_ops::scrub_phantom_anchor_markers(&current, &live_ids);
    Ok(ReanchorPlan {
        content: current,
        note_id: note_id.clone(),
        orphaned,
    })
}

/// Ids of comment rows not flagged `is_orphaned` — the keep-set for
/// [`note_ops::scrub_phantom_anchor_markers`].
pub(crate) fn live_comment_ids(comments: &[Comment]) -> HashSet<String> {
    comments
        .iter()
        .filter(|c| c.is_orphaned != Some(true))
        .map(|c| c.id.clone())
        .collect()
}

/// Result of [`reanchor_note_comments`]: the rewritten note markdown and the
/// ids of root comments whose `is_orphaned` flag needs to flip to `true`.
pub(crate) struct ReanchorPlan {
    pub(crate) content: String,
    note_id: NoteId,
    /// Ids of root comments to flag orphaned (never whole cached rows).
    orphaned: Vec<String>,
}

impl ReanchorPlan {
    /// Persist the queued orphan flips. Callers **must** run this only after
    /// the note-content write has succeeded so a note-write failure cannot
    /// leave comment rows marked orphaned while the persisted markdown still
    /// contains the original anchors. If a comment update fails after the
    /// note has been written the anchors on disk are still a valid pointer
    /// (either intact or scrubbed) and the next mutation's reanchor pass
    /// will finish the job.
    ///
    /// Each flip is a targeted update of the orphan flag and its timestamp,
    /// never a write-back of the comment row read before the note write: a
    /// thread resolved in between must stay resolved (intentd#2592 review).
    pub(crate) async fn apply_orphaned(
        self,
        store: &Store,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        let now = now_iso();
        for id in self.orphaned {
            store
                .mark_comment_orphaned(workspace_id, &self.note_id, &id, &now)
                .await?;
        }
        Ok(())
    }
}
