//! CRDT-backed note-merge engine (`yrs`), ported from the reference
//! `CRDTDocumentManager` / `CRDTNotesService` in
//! `packages/cloudlands-fe/src/features/notes/main/storage/crdt-document-manager.ts`.
//!
//! Session-only: a `yrs::Doc` is created lazily on the first full-content write
//! for a `(workspace, note)` pair, seeded from the caller-supplied baseline
//! (the note's stored content at request start). Subsequent full-content
//! writes compute a single-hunk char-level diff of the new content against the
//! doc's current text and apply it inside a `yrs` transaction; the merged
//! resulting text is returned so the caller persists it to SQLite via the
//! normal `note.*` mutation flow (§5.2). The `yrs` state itself is never
//! persisted.
//!
//! Surgical `note.*` mutations (`add` / `edit` / `editLines` / `task.update`,
//! `task.updateStatus`) invalidate the session, so the next full-content write
//! re-seeds from disk. This is the simplest coherent choice: it avoids reaching
//! into the CRDT for granular ops and guarantees the merge baseline for the
//! next `setContent` is the current stored content.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use intent_core::{NoteId, WorkspaceId};
use yrs::{Doc, GetString, OffsetKind, Options, Text, Transact, TransactionMut};

/// Reference parity (`SESSION_TIMEOUT_MS = 24 * 60 * 60 * 1000`): sweep sessions
/// idle for more than a day. Wired from the composition root via
/// [`crate::Services::spawn_crdt_session_sweep_loop`].
pub(crate) const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// Reference parity (`updateContent` cadence): the yrs-side sweep runs once
/// per hour. Wired from the composition root via
/// [`crate::Services::spawn_crdt_session_sweep_loop`].
pub(crate) const SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Name of the `yrs::Text` container that mirrors the note's markdown content.
const CONTENT_TEXT_KEY: &str = "content";

struct Session {
    doc: Doc,
    last_access: Instant,
}

/// Session-only CRDT store keyed by `(workspace_id, note_id)`.
#[derive(Default)]
pub(crate) struct CrdtNoteManager {
    sessions: Mutex<HashMap<(WorkspaceId, NoteId), Session>>,
}

impl CrdtNoteManager {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Lock the session map, recovering from poisoning. The map is a
    /// session-only cache of best-effort merge state (never persisted). A
    /// panic inside `apply_full_content` can leave a session entry behind with
    /// a partially-applied edit (committed when the transaction unwinds), but
    /// that state self-heals on the next full-content write: `apply_diff`
    /// diffs against whatever the doc currently holds and converges it to the
    /// incoming content.
    fn lock_sessions(&self) -> MutexGuard<'_, HashMap<(WorkspaceId, NoteId), Session>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Route `new_content` through the yrs doc for `(workspace, note)` and
    /// return the merged content that the caller should persist. `base_content`
    /// is the stored note content the request started from; it seeds the doc
    /// on first touch. Subsequent calls compute the diff against the doc's
    /// current text (never `base_content` after seeding), matching the
    /// reference `applyContentUpdate`.
    pub(crate) fn apply_full_content(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        base_content: &str,
        new_content: &str,
    ) -> String {
        let key = (workspace_id.clone(), note_id.clone());
        let mut sessions = self.lock_sessions();
        let session = sessions.entry(key).or_insert_with(|| {
            // `apply_diff` computes UTF-16 code-unit offsets (JS string / `Y.Text`
            // parity), so the doc must index text by UTF-16 units too — the yrs
            // default is byte offsets, which corrupts or panics on multi-byte
            // content (monorepo#721).
            let doc = Doc::with_options(Options {
                offset_kind: OffsetKind::Utf16,
                ..Default::default()
            });
            seed_doc(&doc, base_content);
            Session {
                doc,
                last_access: Instant::now(),
            }
        });
        session.last_access = Instant::now();

        let text = session.doc.get_or_insert_text(CONTENT_TEXT_KEY);
        {
            let mut txn = session.doc.transact_mut();
            let current = text.get_string(&txn);
            if current != new_content {
                apply_diff(&mut txn, &text, &current, new_content);
            }
        }
        let txn = session.doc.transact();
        text.get_string(&txn)
    }

    /// Drop the cached session so the next full-content write re-seeds from
    /// storage. Called after surgical `note.*` mutations that write content
    /// directly (add / edit / editLines / task.update / task.updateStatus /
    /// restoreVersion).
    pub(crate) fn invalidate(&self, workspace_id: &WorkspaceId, note_id: &NoteId) {
        let key = (workspace_id.clone(), note_id.clone());
        let mut sessions = self.lock_sessions();
        sessions.remove(&key);
    }

    /// Remove the session on note deletion (reference: `removeDocument`).
    pub fn remove(&self, workspace_id: &WorkspaceId, note_id: &NoteId) {
        self.invalidate(workspace_id, note_id);
    }

    /// Sweep sessions idle beyond `timeout` (reference: `cleanupStaleSessions`).
    /// Driven by the composition-root sweeper spawned in
    /// [`crate::Services::spawn_crdt_session_sweep_loop`] using
    /// [`SESSION_SWEEP_INTERVAL`] / [`SESSION_IDLE_TIMEOUT`].
    pub(crate) fn sweep_stale(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut sessions = self.lock_sessions();
        let before = sessions.len();
        sessions.retain(|_, session| now.duration_since(session.last_access) <= timeout);
        before - sessions.len()
    }

    /// True if a live session exists for `(workspace, note)`.
    #[cfg(test)]
    pub(crate) fn has_session(&self, workspace_id: &WorkspaceId, note_id: &NoteId) -> bool {
        let key = (workspace_id.clone(), note_id.clone());
        self.lock_sessions().contains_key(&key)
    }

    /// Test-only: seed the last-access timestamp so `sweep_stale` observes an
    /// aged session without waiting real time.
    #[cfg(test)]
    pub(crate) fn set_last_access_for_test(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        when: Instant,
    ) {
        let key = (workspace_id.clone(), note_id.clone());
        if let Some(session) = self.lock_sessions().get_mut(&key) {
            session.last_access = when;
        }
    }
}

fn seed_doc(doc: &Doc, content: &str) {
    let text = doc.get_or_insert_text(CONTENT_TEXT_KEY);
    let mut txn = doc.transact_mut();
    if !content.is_empty() {
        text.insert(&mut txn, 0, content);
    }
}

/// Compute a single-hunk diff (common prefix / suffix trimmed) and apply it as
/// a `delete` + `insert` inside the given transaction. Faithful port of the
/// reference `computeTextDiff` + `applyContentUpdate` loop
/// (`packages/cloudlands-fe/.../crdt-notes.service.ts`).
///
/// The reference compares JavaScript strings, which are UTF-16 code-unit
/// sequences, and `Y.Text` indexes by the same UTF-16 units. To match that
/// exactly — and to keep `yrs::Text::insert` / `remove_range` from panicking
/// when the requested offset does not land on a UTF-16 boundary — we walk
/// both sides as `u16` code units too.
///
/// Trim boundaries can land inside a surrogate pair when the old and new code
/// points share one half of the pair (monorepo#730), e.g. U+1F600
/// `[0xD83D, 0xDE00]` → U+1F601 `[0xD83D, 0xDE01]` share the high surrogate.
/// Both boundaries are backed off to the nearest pair boundary so the deleted
/// range and the inserted middle segment are always well-formed UTF-16.
fn apply_diff(txn: &mut TransactionMut<'_>, text: &yrs::TextRef, current: &str, target: &str) {
    let cur: Vec<u16> = current.encode_utf16().collect();
    let tgt: Vec<u16> = target.encode_utf16().collect();

    let mut prefix = 0;
    while prefix < cur.len() && prefix < tgt.len() && cur[prefix] == tgt[prefix] {
        prefix += 1;
    }
    // Back off if the prefix trim stopped between the halves of a surrogate
    // pair: the matched unit before the boundary is a high surrogate whose low
    // surrogate differs between the two sides.
    if prefix > 0 && is_high_surrogate(cur[prefix - 1]) {
        prefix -= 1;
    }

    let mut suffix = 0;
    while suffix < cur.len() - prefix
        && suffix < tgt.len() - prefix
        && cur[cur.len() - 1 - suffix] == tgt[tgt.len() - 1 - suffix]
    {
        suffix += 1;
    }
    // Symmetric back-off: the first retained suffix unit is a low surrogate
    // whose high surrogate differs between the two sides.
    if suffix > 0 && is_low_surrogate(cur[cur.len() - suffix]) {
        suffix -= 1;
    }

    let delete_len = cur.len() - prefix - suffix;
    let insert_slice = &tgt[prefix..tgt.len() - suffix];

    if delete_len > 0 {
        text.remove_range(txn, prefix as u32, delete_len as u32);
    }
    if !insert_slice.is_empty() {
        // `encode_utf16` always yields well-formed pairs, and the surrogate
        // back-offs above move both trim boundaries to pair boundaries, so
        // `from_utf16` on the middle segment is safe.
        let insert_text = String::from_utf16(insert_slice)
            .expect("utf-16 middle segment is well-formed after surrogate back-off");
        text.insert(txn, prefix as u32, &insert_text);
    }
}

/// True for the leading (high) half of a UTF-16 surrogate pair.
fn is_high_surrogate(unit: u16) -> bool {
    (0xD800..=0xDBFF).contains(&unit)
}

/// True for the trailing (low) half of a UTF-16 surrogate pair.
fn is_low_surrogate(unit: u16) -> bool {
    (0xDC00..=0xDFFF).contains(&unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (WorkspaceId, NoteId) {
        (WorkspaceId::from("ws"), NoteId::from("n"))
    }

    #[test]
    fn seeds_from_base_content_on_first_touch() {
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        // First call with matching base + new leaves the doc = base_content.
        let merged = mgr.apply_full_content(&ws, &note, "hello", "hello");
        assert_eq!(merged, "hello");
        assert!(mgr.has_session(&ws, &note));
    }

    #[test]
    fn applies_diff_to_seeded_doc() {
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        let merged =
            mgr.apply_full_content(&ws, &note, "the quick brown fox", "the QUICK brown fox");
        assert_eq!(merged, "the QUICK brown fox");
    }

    #[test]
    fn sequential_regional_updates_both_survive() {
        // Reference parity: two full-content writes that each observe the doc's
        // current state converge; the diffs target different regions, so both
        // authors' edits are preserved in the final merged content.
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();

        // Author A appends a line at the end.
        let after_a = mgr.apply_full_content(&ws, &note, "BODY", "BODY\nA-line");
        assert_eq!(after_a, "BODY\nA-line");

        // Author B reads the current state (A's write is visible) and prepends
        // their own line; both edits survive in the merged text.
        let after_b = mgr.apply_full_content(&ws, &note, "BODY\nA-line", "B-line\nBODY\nA-line");
        assert_eq!(after_b, "B-line\nBODY\nA-line");
    }

    #[test]
    fn interleaved_surgical_invalidates_session() {
        // Full-content write seeds a doc; a surgical op invalidates it; the
        // next full-content write re-seeds from the caller's baseline (which
        // reflects the surgical op's persisted result).
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();

        mgr.apply_full_content(&ws, &note, "line one", "line one\nline two");
        assert!(mgr.has_session(&ws, &note));

        mgr.invalidate(&ws, &note);
        assert!(!mgr.has_session(&ws, &note));

        // Baseline now reflects the surgical result; new content merges cleanly.
        let after = mgr.apply_full_content(
            &ws,
            &note,
            "line one\nline two\nsurgical",
            "line one\nline two\nsurgical\nfull-write",
        );
        assert_eq!(after, "line one\nline two\nsurgical\nfull-write");
    }

    #[test]
    fn full_replacement_lww_on_the_doc() {
        // When the new content shares neither prefix nor suffix with the
        // current doc content, the diff replaces everything (last-write-wins
        // on the doc, matching the reference).
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, "alpha", "alpha");
        let after = mgr.apply_full_content(&ws, &note, "alpha", "beta");
        assert_eq!(after, "beta");
    }

    #[test]
    fn invalidate_drops_session() {
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, "x", "x");
        assert!(mgr.has_session(&ws, &note));
        mgr.invalidate(&ws, &note);
        assert!(!mgr.has_session(&ws, &note));
    }

    #[test]
    fn remove_drops_session() {
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, "x", "x");
        mgr.remove(&ws, &note);
        assert!(!mgr.has_session(&ws, &note));
    }

    #[test]
    fn sweep_stale_removes_aged_sessions() {
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, "x", "x");
        // Backdate the session past the timeout.
        let long_ago = Instant::now()
            .checked_sub(Duration::from_secs(48 * 60 * 60))
            .expect("subtract 48h");
        mgr.set_last_access_for_test(&ws, &note, long_ago);
        let removed = mgr.sweep_stale(SESSION_IDLE_TIMEOUT);
        assert_eq!(removed, 1);
        assert!(!mgr.has_session(&ws, &note));
    }

    #[test]
    fn sweep_stale_keeps_fresh_sessions() {
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, "x", "x");
        let removed = mgr.sweep_stale(SESSION_IDLE_TIMEOUT);
        assert_eq!(removed, 0);
        assert!(mgr.has_session(&ws, &note));
    }

    #[test]
    fn multibyte_content_edits_before_and_after_emoji() {
        // Regression (monorepo#721): `apply_diff` computes UTF-16 code-unit
        // offsets, so the doc must index text the same way. With the default
        // byte offsets, edits around multi-byte chars (emoji + CJK) land on
        // non-boundary byte offsets and panic inside yrs.
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        let base = "héllo 🌍 世界";
        mgr.apply_full_content(&ws, &note, base, base);

        // Edit before the emoji.
        let after_prefix = mgr.apply_full_content(&ws, &note, base, "héllo! 🌍 世界");
        assert_eq!(after_prefix, "héllo! 🌍 世界");

        // Edit after the emoji (between the multi-byte tail chars).
        let after_suffix =
            mgr.apply_full_content(&ws, &note, after_prefix.as_str(), "héllo! 🌍 世界 end");
        assert_eq!(after_suffix, "héllo! 🌍 世界 end");
    }

    #[test]
    fn surrogate_pair_edit_sharing_high_surrogate() {
        // Regression (monorepo#730): U+1F600 [0xD83D, 0xDE00] → U+1F601
        // [0xD83D, 0xDE01] share the high surrogate, so the prefix trim used
        // to stop mid-pair and `apply_diff` panicked on a lone low surrogate.
        let grinning = "\u{1F600}";
        let beaming = "\u{1F601}";
        let g: Vec<u16> = grinning.encode_utf16().collect();
        let b: Vec<u16> = beaming.encode_utf16().collect();
        assert_eq!(g[0], b[0], "test pair must share the high surrogate");
        assert_ne!(g[1], b[1], "test pair must differ in the low surrogate");

        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, grinning, grinning);
        let after = mgr.apply_full_content(&ws, &note, grinning, beaming);
        assert_eq!(after, beaming);
    }

    #[test]
    fn surrogate_pair_edit_sharing_low_surrogate() {
        // Regression (monorepo#730), symmetric case: U+1F600 [0xD83D, 0xDE00]
        // → U+1FA00 [0xD83E, 0xDE00] share the low surrogate, so the suffix
        // trim used to stop mid-pair and `apply_diff` panicked on a lone high
        // surrogate.
        let grinning = "\u{1F600}";
        let chess = "\u{1FA00}";
        let g: Vec<u16> = grinning.encode_utf16().collect();
        let c: Vec<u16> = chess.encode_utf16().collect();
        assert_ne!(g[0], c[0], "test pair must differ in the high surrogate");
        assert_eq!(g[1], c[1], "test pair must share the low surrogate");

        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, grinning, grinning);
        let after = mgr.apply_full_content(&ws, &note, grinning, chess);
        assert_eq!(after, chess);
    }

    #[test]
    fn surrogate_pair_edits_in_mixed_content() {
        // Regression (monorepo#730): pair-splitting emoji edits embedded in
        // longer mixed content (ASCII + CJK + emoji) round-trip correctly.
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        let base = "notes 世界 \u{1F600} tail 🌍";
        mgr.apply_full_content(&ws, &note, base, base);

        // Shared-high-surrogate swap in the middle of the string.
        let step_1 = "notes 世界 \u{1F601} tail 🌍";
        let after_1 = mgr.apply_full_content(&ws, &note, base, step_1);
        assert_eq!(after_1, step_1);

        // Shared-low-surrogate swap plus a trailing ASCII edit.
        let step_2 = "notes 世界 \u{1FA00} tail 🌍!";
        let after_2 = mgr.apply_full_content(&ws, &note, step_1, step_2);
        assert_eq!(after_2, step_2);
    }

    #[test]
    fn surrogate_pair_edit_triggering_both_backoffs_in_one_diff() {
        // Regression (monorepo#730): a single diff whose prefix trim stops
        // inside the leading pair (shared high surrogate) AND whose suffix
        // trim stops inside the trailing pair (shared low surrogate), so both
        // back-offs fire in one hunk.
        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        let base = "\u{1F600} x \u{1FA00}";
        let target = "\u{1F601} x \u{1F600}";
        mgr.apply_full_content(&ws, &note, base, base);
        let after = mgr.apply_full_content(&ws, &note, base, target);
        assert_eq!(after, target);
    }

    #[test]
    fn poisoned_sessions_mutex_recovers() {
        // Regression (monorepo#721): a panic while holding the sessions lock
        // poisoned the mutex, and every later call panicked on
        // `.expect("crdt sessions poisoned")`. The map is a session-only
        // cache, so recovery via `PoisonError::into_inner` must keep all
        // entry points working.
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let mgr = CrdtNoteManager::new();
        let (ws, note) = ids();
        mgr.apply_full_content(&ws, &note, "seed", "seed");

        // Poison the mutex: panic while holding the lock.
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _guard = mgr.sessions.lock().unwrap();
            panic!("poison the crdt sessions mutex");
        }));
        assert!(result.is_err());
        assert!(mgr.sessions.is_poisoned());

        // All entry points must keep working on the poisoned mutex.
        assert!(mgr.has_session(&ws, &note));
        mgr.invalidate(&ws, &note);
        assert!(!mgr.has_session(&ws, &note));
        let merged = mgr.apply_full_content(&ws, &note, "seed", "seed two");
        assert_eq!(merged, "seed two");
        assert_eq!(mgr.sweep_stale(SESSION_IDLE_TIMEOUT), 0);
    }
}
