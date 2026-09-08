//! Replay-preview truncation helpers shared by the recovery-replay formatter
//! (`intent-services::history_xml`) and the store's tool-payload retention
//! sweep, so a pre-truncated stored preview renders byte-identically to a
//! full body truncated at the same cap.
//!
//! # Replay-preview block contract
//!
//! A persisted `tool_use` / `tool_result` content block may carry a
//! pre-truncated replay body: the heavy field (`input` / `output`) holds the
//! middle-truncated STRING produced by [`truncate_marked`], and the additive
//! integer field [`INPUT_REPLAY_ORIGINAL_CHARS_KEY`] /
//! [`OUTPUT_REPLAY_ORIGINAL_CHARS_KEY`] carries the original char count. The
//! replay formatter renders such a block exactly as it would render the full
//! body at the current cap ([`retruncate_replay_preview`]): if the current cap
//! is SMALLER than the stored preview it re-truncates; it never expands.
//! Blocks without the marker keep the full-body path.

use serde_json::Value;

/// Additive marker on a `tool_use` block whose `input` is a replay preview.
pub const INPUT_REPLAY_ORIGINAL_CHARS_KEY: &str = "inputReplayOriginalChars";
/// Additive marker on a `tool_result` block whose `output` is a replay preview.
pub const OUTPUT_REPLAY_ORIGINAL_CHARS_KEY: &str = "outputReplayOriginalChars";

/// Chars reserved out of the cap for the inline `... [N characters truncated] ...`
/// marker line (TS `truncateMiddleContent`).
pub const TRUNCATION_MARKER_RESERVE_CHARS: usize = 60;

/// Chars of the inline marker excluding the digits of `N` (ASCII, so bytes == chars).
const MARKER_FIXED_CHARS: usize = "\n... [ characters truncated] ...\n".len();

fn marker(omitted: usize) -> String {
    format!("\n... [{omitted} characters truncated] ...\n")
}

/// Stringify a heavy tool body for truncation (TS `safeStringify`): strings
/// pass through; everything else is JSON-encoded. Shared by the formatter's
/// full-body path and the store's replay-preview producer so both truncate
/// the same text.
#[must_use]
pub fn safe_stringify(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| value.to_string()),
    }
}

/// Middle-truncate `text` to `max_chars`, keeping the head and tail (TS
/// `truncateMiddleContent`). Operates on chars to stay on UTF-8 boundaries.
#[must_use]
pub fn truncate_middle_content(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len();
    if len <= max_chars {
        return text.to_string();
    }
    let half_budget = max_chars.saturating_sub(TRUNCATION_MARKER_RESERVE_CHARS) / 2;
    if half_budget == 0 {
        return chars[..max_chars.min(len)].iter().collect();
    }
    let start: String = chars[..half_budget].iter().collect();
    let end: String = chars[len - half_budget..].iter().collect();
    format!("{start}{}{end}", marker(len - half_budget * 2))
}

/// Middle-truncate `text` to `max_chars` and return `(text, original_chars)`:
/// `original_chars` is `Some(N)` when the body was over the cap (the block is
/// abbreviated and `N` is its full char count), `None` when it fit whole.
///
/// Counts chars here and again inside `truncate_middle_content` (two walks of
/// an over-cap block; negligible at the caps in use). For
/// `max_chars < TRUNCATION_MARKER_RESERVE_CHARS + 2` the inline marker is not
/// emitted but `original_chars` is still reported.
#[must_use]
pub fn truncate_marked(text: &str, max_chars: usize) -> (String, Option<usize>) {
    let original_chars = text.chars().count();
    if original_chars <= max_chars {
        return (text.to_string(), None);
    }
    (
        truncate_middle_content(text, max_chars),
        Some(original_chars),
    )
}

/// Locate the stored half-budget of a `truncate_middle_content` preview whose
/// full body had `original_chars` chars: the marker line must sit exactly
/// between two equal halves and name the exact omitted count.
fn stored_half_budget(preview: &[char], original_chars: usize) -> Option<usize> {
    let len = preview.len();
    for digits in 1..=20 {
        let marker_len = MARKER_FIXED_CHARS + digits;
        let rest = len.checked_sub(marker_len)?;
        if rest % 2 != 0 {
            continue;
        }
        let half = rest / 2;
        let Some(omitted) = original_chars.checked_sub(2 * half) else {
            continue;
        };
        let expected = marker(omitted);
        if expected.len() != marker_len {
            continue;
        }
        if preview[half..half + marker_len]
            .iter()
            .copied()
            .eq(expected.chars())
        {
            return Some(half);
        }
    }
    None
}

/// Render a stored replay preview (`preview`, whose full body had
/// `original_chars` chars) at the current `max_chars` cap, returning the same
/// `(text, original_chars)` pair [`truncate_marked`] would return for the full
/// body when that is reconstructible; otherwise the preview is passed through
/// as-is, still marked, since a preview can never be expanded.
///
/// Byte-identical to `truncate_marked(full, max_chars)` whenever the preview
/// was produced at a cap ≥ `max_chars` (or holds the whole body).
#[must_use]
pub fn retruncate_replay_preview(
    preview: &str,
    original_chars: usize,
    max_chars: usize,
) -> (String, Option<usize>) {
    let chars: Vec<char> = preview.chars().collect();
    if chars.len() >= original_chars {
        // The preview holds the whole body: identical to the full-body path.
        return truncate_marked(preview, max_chars);
    }
    if original_chars <= max_chars {
        // The cap grew past the original body; nothing to expand from.
        return (preview.to_string(), Some(original_chars));
    }
    let half_budget = max_chars.saturating_sub(TRUNCATION_MARKER_RESERVE_CHARS) / 2;
    let Some(stored_half) = stored_half_budget(&chars, original_chars) else {
        return (preview.to_string(), Some(original_chars));
    };
    if half_budget == 0 {
        if stored_half >= max_chars {
            return (chars[..max_chars].iter().collect(), Some(original_chars));
        }
        return (preview.to_string(), Some(original_chars));
    }
    if stored_half < half_budget {
        return (preview.to_string(), Some(original_chars));
    }
    let start: String = chars[..half_budget].iter().collect();
    let end: String = chars[chars.len() - half_budget..].iter().collect();
    (
        format!("{start}{}{end}", marker(original_chars - half_budget * 2)),
        Some(original_chars),
    )
}

#[cfg(test)]
mod tests;
