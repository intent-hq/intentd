//! The daemon's agent-card preview cleaner (`clean_response_text`) checked
//! against the shared suggested-prompts block cases
//! (`docs/protocol/fixtures/suggested-prompts/v1/cases.json` in intent-hq/intent,
//! see `docs/protocol/suggested-prompts.md`). The cases are an exact-byte copy
//! under `tests/fixtures/suggested-prompts/`; CI verifies the copy against the
//! monorepo commit named in `suggested-prompts.lock.json`.
//!
//! The preview only removes blocks (it never surfaces prompts), so only cases
//! with a well-formed block (`blockStripped`) apply, and only their `cleaned`
//! text is compared.

use serde_json::Value;

use super::{clean_response_text, live_response_and_digest_from_blocks};
use crate::attachment_upload::sha256_hex;

const CASES: &str = include_str!("../../tests/fixtures/suggested-prompts/v1/cases.json");
const LOCK: &str = include_str!("../../tests/fixtures/suggested-prompts.lock.json");
const CONSUMER: &str = "intentd";

/// The contract's comparison form: CRLF as LF, trimmed, runs of 3+ line feeds
/// collapsed to 2.
fn normalize(text: &str) -> String {
    let unified = text.replace("\r\n", "\n");
    let trimmed = unified.trim();
    let mut out = String::with_capacity(trimmed.len());
    let mut newlines = 0;
    for ch in trimmed.chars() {
        if ch == '\n' {
            newlines += 1;
            if newlines > 2 {
                continue;
            }
        } else {
            newlines = 0;
        }
        out.push(ch);
    }
    out
}

fn cases() -> Vec<Value> {
    let doc: Value = serde_json::from_str(CASES).expect("cases json");
    assert_eq!(doc["fixtureFormatVersion"], 1, "unsupported cases format");
    doc["cases"].as_array().expect("cases array").clone()
}

#[test]
fn suggested_prompts_contract_copy_matches_its_lock() {
    let lock: Value = serde_json::from_str(LOCK).expect("lock json");
    assert_eq!(lock["sourceRepository"], "intent-hq/intent");
    assert_eq!(
        lock["sourcePath"],
        "docs/protocol/fixtures/suggested-prompts/v1"
    );
    let hex = sha256_hex(CASES.as_bytes());
    assert_eq!(
        lock["casesSha256"].as_str(),
        Some(hex.as_str()),
        "cases.json was edited without re-exporting it from the monorepo"
    );
}

#[test]
fn normalize_matches_the_contract() {
    assert_eq!(normalize("  a\n\n\n\nb\n"), "a\n\nb");
    assert_eq!(normalize("a\nb\n\nc"), "a\nb\n\nc");
    assert_eq!(normalize("a\r\n\r\n\r\nb\r\n"), "a\n\nb");
}

#[test]
fn suggested_prompts_contract_strips_every_well_formed_block() {
    let mut checked = 0;
    let mut failures = Vec::new();
    for case in cases() {
        let id = case["id"].as_str().expect("id");
        let expect = &case["expect"];
        if !expect["blockStripped"].as_bool().expect("blockStripped") {
            continue;
        }
        checked += 1;
        let input = case["input"].as_str().expect("input");
        let want = expect["cleaned"].as_str().expect("cleaned");
        let got = normalize(&clean_response_text(input, false));
        let reason = case["divergences"][CONSUMER].as_str();
        if let Some(reason) = reason.filter(|_| got == want) {
            failures.push(format!(
                "{id}: listed as an intentd divergence ({reason}) but now matches; remove the entry"
            ));
        } else if reason.is_none() && got != want {
            failures.push(format!(
                "{id}: cleaned text differs\n  want: {want:?}\n  got:  {got:?}"
            ));
        }
    }
    assert!(
        checked >= 20,
        "only {checked} cases with a well-formed block"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A body line with an embedded arrow (`A --> B`) must not end the block
/// early: the preview used to surface the block's tail (`-->`, ` B -->`).
#[test]
fn suggested_prompts_embedded_arrow_does_not_end_the_block() {
    for text in [
        "Done.\n<!-- suggested-prompts\nA --> B\nRun tests\n-->",
        "Done.\n\n<!-- suggested-prompts\nA --> B -->",
        "Done.\n\n&lt;!-- suggested-prompts\nA --&gt; B\n--&gt;",
        "Done.\r\n<!-- suggested-prompts\r\nA --> B\r\n-->",
        "Done.\n<!--suggested-prompts\nRun tests\n-->",
        "Done.\n<!--\tsuggested-prompts  \nRun tests\n-->",
    ] {
        assert_eq!(clean_response_text(text, false), "Done.", "{text:?}");
    }
}

/// An inline `opener … closer` span is removed within its own line, and an
/// inline mention never reaches past its line to a later block's closer.
#[test]
fn suggested_prompts_inline_mentions_stay_on_their_line() {
    assert_eq!(
        clean_response_text(
            "Write a <!-- suggested-prompts block --> at the end.",
            false
        ),
        "Write a  at the end."
    );
    let text = "Updated the `<!-- suggested-prompts -->` parser.\nAll tests pass.\n\
                <!-- suggested-prompts\nRun tests\n-->";
    assert_eq!(
        clean_response_text(text, false),
        "Updated the `` parser.\nAll tests pass."
    );
    let unclosed_inline =
        "See <!-- suggested-prompts here.\nTail.\n<!-- suggested-prompts\nRun\n-->";
    assert_eq!(
        clean_response_text(unclosed_inline, false),
        "See <!-- suggested-prompts here.\nTail."
    );
    let fused_mention =
        "Done.\n<!-- suggested-prompts\nExplain the <!-- suggested-prompts marker -->\nMore";
    assert_eq!(clean_response_text(fused_mention, false), "Done.\n\nMore");
}

/// A finished reply keeps an unclosed block's text (like the desktop); only a
/// block still streaming hides the rest, so no prompt line leaks mid-turn.
#[test]
fn suggested_prompts_unclosed_block_hides_only_while_streaming() {
    let text = "Done.\n<!-- suggested-prompts\nA --> B\nRun tests";
    assert_eq!(clean_response_text(text, false), text);
    assert_eq!(clean_response_text(text, true), "Done.");
    let blocks = vec![format!("{text}\n")];
    assert_eq!(
        live_response_and_digest_from_blocks(&blocks, true)
            .0
            .as_deref(),
        Some("Done.")
    );
    let fenced = "<!-- suggested-prompts\n\n```markdown\n-->\n```\n\nBody text that must survive.";
    assert_eq!(
        clean_response_text(fenced, false),
        "```\n\nBody text that must survive."
    );
}

/// The other spelling's opener before any closer means the first block was
/// never closed: it stays, and the second block is removed.
#[test]
fn suggested_prompts_either_spelling_reopens() {
    let text = "A\n<!-- suggested-prompts\nX\n&lt;!-- suggested-prompts\nRun\n--&gt;\nTail";
    assert_eq!(
        clean_response_text(text, false),
        "A\n<!-- suggested-prompts\nX\n\nTail"
    );
}

/// Persisted previews keep a block's tail, which can start inside a code
/// fence: fences are not tracked, so the real block is still removed.
#[test]
fn suggested_prompts_block_after_a_cut_fence_is_removed() {
    let tail = "x\n```\nDone.\n<!-- suggested-prompts\nRun tests\n-->";
    assert_eq!(clean_response_text(tail, false), "x\n```\nDone.");
}
