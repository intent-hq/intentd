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

use std::fmt::Write as _;

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::clean_response_text;

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
    let digest = Sha256::digest(CASES.as_bytes());
    let hex = digest.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    });
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
        let got = normalize(&clean_response_text(input));
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
    ] {
        assert_eq!(clean_response_text(text), "Done.", "{text:?}");
    }
}

/// An inline `opener … closer` span within one line is still removed, and an
/// opener with no closer hides the rest of the text (a block still streaming).
#[test]
fn suggested_prompts_inline_span_keeps_the_first_closer_rule() {
    assert_eq!(
        clean_response_text("Write a <!-- suggested-prompts block --> at the end."),
        "Write a  at the end."
    );
    assert_eq!(
        clean_response_text("Done.\n<!-- suggested-prompts\nRun"),
        "Done."
    );
}

/// An inline mention of the opener never reaches past its own line, so it
/// cannot swallow text up to a later block's closer.
#[test]
fn suggested_prompts_inline_mention_does_not_swallow_later_text() {
    let text = "Updated the `<!-- suggested-prompts -->` parser.\nAll tests pass.\n\
                <!-- suggested-prompts\nRun tests\n-->";
    assert_eq!(
        clean_response_text(text),
        "Updated the `` parser.\nAll tests pass."
    );
    let unclosed_inline =
        "See <!-- suggested-prompts here.\nTail.\n<!-- suggested-prompts\nRun\n-->";
    assert_eq!(
        clean_response_text(unclosed_inline),
        "See <!-- suggested-prompts here.\nTail."
    );
}

/// A block still streaming (no closer yet) hides the rest of the text, even
/// when a body line has an embedded arrow, and a fenced example stays.
#[test]
fn suggested_prompts_unclosed_block_and_fenced_example() {
    assert_eq!(
        clean_response_text("Done.\n<!-- suggested-prompts\nA --> B\nRun tests\n"),
        "Done."
    );
    let fenced = "Format:\n```\n<!-- suggested-prompts\nExample\n-->\n```\nEnd.";
    assert_eq!(clean_response_text(fenced), fenced);
}
