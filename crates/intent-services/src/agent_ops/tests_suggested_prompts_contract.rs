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

use super::{clean_response_text, last_response_and_digest_from_blocks};
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
        "Done.\n<!--suggested-prompts\nRun tests\n-->",
        "Done.\n<!--\tsuggested-prompts  \nRun tests\n-->",
        "Done.\n<!-- suggested-prompts\n<!-- note -->\nRun\n-->",
    ] {
        assert_eq!(clean_response_text(text), "Done.", "{text:?}");
    }
}

/// Anything that is not a well-formed block keeps the first-closer rule, so
/// the preview never shows prompt lines or markup: unclosed blocks hide to the
/// end, mismatched spellings and openers with text on their line too.
#[test]
fn suggested_prompts_malformed_markup_stays_hidden() {
    for text in [
        "Done.\n<!-- suggested-prompts\nRun tests",
        "Done.\n<!-- suggested-prompts Run tests\nReview\n-->",
        "Done.\n\n&lt;!-- suggested-prompts\nRun the tests.\n-->",
        "Done.\n\n<!-- suggested-prompts\nRun --> tests\n",
        "Done.\n\n<!-- suggested-prompts\nRun --> tests\nOpen PR",
        "Done.\n<!--suggested-prompts\nRun tests\n",
        "Done.\n<!-- suggested-prompts\nA --> B\nRun tests\n--></group>",
    ] {
        assert_eq!(
            last_response_and_digest_from_blocks(&[text.to_owned()])
                .0
                .as_deref(),
            Some("Done."),
            "{text:?}"
        );
    }
}

/// Inline mentions keep their old preview, and one cannot swallow the text up
/// to a later real block's closer: the real block is removed first.
#[test]
fn suggested_prompts_inline_mentions() {
    assert_eq!(
        clean_response_text("Write a <!-- suggested-prompts block --> at the end."),
        "Write a  at the end."
    );
    let text = "Updated the `<!-- suggested-prompts -->` parser.\nAll tests pass.\n\
                <!-- suggested-prompts\nRun tests\n-->";
    assert_eq!(
        clean_response_text(text),
        "Updated the `` parser.\nAll tests pass."
    );
    let fused_mention =
        "Done.\n<!-- suggested-prompts\nExplain the <!-- suggested-prompts marker -->\nMore";
    assert_eq!(clean_response_text(fused_mention), "Done.");
}

/// A broken opener before a valid block keeps the reply that follows the
/// block: removing the block must not leave the broken opener unclosed and
/// hide everything to the end (both spellings, reopened and inline).
#[test]
fn suggested_prompts_broken_opener_before_a_block_keeps_trailing_prose() {
    for text in [
        "Intro\n<!-- suggested-prompts\nOld\n<!-- suggested-prompts\nRun\n-->\nFinal answer.",
        "Intro\n&lt;!-- suggested-prompts\nOld\n&lt;!-- suggested-prompts\nRun\n--&gt;\nFinal answer.",
        "Intro\n<!-- suggested-prompts\nOld\n&lt;!-- suggested-prompts\nRun\n--&gt;\nFinal answer.",
        "See <!-- suggested-prompts here.\nTail.\n<!-- suggested-prompts\nRun\n-->\nFinal answer.",
        "See &lt;!-- suggested-prompts here.\nTail.\n&lt;!-- suggested-prompts\nRun\n--&gt;\nFinal answer.",
        "See <!-- suggested-prompts here.\n<!-- suggested-prompts\nA --> B\nRun\n-->\nFinal answer.",
    ] {
        assert_eq!(
            last_response_and_digest_from_blocks(&[text.to_owned()])
                .0
                .as_deref(),
            Some("Final answer."),
            "{text:?}"
        );
    }
}

/// Fences are not tracked: a persisted tail can start inside a code fence, and
/// the real block after it is still removed; a closer in a fenced example
/// still ends an open block, as in the old preview.
#[test]
fn suggested_prompts_fence_blind() {
    let tail = "x\n```\nDone.\n<!-- suggested-prompts\nRun tests\n-->";
    assert_eq!(clean_response_text(tail), "x\n```\nDone.");
    let fenced = "<!-- suggested-prompts\n\n```markdown\n-->\n```\n\nBody text that must survive.";
    assert_eq!(
        clean_response_text(fenced),
        "```\n\nBody text that must survive."
    );
}

/// Whatever a shared case expects of the desktop, the preview never shows
/// block markup: no opener and no bare closer line survives cleaning.
#[test]
fn suggested_prompts_preview_never_shows_markup() {
    for case in cases() {
        let id = case["id"].as_str().expect("id");
        let cleaned = clean_response_text(case["input"].as_str().expect("input"));
        for line in cleaned.lines() {
            let line = line.trim();
            assert!(
                !line.contains("suggested-prompts") && line != "-->" && line != "--&gt;",
                "{id}: markup in preview {cleaned:?}"
            );
        }
    }
}
