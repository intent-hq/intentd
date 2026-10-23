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
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
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
        match case["divergences"][CONSUMER].as_str() {
            Some(reason) if got == want => failures.push(format!(
                "{id}: listed as an intentd divergence ({reason}) but now matches; remove the entry"
            )),
            Some(_) => {}
            None if got != want => failures.push(format!(
                "{id}: cleaned text differs\n  want: {want:?}\n  got:  {got:?}"
            )),
            None => {}
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

/// Without a line-ending closer the old first-closer rule still applies, so
/// an inline span keeps its preview.
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
