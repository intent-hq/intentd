//! Vocabulary + context merging for transcription biasing.
//!
//! The daemon biases every transcription with a user-editable vocabulary
//! (the `voice.vocabulary` setting, read per call by the service layer),
//! plus a short style hint for prompt-based providers. Request-supplied
//! context (`prompt`, `keyterms`) is merged on top: `OpenAI` receives one
//! composed `prompt` string; `ElevenLabs` receives the merged `keyterms` array
//! (deduped case-insensitively, capped at [`MAX_KEYTERMS`], each term ≤
//! [`MAX_KEYTERM_CHARS`] chars — the Scribe v2 limits).

/// Style hint prefixed to the composed `OpenAI` `prompt`.
pub(crate) const OPENAI_STYLE_HINT: &str = "Technical dictation in a software-engineering app; \
     preserve code identifiers and file paths verbatim.";

/// `ElevenLabs` Scribe v2 keyterm cap (batch API allows up to 100 terms).
pub(crate) const MAX_KEYTERMS: usize = 100;

/// `ElevenLabs` Scribe v2 per-keyterm length cap.
pub(crate) const MAX_KEYTERM_CHARS: usize = 50;

/// Merge the configured `vocabulary` with request `keyterms`: vocabulary
/// terms first, then request terms; duplicates dropped case-insensitively
/// (first spelling wins); terms longer than [`MAX_KEYTERM_CHARS`] chars or
/// blank are skipped; capped at [`MAX_KEYTERMS`].
#[must_use]
pub fn merge_keyterms(vocabulary: &[String], request_keyterms: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for term in vocabulary.iter().chain(request_keyterms.iter()) {
        let trimmed = term.trim();
        if trimmed.is_empty() || trimmed.chars().count() > MAX_KEYTERM_CHARS {
            continue;
        }
        if !seen.insert(trimmed.to_lowercase()) {
            continue;
        }
        out.push(trimmed.to_string());
        if out.len() >= MAX_KEYTERMS {
            break;
        }
    }
    out
}

/// Compose the `OpenAI` `prompt`: style hint, then the merged vocabulary as a
/// comma-separated list, then the request `prompt` (when present).
#[must_use]
pub fn compose_prompt(keyterms: &[String], request_prompt: Option<&str>) -> String {
    let mut prompt = String::from(OPENAI_STYLE_HINT);
    if !keyterms.is_empty() {
        prompt.push_str(" Vocabulary: ");
        prompt.push_str(&keyterms.join(", "));
        prompt.push('.');
    }
    if let Some(extra) = request_prompt {
        let trimmed = extra.trim();
        if !trimmed.is_empty() {
            prompt.push(' ');
            prompt.push_str(trimmed);
        }
    }
    prompt
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vocab(terms: &[&str]) -> Vec<String> {
        terms.iter().map(std::string::ToString::to_string).collect()
    }

    #[test]
    fn merges_vocabulary_then_request_terms() {
        let merged = merge_keyterms(
            &vocab(&["intentd", "Svelte"]),
            &["Endara".to_string(), "TOON".to_string()],
        );
        assert_eq!(merged[0], "intentd", "vocabulary terms come first");
        assert!(merged.contains(&"Endara".to_string()));
        assert!(merged.contains(&"TOON".to_string()));
    }

    #[test]
    fn empty_vocabulary_yields_only_request_terms() {
        let merged = merge_keyterms(&[], &["Endara".to_string()]);
        assert_eq!(merged, vec!["Endara".to_string()]);
    }

    #[test]
    fn dedupes_case_insensitively() {
        let merged = merge_keyterms(
            &vocab(&["intentd", "Svelte"]),
            &["INTENTD".to_string(), "svelte".to_string()],
        );
        let lower: Vec<String> = merged.iter().map(|t| t.to_lowercase()).collect();
        let unique: std::collections::HashSet<_> = lower.iter().collect();
        assert_eq!(lower.len(), unique.len(), "no case-insensitive dupes");
        assert!(
            merged.contains(&"intentd".to_string()),
            "first spelling wins"
        );
    }

    #[test]
    fn skips_blank_and_overlong_terms() {
        let long = "x".repeat(MAX_KEYTERM_CHARS + 1);
        let merged = merge_keyterms(&[], &["  ".to_string(), long.clone()]);
        assert!(!merged.iter().any(|t| t == &long));
        assert!(!merged.iter().any(|t| t.trim().is_empty()));
    }

    #[test]
    fn caps_at_max_keyterms() {
        let many: Vec<String> = (0..200).map(|i| format!("term{i}")).collect();
        let merged = merge_keyterms(&[], &many);
        assert_eq!(merged.len(), MAX_KEYTERMS);
    }

    #[test]
    fn composes_prompt_with_hint_vocab_and_request() {
        let keyterms = vec!["intentd".to_string(), "clippy".to_string()];
        let p = compose_prompt(&keyterms, Some("Discussing release automation."));
        assert!(p.starts_with(OPENAI_STYLE_HINT));
        assert!(p.contains("Vocabulary: intentd, clippy."));
        assert!(p.ends_with("Discussing release automation."));
    }

    #[test]
    fn composes_prompt_without_request_prompt() {
        let p = compose_prompt(&[], None);
        assert_eq!(p, OPENAI_STYLE_HINT);
        let p2 = compose_prompt(&[], Some("   "));
        assert_eq!(p2, OPENAI_STYLE_HINT);
    }
}
