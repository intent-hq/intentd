//! Vocabulary + context merging for transcription biasing.
//!
//! The daemon biases every transcription with a user-editable vocabulary
//! (the `voice.vocabulary` setting, read per call by the service layer),
//! plus a short style hint for prompt-based providers. Request-supplied
//! context (`prompt`, `keyterms`) is merged on top: `OpenAI` receives one
//! composed `prompt` string; `ElevenLabs` receives the merged `keyterms` array
//! sanitized to the Scribe v2 limits: the unsupported characters
//! `<` `>` `{` `}` `[` `]` `\` are stripped and whitespace runs collapsed,
//! then terms that are blank, longer than [`MAX_KEYTERM_CHARS`] chars, or more
//! than [`MAX_KEYTERM_WORDS`] words are dropped; the result is deduped
//! case-insensitively and capped at [`MAX_KEYTERMS`].

/// Style hint prefixed to the composed `OpenAI` `prompt`.
pub(crate) const OPENAI_STYLE_HINT: &str = "Technical dictation in a software-engineering app; \
     preserve code identifiers and file paths verbatim.";

/// `ElevenLabs` Scribe v2 keyterm cap (batch API allows up to 100 terms).
pub(crate) const MAX_KEYTERMS: usize = 100;

/// `ElevenLabs` Scribe v2 per-keyterm length cap.
pub(crate) const MAX_KEYTERM_CHARS: usize = 50;

/// `ElevenLabs` Scribe v2 per-keyterm word cap.
pub(crate) const MAX_KEYTERM_WORDS: usize = 5;

/// Characters `ElevenLabs` Scribe v2 rejects in keyterms.
const REJECTED_CHARS: [char; 7] = ['<', '>', '{', '}', '[', ']', '\\'];

/// Sanitize one candidate keyterm to the `ElevenLabs` Scribe v2 rules:
/// strip [`REJECTED_CHARS`], collapse whitespace runs to single spaces, and
/// trim. Returns the sanitized spelling.
fn sanitize_keyterm(term: &str) -> String {
    term.chars()
        .filter(|c| !REJECTED_CHARS.contains(c))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Merge the configured `vocabulary` with request `keyterms`: vocabulary
/// terms first, then request terms; each term sanitized via
/// [`sanitize_keyterm`]; duplicates dropped case-insensitively on the
/// sanitized spelling (first spelling wins); terms that are blank, longer
/// than [`MAX_KEYTERM_CHARS`] chars, or more than [`MAX_KEYTERM_WORDS`]
/// words after sanitization are skipped; capped at [`MAX_KEYTERMS`].
#[must_use]
pub fn merge_keyterms(vocabulary: &[String], request_keyterms: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for term in vocabulary.iter().chain(request_keyterms.iter()) {
        let sanitized = sanitize_keyterm(term);
        if sanitized.is_empty()
            || sanitized.chars().count() > MAX_KEYTERM_CHARS
            || sanitized.split_whitespace().count() > MAX_KEYTERM_WORDS
        {
            continue;
        }
        if !seen.insert(sanitized.to_lowercase()) {
            continue;
        }
        out.push(sanitized);
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
    fn strips_each_rejected_character() {
        let cases = [
            ("[fix] task", "fix task"),
            ("<intentd>", "intentd"),
            ("{brace}", "brace"),
            ("back\\slash", "backslash"),
        ];
        for (input, expected) in cases {
            let merged = merge_keyterms(&[], &[input.to_string()]);
            assert_eq!(merged, vec![expected.to_string()], "input: {input:?}");
        }
    }

    #[test]
    fn collapses_whitespace_runs() {
        let merged = merge_keyterms(&[], &["foo \t bar   baz".to_string()]);
        assert_eq!(merged, vec!["foo bar baz".to_string()]);
    }

    #[test]
    fn drops_terms_blank_after_stripping() {
        let merged = merge_keyterms(&[], &["[] {} <> \\".to_string()]);
        assert!(merged.is_empty());
    }

    #[test]
    fn drops_terms_with_more_than_five_words() {
        let six = "one two three four five six".to_string();
        let five = "one two three four five".to_string();
        let merged = merge_keyterms(&[], &[six, five.clone()]);
        assert_eq!(merged, vec![five]);
    }

    #[test]
    fn dedupes_on_sanitized_spelling() {
        let merged = merge_keyterms(&[], &["[fix] task".to_string(), "fix task".to_string()]);
        assert_eq!(merged, vec!["fix task".to_string()]);
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
