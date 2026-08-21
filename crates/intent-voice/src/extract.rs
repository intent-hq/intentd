//! Pure vocabulary extraction + ranking for workspace dictation biasing.
//!
//! Given workspace source texts (README/AGENTS/spec-style documents),
//! [`extract_vocabulary`] produces a deterministic ranked list of vocabulary
//! terms — unique/non-dictionary and rare words worth biasing transcription
//! with (e.g. "intentd", "clippy", "submodule"). Pure functions only: no
//! file, store, or network access.
//!
//! Candidates are identifier-shaped tokens (camelCase, `snake_case`,
//! kebab-case, dotted, digit-bearing, ALL-CAPS acronyms) and plain or
//! capitalized words that are absent from — or rare in — the embedded
//! English frequency list. URLs, emails, hex strings, UUIDs, and pure
//! numbers are stripped. Each term scores
//! `rarity × salience × dictation-usefulness`; output is deduped
//! case-insensitively (most frequent spelling wins), capped at `max_terms`,
//! every term ≤ [`MAX_KEYTERM_CHARS`] chars.
//!
//! ## Embedded frequency list — provenance and license
//!
//! `assets/en_50k_zipf.txt` (~427 KB, compiled in via `include_str!`) holds
//! the top 50,000 English word forms grouped into Zipf bands (7 = most
//! common … 1 = rarest; band = `clamp(round(log10(count/T × 1e9)), 1, 7)`
//! with `T` = 743,842,922,321 summed source counts). It is derived from
//! `google-books-common-words.txt` published by Peter Norvig
//! (<https://norvig.com/mayzner.html>), itself derived from the Google Books
//! Ngram corpus (English, version 20120701,
//! <https://books.google.com/ngrams/>), which Google licenses under the
//! Creative Commons Attribution 3.0 Unported License (CC BY 3.0,
//! <https://creativecommons.org/licenses/by/3.0/>). CC BY 3.0 permits
//! redistribution and commercial use with attribution, which this note and
//! the asset header provide.

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use crate::context::MAX_KEYTERM_CHARS;

/// Kind of a source document fed to [`extract_vocabulary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// Markdown: code fences and inline code are kept (and boosted — highest
    /// signal), heading text is boosted, `[text](target)` link targets are
    /// stripped.
    Markdown,
    /// Plain prose: tokenized line by line with no markdown handling.
    Plain,
}

/// Minimum term length in chars; shorter tokens are noise for dictation.
const MIN_TERM_CHARS: usize = 3;

/// Words at or below this Zipf band count as "rare" (candidates); words
/// above it are everyday English and are never emitted on their own.
const RARE_MAX_BAND: u8 = 3;

/// Occurrence weight for heading text.
const HEADING_WEIGHT: f64 = 2.0;

/// Occurrence weight for code (fenced blocks and inline code).
const CODE_WEIGHT: f64 = 1.5;

/// Occurrence weight for plain prose.
const PLAIN_WEIGHT: f64 = 1.0;

static EN_50K_ZIPF: &str = include_str!("../assets/en_50k_zipf.txt");

/// Zipf band (7 = most common … 1 = rarest) for a lowercase word, or `None`
/// when the word is not among the embedded top-50k forms.
fn zipf_band(word_lower: &str) -> Option<u8> {
    static MAP: OnceLock<HashMap<&'static str, u8>> = OnceLock::new();
    let map = MAP.get_or_init(|| {
        let mut m = HashMap::new();
        let mut band = 1u8;
        for line in EN_50K_ZIPF.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(rest) = line.strip_prefix('=') {
                band = rest.parse().unwrap_or(1);
            } else {
                m.insert(line, band);
            }
        }
        m
    });
    map.get(word_lower).copied()
}

/// Where a token occurrence was found; drives its salience weight.
#[derive(Debug, Clone, Copy)]
enum Context {
    Plain,
    Heading,
    Code,
}

impl Context {
    fn weight(self) -> f64 {
        match self {
            Context::Plain => PLAIN_WEIGHT,
            Context::Heading => HEADING_WEIGHT,
            Context::Code => CODE_WEIGHT,
        }
    }
}

/// Per-term (case-insensitive key) accumulation state.
struct TermStats {
    /// Sum of context weights across occurrences.
    weighted: f64,
    /// Distinct source indexes the term appeared in.
    sources: BTreeSet<usize>,
    /// Spelling → (occurrence count, first-seen token index).
    spellings: HashMap<String, (usize, usize)>,
    /// First-seen token index across all spellings (deterministic ties).
    first_idx: usize,
}

impl TermStats {
    /// Most frequent spelling; ties broken by earliest occurrence, then
    /// lexicographically, so the result is deterministic.
    fn best_spelling(&self) -> &str {
        self.spellings
            .iter()
            .min_by(|(sa, (ca, ia)), (sb, (cb, ib))| cb.cmp(ca).then(ia.cmp(ib)).then(sa.cmp(sb)))
            .map(|(s, _)| s.as_str())
            .expect("stats entries always hold at least one spelling")
    }
}

/// Extract a ranked vocabulary from `sources`, best terms first.
///
/// Deterministic for identical input. At most `max_terms` terms; each term
/// is 3..=[`MAX_KEYTERM_CHARS`] chars; no case-insensitive duplicates (the
/// most frequent spelling wins).
#[must_use]
// Term/source counts are far below 2^53: loss-free in f64.
#[allow(clippy::cast_precision_loss)]
pub fn extract_vocabulary(sources: &[(SourceKind, &str)], max_terms: usize) -> Vec<String> {
    if max_terms == 0 {
        return Vec::new();
    }
    let mut stats: HashMap<String, TermStats> = HashMap::new();
    let mut next_idx = 0usize;
    for (source_idx, (kind, text)) in sources.iter().enumerate() {
        scan_source(*kind, text, &mut |token, ctx| {
            record(&mut stats, token, ctx, source_idx, &mut next_idx);
        });
    }

    let mut ranked: Vec<(f64, usize, String)> = Vec::with_capacity(stats.len());
    for (key, term) in &stats {
        let spelling = term.best_spelling();
        let Some(rarity) = candidate_rarity(spelling, key) else {
            continue;
        };
        let salience = (1.0 + term.weighted.ln()) * (1.0 + (term.sources.len() as f64).ln());
        let score = rarity * salience * usefulness(spelling);
        ranked.push((score, term.first_idx, spelling.to_string()));
    }
    ranked.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    ranked.truncate(max_terms);
    ranked
        .into_iter()
        .map(|(_, _, spelling)| spelling)
        .collect()
}

/// Record one token occurrence if it passes the shape filters and is a
/// vocabulary candidate.
fn record(
    stats: &mut HashMap<String, TermStats>,
    token: &str,
    ctx: Context,
    source_idx: usize,
    next_idx: &mut usize,
) {
    let n = token.chars().count();
    if !(MIN_TERM_CHARS..=MAX_KEYTERM_CHARS).contains(&n) {
        return;
    }
    if is_excluded_shape(token) {
        return;
    }
    let lower = token.to_lowercase();
    if candidate_rarity(token, &lower).is_none() {
        return;
    }
    let idx = *next_idx;
    *next_idx += 1;
    let entry = stats.entry(lower).or_insert_with(|| TermStats {
        weighted: 0.0,
        sources: BTreeSet::new(),
        spellings: HashMap::new(),
        first_idx: idx,
    });
    entry.weighted += ctx.weight();
    entry.sources.insert(source_idx);
    entry
        .spellings
        .entry(token.to_string())
        .or_insert((0, idx))
        .0 += 1;
}

/// Walk a source, feeding `(token, context)` pairs to `sink`.
fn scan_source(kind: SourceKind, text: &str, sink: &mut impl FnMut(&str, Context)) {
    match kind {
        SourceKind::Plain => {
            for line in text.lines() {
                scan_chunks(line, Context::Plain, sink);
            }
        }
        SourceKind::Markdown => {
            let mut in_fence = false;
            for line in text.lines() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
                    in_fence = !in_fence;
                    continue;
                }
                if in_fence {
                    scan_chunks(line, Context::Code, sink);
                    continue;
                }
                let heading = is_heading(trimmed);
                let line = strip_link_targets(line);
                for (i, part) in line.split('`').enumerate() {
                    let ctx = if i % 2 == 1 {
                        Context::Code
                    } else if heading {
                        Context::Heading
                    } else {
                        Context::Plain
                    };
                    scan_chunks(part, ctx, sink);
                }
            }
        }
    }
}

/// ATX heading: 1–6 `#`s followed by a space (or end of line).
fn is_heading(trimmed: &str) -> bool {
    let hashes = trimmed.chars().take_while(|&c| c == '#').count();
    (1..=6).contains(&hashes) && matches!(trimmed.chars().nth(hashes), None | Some(' '))
}

/// Drop `[text](target)` link targets so URLs/paths inside them are never
/// tokenized; the link text is kept.
fn strip_link_targets(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(i) = rest.find("](") {
        out.push_str(&rest[..=i]);
        let after = &rest[i + 2..];
        if let Some(j) = after.find(')') {
            rest = &after[j + 1..];
        } else {
            rest = "";
            break;
        }
    }
    out.push_str(rest);
    out
}

/// Split whitespace chunks, drop URL/email-looking chunks whole, then emit
/// maximal `[alphanumeric - _ .]` runs (trimmed of edge separators).
fn scan_chunks(text: &str, ctx: Context, sink: &mut impl FnMut(&str, Context)) {
    for chunk in text.split_whitespace() {
        if chunk.contains("://") || chunk.contains('@') || chunk.starts_with("www.") {
            continue;
        }
        let mut cur = String::new();
        for c in chunk.chars() {
            if c.is_alphanumeric() || matches!(c, '-' | '_' | '.') {
                cur.push(c);
            } else {
                emit(&mut cur, ctx, sink);
            }
        }
        emit(&mut cur, ctx, sink);
    }
}

fn emit(cur: &mut String, ctx: Context, sink: &mut impl FnMut(&str, Context)) {
    if !cur.is_empty() {
        let token = cur.trim_matches(|c| matches!(c, '.' | '-' | '_'));
        if !token.is_empty() {
            sink(token, ctx);
        }
        cur.clear();
    }
}

/// Shapes that are never vocabulary: pure numbers/separators, hex strings,
/// UUIDs, and dotted abbreviations like `e.g` / `i.e`.
fn is_excluded_shape(token: &str) -> bool {
    if !token.chars().any(char::is_alphabetic) {
        return true;
    }
    if is_hexish(token) || is_uuid(token) {
        return true;
    }
    token.contains('.') && token.split('.').any(|seg| seg.chars().count() < 2)
}

fn is_hexish(token: &str) -> bool {
    let all_hex = token.chars().all(|c| c.is_ascii_hexdigit());
    if all_hex && token.len() >= 7 && token.chars().any(|c| c.is_ascii_digit()) {
        return true;
    }
    (token.starts_with("0x") || token.starts_with("0X"))
        && token.len() > 2
        && token[2..].chars().all(|c| c.is_ascii_hexdigit())
}

fn is_uuid(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(i, &b)| match i {
            8 | 13 | 18 | 23 => b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
}

/// Rarity factor (1.0–8.0) when the token is a vocabulary candidate, `None`
/// when it is everyday English not worth biasing with.
///
/// Identifier-shaped tokens (separators, digits, or interior case changes)
/// are always candidates — except all-lowercase hyphen compounds of common
/// words ("well-known") — and take their rarity from their rarest alphabetic
/// segment. Plain/capitalized/ALL-CAPS words are candidates only when absent
/// from or rare in the frequency list (band ≤ [`RARE_MAX_BAND`]); absent
/// means maximum rarity.
fn candidate_rarity(spelling: &str, lower: &str) -> Option<f64> {
    let has_sep = spelling.chars().any(|c| matches!(c, '-' | '_' | '.'));
    let has_digit = spelling.chars().any(|c| c.is_ascii_digit());
    let camel = spelling.chars().skip(1).any(char::is_uppercase)
        && spelling.chars().any(char::is_lowercase);
    if has_sep || has_digit || camel {
        if is_natural_compound(spelling) {
            return None;
        }
        let min_band = alpha_segments(spelling)
            .iter()
            .map(|seg| zipf_band(seg).unwrap_or(0))
            .min()
            .unwrap_or(0);
        Some(f64::from(8 - min_band.min(7)))
    } else {
        let band = zipf_band(lower).unwrap_or(0);
        if band > RARE_MAX_BAND {
            return None;
        }
        Some(f64::from(8 - band))
    }
}

/// All-lowercase hyphen compound whose every segment is a common dictionary
/// word — natural English ("well-known", "built-in"), not vocabulary.
fn is_natural_compound(token: &str) -> bool {
    token.contains('-')
        && token.chars().all(|c| c == '-' || c.is_lowercase())
        && token
            .split('-')
            .all(|seg| !seg.is_empty() && zipf_band(seg).is_some_and(|b| b > RARE_MAX_BAND))
}

/// Lowercased alphabetic segments (≥ 2 chars) split at separators, digits,
/// and camelCase hump boundaries.
fn alpha_segments(token: &str) -> Vec<String> {
    let chars: Vec<char> = token.chars().collect();
    let mut segs = Vec::new();
    let mut cur = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_alphabetic() {
            flush_segment(&mut cur, &mut segs);
            continue;
        }
        if i > 0 {
            let prev = chars[i - 1];
            let hump = (prev.is_lowercase() && c.is_uppercase())
                || (prev.is_uppercase()
                    && c.is_uppercase()
                    && chars.get(i + 1).is_some_and(|n| n.is_lowercase()));
            if hump {
                flush_segment(&mut cur, &mut segs);
            }
        }
        cur.extend(c.to_lowercase());
    }
    flush_segment(&mut cur, &mut segs);
    segs
}

fn flush_segment(cur: &mut String, segs: &mut Vec<String>) {
    if cur.chars().count() >= 2 {
        segs.push(std::mem::take(cur));
    } else {
        cur.clear();
    }
}

/// Dictation-usefulness factor: penalizes overlong tokens, separator-heavy
/// path-like tokens, and vowel-less (unpronounceable) strings.
// Token lengths are far below 2^53: loss-free in f64.
#[allow(clippy::cast_precision_loss)]
fn usefulness(spelling: &str) -> f64 {
    let n = spelling.chars().count();
    let mut factor = 1.0;
    if n > 24 {
        factor *= 24.0 / n as f64;
    }
    let seps = spelling
        .chars()
        .filter(|c| matches!(c, '-' | '_' | '.'))
        .count();
    if seps >= 3 {
        factor *= 0.5;
    }
    let has_vowel = spelling
        .to_lowercase()
        .chars()
        .any(|c| matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'));
    if n >= 5 && !has_vowel {
        factor *= 0.5;
    }
    factor
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;

    fn extract(sources: &[(SourceKind, &str)]) -> Vec<String> {
        extract_vocabulary(sources, 100)
    }

    fn lower(terms: &[String]) -> Vec<String> {
        terms.iter().map(|t| t.to_lowercase()).collect()
    }

    #[test]
    fn extracts_identifier_shapes() {
        let out = extract(&[(
            SourceKind::Markdown,
            "workspaceId uses snake_case_name and kebab-case-token plus cargo.toml, gpt-4o, WSS.",
        )]);
        for term in [
            "workspaceId",
            "snake_case_name",
            "kebab-case-token",
            "cargo.toml",
            "gpt-4o",
            "WSS",
        ] {
            assert!(out.contains(&term.to_string()), "missing {term} in {out:?}");
        }
        assert!(!lower(&out).contains(&"uses".to_string()));
        assert!(!lower(&out).contains(&"and".to_string()));
    }

    #[test]
    fn rare_real_words_rank_common_words_do_not() {
        let out = extract(&[(
            SourceKind::Plain,
            "The daemon and the submodule build the test of the release.",
        )]);
        let low = lower(&out);
        assert!(low.contains(&"daemon".to_string()), "{out:?}");
        assert!(low.contains(&"submodule".to_string()), "{out:?}");
        for common in ["the", "build", "test", "release"] {
            assert!(!low.contains(&common.to_string()), "{common} in {out:?}");
        }
    }

    #[test]
    fn non_dictionary_words_rank() {
        let out = extract(&[(SourceKind::Plain, "intentd transcribes with clippy today")]);
        let low = lower(&out);
        assert!(low.contains(&"intentd".to_string()));
        assert!(low.contains(&"clippy".to_string()));
        assert!(!low.contains(&"today".to_string()));
    }

    #[test]
    fn multi_source_appearance_outranks_single_source() {
        let out = extract(&[
            (SourceKind::Plain, "flumox brumble brumble"),
            (SourceKind::Plain, "flumox"),
        ]);
        let flumox = out.iter().position(|t| t == "flumox").unwrap();
        let brumble = out.iter().position(|t| t == "brumble").unwrap();
        assert!(flumox < brumble, "{out:?}");
    }

    #[test]
    fn heading_occurrence_outranks_body_occurrence() {
        let out = extract(&[(SourceKind::Markdown, "betacorp\n\n# Alphacorp\n")]);
        let alpha = out.iter().position(|t| t == "Alphacorp").unwrap();
        let beta = out.iter().position(|t| t == "betacorp").unwrap();
        assert!(alpha < beta, "{out:?}");
    }

    #[test]
    fn code_fences_are_kept_and_boosted() {
        let out = extract(&[(
            SourceKind::Markdown,
            "zorgle\n\n```rust\nflibberwock\n```\n",
        )]);
        let fenced = out.iter().position(|t| t == "flibberwock").unwrap();
        let plain = out.iter().position(|t| t == "zorgle").unwrap();
        assert!(fenced < plain, "{out:?}");
    }

    #[test]
    fn inline_code_is_kept() {
        let out = extract(&[(SourceKind::Markdown, "call the `merge_keyterms` helper")]);
        assert!(out.contains(&"merge_keyterms".to_string()), "{out:?}");
    }

    #[test]
    fn link_targets_are_stripped_link_text_kept() {
        let out = extract(&[(
            SourceKind::Markdown,
            "[Cloudlands guide](https://internal.example.com/x) and [notes](../docs/scrivenly.md)",
        )]);
        assert!(out.contains(&"Cloudlands".to_string()), "{out:?}");
        let low = lower(&out);
        assert!(!low.iter().any(|t| t.contains("example")), "{out:?}");
        assert!(!low.iter().any(|t| t.contains("scrivenly")), "{out:?}");
    }

    #[test]
    fn urls_emails_hex_uuids_numbers_are_excluded() {
        let out = extract(&[(
            SourceKind::Plain,
            "See https://example.com/docs and mail me@example.com about 0xdeadbeef \
             12345 3.14 550e8400-e29b-41d4-a716-446655440000 abc123f7 www.example.com",
        )]);
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn dotted_abbreviations_and_natural_compounds_are_excluded() {
        let out = extract(&[(
            SourceKind::Plain,
            "e.g. i.e. a well-known built-in and so-called thing --- ...",
        )]);
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn all_caps_emphasis_of_common_words_is_excluded() {
        let out = extract(&[(SourceKind::Plain, "NOTE IMPORTANT WSS UDS")]);
        let low = lower(&out);
        assert!(!low.contains(&"note".to_string()), "{out:?}");
        assert!(!low.contains(&"important".to_string()), "{out:?}");
        assert!(low.contains(&"wss".to_string()), "{out:?}");
        assert!(low.contains(&"uds".to_string()), "{out:?}");
    }

    #[test]
    fn dedupes_case_insensitively_most_frequent_spelling_wins() {
        let out = extract(&[(SourceKind::Plain, "Intentd intentd INTENTD intentd")]);
        assert_eq!(out, vec!["intentd".to_string()]);
    }

    #[test]
    fn preserves_original_spelling() {
        let out = extract(&[(SourceKind::Plain, "Svelte ships workspaceId")]);
        assert!(out.contains(&"Svelte".to_string()), "{out:?}");
        assert!(out.contains(&"workspaceId".to_string()), "{out:?}");
    }

    #[test]
    fn caps_at_max_terms() {
        let text = (0..20).fold(String::new(), |mut s, i| {
            let _ = write!(s, "zzterm{i:02}x ");
            s
        });
        let out = extract_vocabulary(&[(SourceKind::Plain, &text)], 5);
        assert_eq!(out.len(), 5);
    }

    #[test]
    fn max_terms_zero_yields_empty() {
        let out = extract_vocabulary(&[(SourceKind::Plain, "intentd clippy")], 0);
        assert!(out.is_empty());
    }

    #[test]
    fn respects_term_length_cap() {
        let ok = "a".repeat(MAX_KEYTERM_CHARS);
        let too_long = "b".repeat(MAX_KEYTERM_CHARS + 1);
        let text = format!("{ok} {too_long}");
        let out = extract(&[(SourceKind::Plain, &text)]);
        assert!(out.contains(&ok), "{out:?}");
        assert!(!out.contains(&too_long), "{out:?}");
        assert!(out.iter().all(|t| t.chars().count() <= MAX_KEYTERM_CHARS));
    }

    #[test]
    fn output_is_deterministic() {
        let sources = [
            (
                SourceKind::Markdown,
                "# intentd daemon\n\nUses `cargo clippy`, release-plz, sqlx and \
                 workspaceId across submodule crates.\n\n```bash\ncargo fmt --check\n```\n",
            ),
            (
                SourceKind::Plain,
                "The daemon transcribes via ElevenLabs Scribe, OpenAI, and WSS listeners.",
            ),
        ];
        let a = extract(&sources);
        let b = extract(&sources);
        assert_eq!(a, b);
        assert!(!a.is_empty());
    }

    #[test]
    fn zipf_band_lookup_matches_expectations() {
        assert!(zipf_band("the").unwrap() >= 6);
        assert!(zipf_band("build").unwrap() > RARE_MAX_BAND);
        assert!(zipf_band("daemon").unwrap() <= RARE_MAX_BAND);
        assert_eq!(zipf_band("intentd"), None);
    }
}
