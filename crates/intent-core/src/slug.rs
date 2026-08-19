//! Friendly branch/workspace slugs (TS `workspace-slug.ts` +
//! `local-slug-generator.ts` parity).
//!
//! The reference app names auto-generated workspace branches with a
//! human-readable slug — `word-word` (e.g. `auth-fix`, `amber-forest`), with a
//! numeric `-N` suffix only on collision. The base comes from local keyword
//! extraction over the initial agent prompt when one exists, else from a
//! random adjective-animal pair. Collision suffixing itself lives with the
//! caller (`intent-git::branches::ensure_unique_branch_name`).

mod words;

use words::{ACTION_WORDS, ADJECTIVES, ANIMALS, STOP_WORDS};

/// A single slug word: 2–15 lowercase ASCII letters (TS `validWordPattern`).
fn is_valid_slug_word(w: &str) -> bool {
    (2..=15).contains(&w.len()) && w.bytes().all(|b| b.is_ascii_lowercase())
}

/// Generate a random `adjective-animal` base slug (TS
/// `generateWorkspaceSlug`). Uses a fresh UUIDv4 as the entropy source so the
/// crate needs no extra RNG dependency.
pub fn generate_workspace_slug() -> String {
    let bytes = *uuid::Uuid::new_v4().as_bytes();
    let adj = u16::from_le_bytes([bytes[0], bytes[1]]) as usize % ADJECTIVES.len();
    let animal = u16::from_le_bytes([bytes[2], bytes[3]]) as usize % ANIMALS.len();
    format!("{}-{}", ADJECTIVES[adj], ANIMALS[animal])
}

/// Append a numeric collision suffix (TS `appendSlugSuffix`):
/// `auth-fix` + 2 → `auth-fix-2`.
pub fn append_slug_suffix(base: &str, number: u32) -> String {
    format!("{base}-{number}")
}

/// Strip a trailing numeric collision suffix from a `word-word(-N)` slug (TS
/// `extractBaseSlug`); anything else is returned unchanged.
pub(crate) fn extract_base_slug(slug: &str) -> &str {
    let Some((base, tail)) = slug.rsplit_once('-') else {
        return slug;
    };
    if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) {
        let mut parts = base.split('-');
        if parts.clone().count() == 2 && parts.all(is_valid_slug_word) {
            return base;
        }
    }
    slug
}

/// Check whether a string looks like an auto-generated workspace slug — a
/// `word-word` (or `word-word-N` collision-suffixed) pair drawn from the
/// [`ADJECTIVES`]/[`ANIMALS`] dictionaries used by [`generate_workspace_slug`]
/// (TS `isWorkspaceSlug`). Used by callers that decide whether the workspace
/// still needs a human title — a slug-shaped title is treated as untitled.
pub fn is_workspace_slug(s: &str) -> bool {
    let base = extract_base_slug(s);
    let (adj, animal) = match base.split_once('-') {
        Some(pair) => pair,
        None => return false,
    };
    if animal.contains('-') || !is_valid_slug_word(adj) || !is_valid_slug_word(animal) {
        return false;
    }
    ADJECTIVES.contains(&adj) && ANIMALS.contains(&animal)
}

/// Extract a `word-word` slug from a user prompt using local heuristics (TS
/// `extractLocalSlug` / `generateLocalSlug`; fast, no LLM). Returns `None`
/// when nothing meaningful can be extracted — the caller falls back to
/// [`generate_workspace_slug`].
pub fn extract_local_slug(prompt: &str) -> Option<String> {
    if prompt.trim().len() < 3 {
        return None;
    }

    // Remove context mentions like `@context[...]` / `@file[...]`.
    let cleaned = strip_context_mentions(prompt);
    if cleaned.trim().len() < 3 {
        return None;
    }

    // Lowercase, keep [a-z0-9-] as word characters, split on the rest, trim
    // stray hyphens, then keep 2–15-letter non-stop-words.
    let lowered = cleaned.to_lowercase();
    let words: Vec<&str> = lowered
        .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'))
        .map(|w| w.trim_matches('-'))
        .filter(|w| is_valid_slug_word(w))
        .filter(|w| !STOP_WORDS.contains(w))
        .collect();

    if words.is_empty() {
        return None;
    }

    // Strategy 1: "action noun" → `noun-action` ("fix auth" → `auth-fix`).
    for pair in words.windows(2) {
        if ACTION_WORDS.contains(&pair[0]) && !ACTION_WORDS.contains(&pair[1]) {
            return Some(format!("{}-{}", pair[1], pair[0]));
        }
    }

    // Strategy 2: "noun action" → `noun-action` ("auth refactor").
    for pair in words.windows(2) {
        if !ACTION_WORDS.contains(&pair[0]) && ACTION_WORDS.contains(&pair[1]) {
            return Some(format!("{}-{}", pair[0], pair[1]));
        }
    }

    // Strategy 3: first two meaningful words.
    if words.len() >= 2 {
        return Some(format!("{}-{}", words[0], words[1]));
    }

    // Strategy 4: single word → generic suffix.
    Some(format!("{}-task", words[0]))
}

/// Remove `@kind[...]` context mentions (TS regex
/// `@(context|file|folder|symbol|url|image|linear|sentry|github)\[[^\]]*\]`).
fn strip_context_mentions(prompt: &str) -> String {
    const KINDS: [&str; 9] = [
        "context", "file", "folder", "symbol", "url", "image", "linear", "sentry", "github",
    ];
    let mut out = String::with_capacity(prompt.len());
    let mut rest = prompt;
    'outer: while let Some(at) = rest.find('@') {
        let (before, tail) = rest.split_at(at);
        out.push_str(before);
        for kind in KINDS {
            let Some(after_kind) = tail[1..]
                .strip_prefix(kind)
                .or_else(|| lowercase_strip(&tail[1..], kind))
            else {
                continue;
            };
            if let Some(after_bracket) = after_kind.strip_prefix('[') {
                if let Some(close) = after_bracket.find(']') {
                    rest = &after_bracket[close + 1..];
                    continue 'outer;
                }
            }
        }
        out.push('@');
        rest = &tail[1..];
    }
    out.push_str(rest);
    out
}

/// Case-insensitive `strip_prefix` for ASCII mention kinds.
fn lowercase_strip<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
