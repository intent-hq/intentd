//! Port of the Claude Code ACP adapter's model-name resolution
//! (`resolveModelPreference` in `@agentclientprotocol/claude-agent-acp`
//! `dist/session-model.js`, mirrored at 0.95.0 — unchanged since 0.81.1; a
//! drift test fails when the adapter pin moves), used by the unknown-model guard
//! (intent-hq/intent#7121). The adapter applies a caller's model through
//! `session/set_config_option` and resolves it against its picker rows with
//! these tiers: exact / canonical id or display name, substring containment
//! (version- and context-lane compatible), then token scoring (`opusplan` →
//! `opus`, `claude` ignored). intentd's cached catalog rows are the picker
//! options and never carry the adapter's private `resolvedModel` metadata,
//! so the `resolvedModel` tier is omitted — the adapter's own fallback pass
//! over its option list works the same way.

/// One picker row: the option value (catalog `id`), display name and
/// description.
pub(crate) struct ModelOption<'a> {
    pub(crate) value: &'a str,
    pub(crate) display_name: &'a str,
    pub(crate) description: &'a str,
}

fn is_word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Byte offset where a trailing `-(\d+m)` suffix starts, if present.
///
/// Every index is guarded before use: an all-digit id such as `123m` walks
/// the digit scan down to offset 0, which must return `None`, never compute
/// `0 - 1` (intent-hq/intent#7121 review).
fn trailing_dash_hint(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (&last, rest) = b.split_last()?;
    if !last.eq_ignore_ascii_case(&b'm') {
        return None;
    }
    let digits = rest.iter().rev().take_while(|c| c.is_ascii_digit()).count();
    if digits == 0 {
        return None;
    }
    let dash = rest.len().checked_sub(digits + 1)?;
    (rest[dash] == b'-').then_some(dash)
}

/// `canonicalizeModelId`: trim, lowercase, `-1m` suffix → `[1m]`.
fn canonicalize(value: &str) -> String {
    let lower = value.trim().to_lowercase();
    match trailing_dash_hint(&lower) {
        Some(at) => format!("{}[{}]", &lower[..at], &lower[at + 1..]),
        None => lower,
    }
}

/// Trailing `[\d+m]` hint of an already-lowercased string.
fn trailing_bracket_hint(lower: &str) -> Option<&str> {
    let inner = lower.strip_suffix(']')?;
    let open = inner.rfind('[')?;
    let hint = &inner[open + 1..];
    let digits = hint.strip_suffix('m')?;
    (!digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())).then_some(hint)
}

/// `contextHintOf`.
fn context_hint_of(value: &str) -> Option<String> {
    trailing_bracket_hint(&canonicalize(value)).map(str::to_string)
}

/// `stripContextHints`: drop every `[\d+m]` and a trailing `-\d+m`.
fn strip_context_hints(value: &str) -> String {
    let b = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'[' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 && j + 1 < b.len() && b[j].eq_ignore_ascii_case(&b'm') && b[j + 1] == b']'
            {
                i = j + 2;
                continue;
            }
        }
        let ch = value[i..].chars().next().expect("char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }
    match trailing_dash_hint(&out) {
        Some(at) => out[..at].to_string(),
        None => out,
    }
}

/// `extractModelFamilyVersion`: first `\b(\d+)(?:[-.](\d+))?\b` → `a` / `a.b`.
fn family_version(value: &str) -> Option<String> {
    let s = strip_context_hints(value);
    let b = s.as_bytes();
    let digits_end = |mut k: usize| {
        while k < b.len() && b[k].is_ascii_digit() {
            k += 1;
        }
        k
    };
    for i in 0..b.len() {
        if !b[i].is_ascii_digit() || (i > 0 && is_word(b[i - 1])) {
            continue;
        }
        let j = digits_end(i);
        if j + 1 < b.len() && (b[j] == b'-' || b[j] == b'.') && b[j + 1].is_ascii_digit() {
            let k = digits_end(j + 1);
            if k == b.len() || !is_word(b[k]) {
                return Some(format!("{}.{}", &s[i..j], &s[j + 1..k]));
            }
        }
        if j == b.len() || !is_word(b[j]) {
            return Some(s[i..j].to_string());
        }
    }
    None
}

/// `modelVersionsCompatible`.
fn versions_compatible(preference: &str, candidate: &ModelOption<'_>) -> bool {
    let Some(preferred) = family_version(preference) else {
        return true;
    };
    family_version(candidate.value)
        .or_else(|| family_version(candidate.display_name))
        .or_else(|| family_version(candidate.description))
        .is_none_or(|v| v == preferred)
}

/// `tokenizeModelPreference`.
fn tokenize(preference: &str) -> (Vec<String>, Option<String>) {
    let lower = preference.trim().to_lowercase();
    let hint = trailing_bracket_hint(&lower).map(str::to_string);
    let spaced = match &hint {
        Some(h) => format!("{} {h} ", &lower[..lower.len() - h.len() - 2]),
        None => lower.clone(),
    };
    let tokens = spaced
        .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
        .filter(|t| !t.is_empty())
        .map(|t| match t {
            "opusplan" => "opus",
            "best" | "default" => "",
            other => other,
        })
        .filter(|t| !t.is_empty() && *t != "claude")
        .filter(|t| t.bytes().any(|b| b.is_ascii_lowercase()) || t.ends_with('m'))
        .map(str::to_string)
        .collect();
    (tokens, hint)
}

/// `scoreModelMatch`.
fn score(model: &ModelOption<'_>, tokens: &[String], hint: Option<&str>) -> u32 {
    let haystack = format!("{} {}", model.value, model.display_name).to_lowercase();
    let mut total = 0;
    let mut non_hint_matched = false;
    for token in tokens {
        if !haystack.contains(token.as_str()) {
            continue;
        }
        let is_hint = hint == Some(token.as_str());
        non_hint_matched |= !is_hint;
        total += if is_hint { 3 } else { 1 };
    }
    if hint.is_some() && !non_hint_matched {
        0
    } else {
        total
    }
}

/// `resolveModelPreference`: the index of the row the adapter would select
/// for `preference`, or `None` when it would reject the value.
pub(crate) fn resolve_model_preference(
    models: &[ModelOption<'_>],
    preference: &str,
) -> Option<usize> {
    let trimmed = preference.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_lowercase();
    let canonical = canonicalize(trimmed);
    if let Some(i) = models.iter().position(|m| {
        m.value == trimmed
            || canonicalize(m.value) == canonical
            || m.display_name.to_lowercase() == lower
    }) {
        return Some(i);
    }
    let preference_hint = context_hint_of(trimmed);
    if let Some(i) = models.iter().position(|m| {
        if !versions_compatible(trimmed, m) || context_hint_of(m.value) != preference_hint {
            return false;
        }
        let value = m.value.to_lowercase();
        value.contains(&lower)
            || m.display_name.to_lowercase().contains(&lower)
            || lower.contains(&value)
    }) {
        return Some(i);
    }
    let (tokens, hint) = tokenize(trimmed);
    if tokens.is_empty() {
        return None;
    }
    let mut best: Option<(usize, u32)> = None;
    for (i, m) in models.iter().enumerate() {
        if !versions_compatible(trimmed, m) {
            continue;
        }
        let s = score(m, &tokens, hint.as_deref());
        if s > 0 && best.is_none_or(|(_, b)| s > b) {
            best = Some((i, s));
        }
    }
    best.map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opt<'a>(value: &'a str, name: &'a str, description: &'a str) -> ModelOption<'a> {
        ModelOption {
            value,
            display_name: name,
            description,
        }
    }

    fn picker() -> Vec<ModelOption<'static>> {
        vec![
            opt("opus[1m]", "Opus", "Opus 4.8 with 1M context"),
            opt("claude-fable-5[1m]", "Fable", "Fable 5 · Most capable"),
            opt("sonnet", "Sonnet", "Sonnet 5 · Efficient for routine tasks"),
            opt("haiku", "Haiku", "Haiku 4.5 · Fastest for quick answers"),
        ]
    }

    fn resolved(models: &[ModelOption<'_>], pref: &str) -> Option<String> {
        resolve_model_preference(models, pref).map(|i| models[i].value.to_string())
    }

    /// The claude-agent-acp release whose `resolveModelPreference` this
    /// module mirrors.
    const MIRRORED_CLAUDE_AGENT_ACP_VERSION: &str = "0.95.0";

    /// Drift guard: the port mirrors one adapter release. When the
    /// claude-agent-acp pin moves, re-read the new adapter's
    /// `dist/session-model.js` `resolveModelPreference` (and its helpers),
    /// port any change here, re-run the cross-check against the adapter, then
    /// bump [`MIRRORED_CLAUDE_AGENT_ACP_VERSION`].
    #[test]
    fn port_tracks_the_pinned_adapter_version() {
        assert_eq!(
            intent_providers::CLAUDE_AGENT_ACP_VERSION,
            MIRRORED_CLAUDE_AGENT_ACP_VERSION,
            "the claude-agent-acp pin moved: re-check model_preference.rs against the new \
             adapter's resolveModelPreference before bumping MIRRORED_CLAUDE_AGENT_ACP_VERSION"
        );
    }

    // Expected values below were cross-checked by running the pinned
    // adapter's own `resolveModelPreference` (claude-agent-acp 0.81.1) on
    // the same inputs. In 0.95.0 that function and every helper it calls are
    // byte-identical, so the expectations still hold.
    #[test]
    fn helpers_match_the_adapter_regexes() {
        assert_eq!(canonicalize(" Opus-1M "), "opus[1m]");
        assert_eq!(canonicalize("sonnet"), "sonnet");
        assert_eq!(context_hint_of("claude-opus-4-6-1m").as_deref(), Some("1m"));
        assert_eq!(context_hint_of("opus[1m]").as_deref(), Some("1m"));
        assert_eq!(context_hint_of("opus"), None);
        assert_eq!(strip_context_hints("opus[1M] x-1m"), "opus x");
        assert_eq!(family_version("claude-sonnet-4-5").as_deref(), Some("4.5"));
        assert_eq!(
            family_version("claude-haiku-4-5-20251001").as_deref(),
            Some("4.5")
        );
        assert_eq!(family_version("sonnet4.5").as_deref(), Some("5"));
        assert_eq!(
            family_version("Opus 4.8 with 1M context").as_deref(),
            Some("4.8")
        );
        assert_eq!(family_version("opus[1m]"), None);
        let (tokens, hint) = tokenize("Claude-Opusplan[1m]");
        assert_eq!(tokens, ["opus", "1m"]);
        assert_eq!(hint.as_deref(), Some("1m"));
    }

    #[test]
    fn resolves_aliases_full_ids_and_family_names() {
        let aliases = [
            opt("opus[1m]", "Opus", ""),
            opt("sonnet", "Sonnet", ""),
            opt("haiku", "Haiku", ""),
        ];
        for (pref, want) in [
            ("sonnet", "sonnet"),
            ("Sonnet", "sonnet"),
            ("opus[1m]", "opus[1m]"),
            ("opus-1m", "opus[1m]"),
            ("opus", "opus[1m]"),
            ("opusplan", "opus[1m]"),
            ("claude-sonnet-4-5", "sonnet"),
            ("claude-opus-4-1", "opus[1m]"),
            ("claude-haiku-4-5-20251001", "haiku"),
            ("Claude-Opusplan[1m]", "opus[1m]"),
            // The adapter's token tier is fuzzy: a recognizable family token
            // resolves even with trailing noise.
            ("opusplan-xyz", "opus[1m]"),
        ] {
            assert_eq!(resolved(&aliases, pref).as_deref(), Some(want), "{pref}");
        }
        assert_eq!(
            resolved(&picker(), "claude-haiku-4-5").as_deref(),
            Some("haiku")
        );
        assert_eq!(
            resolved(&picker(), "fable").as_deref(),
            Some("claude-fable-5[1m]")
        );
    }

    /// Adversarial ids must resolve or reject, never panic
    /// (intent-hq/intent#7121 review: `123m` underflowed the suffix scan).
    #[test]
    fn adversarial_inputs_never_panic() {
        let aliases = [
            opt("opus[1m]", "Opus", ""),
            opt("sonnet", "Sonnet", ""),
            opt("haiku", "Haiku", ""),
        ];
        for pref in [
            "123m",
            "123M",
            "1m",
            "m",
            "M",
            "",
            "-",
            "-m",
            "-1m",
            "1-m",
            "12345",
            "[1m]",
            "[]",
            "[m]",
            "[1m",
            "1m]",
            "a[1m]]",
            "é",
            "é1m",
            "ü-1m",
            "日本語",
            "sonnet-日本",
            "4-5",
            "4.5",
            "4-",
            ".5",
            "1.2.3",
            "claude-",
            "opus[1m]-1m",
            "x-0m",
            "--1m",
        ] {
            let _ = resolve_model_preference(&aliases, pref);
            let _ = canonicalize(pref);
            let _ = context_hint_of(pref);
            let _ = strip_context_hints(pref);
            let _ = family_version(pref);
            let _ = tokenize(pref);
        }
        let long = "9".repeat(10_000) + "m";
        assert_eq!(resolve_model_preference(&aliases, &long), None);
        assert_eq!(resolve_model_preference(&aliases, "123m"), None);
        assert_eq!(resolve_model_preference(&aliases, "123M"), None);
        // The adapter's token tier matches a bare context hint (checked
        // against the pinned adapter): these resolve rather than reject.
        assert_eq!(resolved(&aliases, "1m").as_deref(), Some("opus[1m]"));
        assert_eq!(resolved(&aliases, "m").as_deref(), Some("opus[1m]"));
    }

    /// Deterministic property sweep: every string up to length 4 over an
    /// alphabet of the characters the parser branches on (digits, `m`, `-`,
    /// `.`, brackets, a letter, a multibyte char) resolves without panicking,
    /// both as a preference and as a catalog row.
    #[test]
    fn resolver_never_panics_on_short_strings() {
        let alphabet = ['1', '0', 'm', 'M', '-', '.', '[', ']', 'o', 'é'];
        let mut inputs = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..4 {
            let mut next = Vec::new();
            for prefix in &frontier {
                for c in alphabet {
                    let mut s = prefix.clone();
                    s.push(c);
                    next.push(s);
                }
            }
            inputs.extend(next.iter().cloned());
            frontier = next;
        }
        for input in &inputs {
            let rows = [opt(input, input, input), opt("sonnet", "Sonnet", "")];
            let _ = resolve_model_preference(&rows, input);
            let _ = resolve_model_preference(&rows, "sonnet");
        }
    }

    #[test]
    fn rejects_what_the_adapter_rejects() {
        let aliases = [
            opt("opus[1m]", "Opus", ""),
            opt("sonnet", "Sonnet", ""),
            opt("haiku", "Haiku", ""),
        ];
        for pref in [
            "no-such-model-xyz",
            "gpt-5",
            "",
            "  ",
            "default",
            "claude",
            "4-5",
            "legacy-model-q",
            "profile-only-model",
        ] {
            assert_eq!(resolved(&aliases, pref), None, "{pref}");
        }
        // Version metadata disqualifies a different generation.
        assert_eq!(resolved(&picker(), "claude-sonnet-4-5"), None);
    }
}
