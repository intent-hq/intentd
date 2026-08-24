//! Wire-policy glue for the `voice.transcribe` method.
//!
//! The engine + HTTP transports live in `intent-voice`; this module owns only
//! the parity-critical wire glue so it stays unit-testable without a network:
//! parsing/validating the wire request (base64 audio, size cap, provider
//! override, context), selecting the provider (per-call override else the
//! `voice.provider` setting), resolving the active [`VoiceEngine`] (injected
//! handle else the registry-built engine from the secrets store / env), and
//! mapping engine errors onto domain errors. It mirrors
//! `linear_ops::resolve_engine` / `sentry_ops`.
//!
//! GUARDRAIL: the provider API keys are secrets. They never appear here — the
//! engine reads them internally and only the transcript crosses the wire.

use std::sync::Arc;

use base64::Engine as _;
use intent_core::{Error, Result};
use intent_voice::{
    context, TranscribeRequest, VoiceEngine, VoiceProvider, VoiceRegistry, VoiceSettings,
};

/// Decoded-audio size cap (~25 MB): the provider upload limit; anything
/// larger is rejected with `InvalidParams` before touching the network.
pub(crate) const MAX_AUDIO_BYTES: usize = 25 * 1024 * 1024;

/// Default MIME type when the wire request omits `mimeType` (the FE recorder
/// produces webm/opus).
const DEFAULT_MIME_TYPE: &str = "audio/webm";

/// Default value of the `voice.vocabulary` setting: a minimal seed biased
/// into every transcription until the user edits the list. Users add their
/// own terms; the shipped default carries only the product name.
pub(crate) const DEFAULT_VOCABULARY: &[&str] = &["Intent"];

/// The retired original 17-term seed default. Referenced only by the boot
/// migration ([`crate::settings::migrate_default_vocabulary`]): a stored
/// `voice.vocabulary` row that exactly matches this list (order-sensitive)
/// is treated as an untouched default and deleted so [`DEFAULT_VOCABULARY`]
/// applies.
pub(crate) const LEGACY_DEFAULT_VOCABULARY: &[&str] = &[
    "intentd",
    "Cloudlands",
    "workspace",
    "agent",
    "spec",
    "PR",
    "CI",
    "clippy",
    "Svelte",
    "TypeScript",
    "submodule",
    "monorepo",
    "JSON-RPC",
    "SQLite",
    "Rust",
    "cargo",
    "WebSocket",
];

/// A parsed `voice.transcribe` wire request.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParsedRequest {
    pub audio: Vec<u8>,
    pub mime_type: String,
    pub language: Option<String>,
    pub provider_override: Option<VoiceProvider>,
    pub prompt: Option<String>,
    pub keyterms: Vec<String>,
    /// Opt-in workspace-vocabulary injection (PROTOCOL §5.41, v4.6).
    /// Tolerant by design: absent/blank (and explicit `null`, like
    /// `context.keyterms`) parse to `None`; an unknown or stale id is the
    /// caller's concern (never an error there either); only a non-string
    /// value rejects with `InvalidParams`.
    pub workspace_id: Option<String>,
}

/// Map a voice engine/registry error onto a domain error (→ `-32603`): a
/// missing key (`NotConfigured` — exclusively the registry's no-key case;
/// provider failures such as `OpenAI` model-unavailable use distinct variants)
/// surfaces as `VoiceNotConfigured` so the wire carries
/// `error.data.code = "voice-no-api-key"` (monorepo#1448); any other
/// provider failure surfaces as `Internal` with a descriptive message (§9).
/// The descriptive text is identical in both shapes.
pub(crate) fn map_voice_err(e: intent_voice::Error) -> Error {
    match e {
        intent_voice::Error::NotConfigured(_) => Error::VoiceNotConfigured {
            detail: e.to_string(),
        },
        other => Error::Internal(other.to_string()),
    }
}

/// Parse/validate the wire params: `audio` (required, base64), optional
/// `mimeType`, `language`, `provider`, and `context { prompt?, keyterms? }`.
/// Violations reject with `InvalidParams` (→ `-32602`).
pub(crate) fn parse_request(params: &serde_json::Value) -> Result<ParsedRequest> {
    let audio_b64 = params
        .get("audio")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            Error::InvalidParams("Missing required parameter: audio (base64)".to_string())
        })?;
    // Reject oversized payloads before decoding: base64 expands ~4/3, so a
    // decoded cap of N is exceeded whenever the encoded text tops N*4/3.
    if audio_b64.len() > MAX_AUDIO_BYTES / 3 * 4 + 4 {
        return Err(Error::InvalidParams(format!(
            "audio exceeds the {} MB limit",
            MAX_AUDIO_BYTES / (1024 * 1024)
        )));
    }
    let audio = base64::engine::general_purpose::STANDARD
        .decode(audio_b64.trim())
        .map_err(|e| Error::InvalidParams(format!("audio is not valid base64: {e}")))?;
    if audio.is_empty() {
        return Err(Error::InvalidParams(
            "audio decoded to zero bytes".to_string(),
        ));
    }
    if audio.len() > MAX_AUDIO_BYTES {
        return Err(Error::InvalidParams(format!(
            "audio exceeds the {} MB limit",
            MAX_AUDIO_BYTES / (1024 * 1024)
        )));
    }

    let mime_type = params
        .get("mimeType")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(DEFAULT_MIME_TYPE)
        .to_string();
    let language = params
        .get("language")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    let provider_override = match params.get("provider").and_then(|v| v.as_str()) {
        None => None,
        Some(s) => Some(VoiceProvider::parse(s).ok_or_else(|| {
            Error::InvalidParams("provider must be one of: elevenlabs, openai".to_string())
        })?),
    };

    let workspace_id = match params.get("workspaceId") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(s)) => {
            let trimmed = s.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Some(_) => {
            return Err(Error::InvalidParams(
                "workspaceId must be a string".to_string(),
            ))
        }
    };

    let ctx = params.get("context");
    let prompt = ctx
        .and_then(|c| c.get("prompt"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    let keyterms = match ctx.and_then(|c| c.get("keyterms")) {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|v| {
                v.as_str().map(str::to_string).ok_or_else(|| {
                    Error::InvalidParams("context.keyterms must be an array of strings".to_string())
                })
            })
            .collect::<Result<Vec<String>>>()?,
        Some(_) => {
            return Err(Error::InvalidParams(
                "context.keyterms must be an array of strings".to_string(),
            ))
        }
    };

    Ok(ParsedRequest {
        audio,
        mime_type,
        language,
        provider_override,
        prompt,
        keyterms,
        workspace_id,
    })
}

/// Select the active provider: the per-call override wins, else the
/// `voice.provider` setting value, else the default (`ElevenLabs`). An invalid
/// stored setting value falls back to the default rather than erroring.
pub(crate) fn select_provider(
    setting_value: Option<&str>,
    override_provider: Option<VoiceProvider>,
) -> VoiceProvider {
    if let Some(p) = override_provider {
        return p;
    }
    setting_value
        .and_then(VoiceProvider::parse)
        .unwrap_or_default()
}

/// Resolve the transcription language: the per-call `language` wins, else
/// the `voice.language` setting, else `None` (provider auto-detection). Both
/// inputs are trimmed and blank degrades to unset.
pub(crate) fn resolve_language(
    per_call: Option<&str>,
    setting_value: Option<&str>,
) -> Option<String> {
    per_call
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            setting_value
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
}

/// Parse the stored `voice.vocabulary` setting value into a term list: a JSON
/// array yields its string elements; anything else (absent, `null`, wrong
/// type, non-string elements) degrades to an empty list — never an error.
pub(crate) fn parse_vocabulary_setting(value: Option<&serde_json::Value>) -> Vec<String> {
    match value.and_then(|v| v.as_array()) {
        Some(items) => items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        None => Vec::new(),
    }
}

/// Build the provider [`TranscribeRequest`]: merge the configured
/// `voice.vocabulary` terms, the auto-derived workspace vocabulary (empty
/// when the call carries no `workspaceId` — PROTOCOL §5.41, v4.6), and the
/// request keyterms — in that fixed order, under the existing dedup/cap
/// rules — and compose the `OpenAI` prompt from the merged terms verbatim
/// (see [`intent_voice::context`]). The `keyterms` field alone is then
/// sanitized to the `ElevenLabs` Scribe v2 rules
/// ([`context::sanitize_keyterms`]) — only `ElevenLabs` consumes it, and
/// `OpenAI` biasing must keep unsanitized spellings. Both fields are always
/// populated; each engine consumes the one it supports. `language` is the
/// resolved value from [`resolve_language`] (per-call > `voice.language`
/// setting > auto-detect).
pub(crate) fn build_engine_request(
    parsed: &ParsedRequest,
    vocabulary: &[String],
    workspace_terms: &[String],
    language: Option<String>,
) -> TranscribeRequest {
    // user voice.vocabulary → workspace auto-terms, ahead of the request
    // keyterms; merge_keyterms dedups case-insensitively with first spelling
    // winning, so earlier tiers keep priority.
    let mut biased: Vec<String> = Vec::with_capacity(vocabulary.len() + workspace_terms.len());
    biased.extend_from_slice(vocabulary);
    biased.extend_from_slice(workspace_terms);
    let merged = context::merge_keyterms(&biased, &parsed.keyterms);
    let prompt = context::compose_prompt(&merged, parsed.prompt.as_deref());
    let keyterms = context::sanitize_keyterms(&merged);
    TranscribeRequest {
        audio: parsed.audio.clone(),
        mime_type: parsed.mime_type.clone(),
        language,
        keyterms,
        prompt: Some(prompt),
    }
}

/// Resolve the active [`VoiceEngine`] for `provider`: the injected handle
/// (tests / explicit wiring) else the registry-built engine (key from the
/// secrets store / `ELEVENLABS_API_KEY` / `OPENAI_API_KEY`). A missing key
/// yields `VoiceNotConfigured` (graceful "not configured"), never a panic. Async
/// because the secrets lookup runs on the blocking pool with a bounded
/// timeout so a wedged backing store never blocks the async runtime.
pub(crate) async fn resolve_engine(
    injected: Option<Arc<dyn VoiceEngine>>,
    provider: VoiceProvider,
    openai_model: Option<String>,
) -> Result<Arc<dyn VoiceEngine>> {
    match injected {
        Some(engine) => Ok(engine),
        None => VoiceRegistry::from_settings(&VoiceSettings {
            provider,
            openai_model,
            ..VoiceSettings::default()
        })
        .await
        .map_err(map_voice_err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn parses_minimal_request_with_defaults() {
        let parsed = parse_request(&json!({ "audio": b64(b"hi") })).unwrap();
        assert_eq!(parsed.audio, b"hi");
        assert_eq!(parsed.mime_type, DEFAULT_MIME_TYPE);
        assert_eq!(parsed.language, None);
        assert_eq!(parsed.provider_override, None);
        assert_eq!(parsed.prompt, None);
        assert!(parsed.keyterms.is_empty());
    }

    #[test]
    fn parses_full_request() {
        let parsed = parse_request(&json!({
            "audio": b64(b"audio-bytes"),
            "mimeType": "audio/wav",
            "language": "en",
            "provider": "openai",
            "context": { "prompt": "Discussing CI.", "keyterms": ["Endara"] },
        }))
        .unwrap();
        assert_eq!(parsed.mime_type, "audio/wav");
        assert_eq!(parsed.language.as_deref(), Some("en"));
        assert_eq!(parsed.provider_override, Some(VoiceProvider::OpenAi));
        assert_eq!(parsed.prompt.as_deref(), Some("Discussing CI."));
        assert_eq!(parsed.keyterms, vec!["Endara".to_string()]);
    }

    #[test]
    fn missing_audio_rejects() {
        for params in [json!({}), json!({ "audio": "" }), json!({ "audio": "   " })] {
            let err = parse_request(&params).unwrap_err();
            assert!(matches!(err, Error::InvalidParams(m) if m.contains("audio")));
        }
    }

    #[test]
    fn invalid_base64_rejects() {
        let err = parse_request(&json!({ "audio": "!!not-base64!!" })).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("base64")));
    }

    #[test]
    fn oversized_audio_rejects_before_decoding() {
        // Encoded length > cap*4/3 without allocating a 25MB decode.
        let big = "A".repeat(MAX_AUDIO_BYTES / 3 * 4 + 8);
        let err = parse_request(&json!({ "audio": big })).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("limit")));
    }

    #[test]
    fn invalid_provider_rejects() {
        let err = parse_request(&json!({ "audio": b64(b"x"), "provider": "whisper" })).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("provider")));
    }

    #[test]
    fn non_string_keyterms_reject() {
        let err = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "keyterms": [1, 2] },
        }))
        .unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("keyterms")));
        let err = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "keyterms": "intentd" },
        }))
        .unwrap_err();
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("keyterms")));
    }

    #[test]
    fn selects_provider_override_then_setting_then_default() {
        assert_eq!(
            select_provider(None, Some(VoiceProvider::OpenAi)),
            VoiceProvider::OpenAi
        );
        assert_eq!(select_provider(Some("openai"), None), VoiceProvider::OpenAi);
        assert_eq!(
            select_provider(Some("elevenlabs"), Some(VoiceProvider::OpenAi)),
            VoiceProvider::OpenAi,
            "per-call override wins over the setting"
        );
        assert_eq!(select_provider(None, None), VoiceProvider::ElevenLabs);
        assert_eq!(
            select_provider(Some("bogus"), None),
            VoiceProvider::ElevenLabs,
            "invalid stored value falls back to the default"
        );
    }

    #[test]
    fn engine_request_merges_context() {
        let parsed = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "prompt": "Release planning.", "keyterms": ["Endara"] },
        }))
        .unwrap();
        let vocabulary = vec!["intentd".to_string(), "clippy".to_string()];
        let req = build_engine_request(&parsed, &vocabulary, &[], parsed.language.clone());
        assert!(
            req.keyterms.contains(&"intentd".to_string()),
            "configured vocabulary merged in"
        );
        assert!(
            req.keyterms.contains(&"Endara".to_string()),
            "request terms"
        );
        let prompt = req.prompt.unwrap();
        assert!(prompt.contains("Vocabulary:"));
        assert!(prompt.ends_with("Release planning."));
    }

    #[test]
    fn engine_request_honors_custom_vocabulary() {
        let parsed = parse_request(&json!({ "audio": b64(b"x") })).unwrap();
        let vocabulary = vec!["Endara".to_string(), "TOON".to_string()];
        let req = build_engine_request(&parsed, &vocabulary, &[], parsed.language.clone());
        assert_eq!(req.keyterms, vec!["Endara".to_string(), "TOON".to_string()]);
        assert!(
            !req.keyterms.contains(&"intentd".to_string()),
            "no hardcoded base vocabulary"
        );
    }

    #[test]
    fn engine_request_with_empty_vocabulary_sends_only_request_keyterms() {
        let parsed = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "keyterms": ["Endara"] },
        }))
        .unwrap();
        let req = build_engine_request(&parsed, &[], &[], parsed.language.clone());
        assert_eq!(req.keyterms, vec!["Endara".to_string()]);
    }

    #[test]
    fn engine_request_sanitizes_keyterms_but_keeps_prompt_unsanitized() {
        let parsed = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "keyterms": ["[fix] task"] },
        }))
        .unwrap();
        let vocabulary = vec!["C:\\src".to_string()];
        let req = build_engine_request(&parsed, &vocabulary, &[], parsed.language.clone());
        assert_eq!(
            req.keyterms,
            vec!["C:src".to_string(), "fix task".to_string()],
            "keyterms carry the ElevenLabs-sanitized spellings"
        );
        let prompt = req.prompt.unwrap();
        assert!(
            prompt.contains("C:\\src") && prompt.contains("[fix] task"),
            "OpenAI prompt keeps unsanitized spellings: {prompt}"
        );
    }

    #[test]
    fn parses_workspace_id_tolerantly() {
        let parsed = parse_request(&json!({ "audio": b64(b"x") })).unwrap();
        assert_eq!(parsed.workspace_id, None, "absent → None");
        let parsed = parse_request(&json!({ "audio": b64(b"x"), "workspaceId": null })).unwrap();
        assert_eq!(parsed.workspace_id, None, "explicit null → None");
        let parsed = parse_request(&json!({ "audio": b64(b"x"), "workspaceId": "  " })).unwrap();
        assert_eq!(parsed.workspace_id, None, "blank → None");
        let parsed =
            parse_request(&json!({ "audio": b64(b"x"), "workspaceId": "ws-abc" })).unwrap();
        assert_eq!(parsed.workspace_id.as_deref(), Some("ws-abc"));
    }

    #[test]
    fn non_string_workspace_id_rejects() {
        for bad in [json!(42), json!(true), json!(["ws-abc"]), json!({})] {
            let err =
                parse_request(&json!({ "audio": b64(b"x"), "workspaceId": bad })).unwrap_err();
            assert!(
                matches!(err, Error::InvalidParams(ref m) if m.contains("workspaceId")),
                "expected InvalidParams naming workspaceId"
            );
        }
    }

    #[test]
    fn engine_request_merges_workspace_terms_between_vocab_and_request() {
        let parsed = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "keyterms": ["Endara"] },
        }))
        .unwrap();
        let vocabulary = vec!["Intent".to_string()];
        let workspace_terms = vec!["intentd".to_string(), "clippy".to_string()];
        let req = build_engine_request(
            &parsed,
            &vocabulary,
            &workspace_terms,
            parsed.language.clone(),
        );
        assert_eq!(
            req.keyterms,
            vec![
                "Intent".to_string(),
                "intentd".to_string(),
                "clippy".to_string(),
                "Endara".to_string(),
            ],
            "fixed merge order: user vocabulary → workspace auto-terms → request keyterms"
        );
    }

    #[test]
    fn workspace_terms_dedup_first_spelling_wins() {
        let parsed = parse_request(&json!({
            "audio": b64(b"x"),
            "context": { "keyterms": ["INTENTD"] },
        }))
        .unwrap();
        let vocabulary = vec!["Clippy".to_string()];
        let workspace_terms = vec!["intentd".to_string(), "clippy".to_string()];
        let req = build_engine_request(
            &parsed,
            &vocabulary,
            &workspace_terms,
            parsed.language.clone(),
        );
        assert_eq!(
            req.keyterms,
            vec!["Clippy".to_string(), "intentd".to_string()],
            "case-insensitive dedup, earlier tier's spelling wins"
        );
    }

    #[test]
    fn resolves_language_per_call_then_setting_then_none() {
        assert_eq!(
            resolve_language(Some("en"), Some("de")),
            Some("en".to_string()),
            "per-call language wins over the setting"
        );
        assert_eq!(resolve_language(None, Some("de")), Some("de".to_string()));
        assert_eq!(
            resolve_language(None, Some("  fr  ")),
            Some("fr".to_string()),
            "stored value is trimmed"
        );
        assert_eq!(
            resolve_language(Some("  fr  "), None),
            Some("fr".to_string()),
            "per-call value is trimmed"
        );
        assert_eq!(
            resolve_language(Some("   "), Some("de")),
            Some("de".to_string()),
            "blank per-call value falls back to the setting"
        );
        assert_eq!(resolve_language(None, Some("")), None, "blank means unset");
        assert_eq!(resolve_language(None, Some("   ")), None);
        assert_eq!(resolve_language(None, None), None, "auto-detect");
    }

    #[test]
    fn default_vocabulary_is_the_minimal_intent_seed() {
        assert_eq!(DEFAULT_VOCABULARY, &["Intent"]);
    }

    #[test]
    fn legacy_default_vocabulary_matches_the_original_terms() {
        assert_eq!(
            LEGACY_DEFAULT_VOCABULARY,
            &[
                "intentd",
                "Cloudlands",
                "workspace",
                "agent",
                "spec",
                "PR",
                "CI",
                "clippy",
                "Svelte",
                "TypeScript",
                "submodule",
                "monorepo",
                "JSON-RPC",
                "SQLite",
                "Rust",
                "cargo",
                "WebSocket",
            ]
        );
        assert_eq!(LEGACY_DEFAULT_VOCABULARY.len(), 17);
    }

    #[test]
    fn parses_vocabulary_setting_array() {
        let value = json!(["Endara", "TOON"]);
        assert_eq!(
            parse_vocabulary_setting(Some(&value)),
            vec!["Endara".to_string(), "TOON".to_string()]
        );
    }

    #[test]
    fn malformed_vocabulary_setting_degrades_to_empty() {
        for value in [
            json!(null),
            json!("intentd"),
            json!(42),
            json!({ "terms": ["intentd"] }),
        ] {
            assert!(
                parse_vocabulary_setting(Some(&value)).is_empty(),
                "expected empty for {value}"
            );
        }
        assert!(parse_vocabulary_setting(None).is_empty());
        // Non-string elements are skipped, string elements survive.
        let mixed = json!(["Endara", 42, null]);
        assert_eq!(
            parse_vocabulary_setting(Some(&mixed)),
            vec!["Endara".to_string()]
        );
    }

    #[test]
    fn maps_not_configured_to_voice_not_configured() {
        let mapped = map_voice_err(intent_voice::Error::NotConfigured("no key".into()));
        match mapped {
            Error::VoiceNotConfigured { detail } => {
                assert_eq!(
                    detail, "voice not configured: no key",
                    "detail carries the descriptive text unchanged"
                );
            }
            other => panic!("expected VoiceNotConfigured, got {other:?}"),
        }
    }

    #[test]
    fn maps_other_voice_errors_to_internal() {
        for e in [
            intent_voice::Error::Auth("401 Unauthorized".into()),
            intent_voice::Error::Api("500: Internal Server Error".into()),
            intent_voice::Error::ModelUnavailable("openai returned 404: model not found".into()),
        ] {
            let expected = e.to_string();
            let mapped = map_voice_err(e);
            assert!(
                matches!(mapped, Error::Internal(m) if m == expected),
                "non-NotConfigured errors keep mapping to Internal"
            );
        }
    }
}
