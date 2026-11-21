//! Wire glue for the `input.autocomplete` method (composer ghost text).
//!
//! The daemon asks an OpenAI-compatible gateway (Kilo Gateway or `OpenRouter`)
//! for a short continuation of the user's composer draft. Everything here is
//! pure and unit-testable except [`request_completion`], which performs the
//! one upstream HTTP call:
//!
//! - [`parse_params`] validates `{ text, context }` (→ `-32602` on bad input),
//! - [`resolve_model_id`] applies the service × key model-id table,
//! - [`build_body`] renders the chat-completions request,
//! - [`clean_reply`] turns the raw model reply into the text to append.
//!
//! Every upstream or configuration failure is soft: the method answers
//! `{ completion: "" }` and logs at debug level only. The API keys are
//! secrets: they go into the `Authorization` header and nowhere else (never
//! logged, never returned). The draft and conversation text are not logged
//! either.

use std::time::Duration;

use intent_core::settings_file::{AutocompleteModel, AutocompleteService};
use intent_core::{Error, Result};
use serde_json::{json, Value};

/// Most characters (Unicode scalar values) accepted in `text`.
pub(crate) const MAX_TEXT_CHARS: usize = 2000;
/// Most characters (Unicode scalar values) accepted in `context`.
pub(crate) const MAX_CONTEXT_CHARS: usize = 4000;
/// Most characters returned in `completion`.
pub(crate) const MAX_COMPLETION_CHARS: usize = 200;
/// Upstream response bodies larger than this are treated as a failure.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Kilo Gateway chat-completions endpoint.
pub(crate) const KILO_URL: &str = "https://api.kilo.ai/api/gateway/chat/completions";
/// `OpenRouter` chat-completions endpoint.
pub(crate) const OPENROUTER_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
/// Whole-request timeout for one suggestion.
pub(crate) const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(4);

/// Secrets-store account (= sensitive setting path) of the Kilo key.
pub(crate) const KILO_KEY_PATH: &str = "autocomplete.kilo.apiKey";
/// Secrets-store account (= sensitive setting path) of the `OpenRouter` key.
pub(crate) const OPENROUTER_KEY_PATH: &str = "autocomplete.openrouter.apiKey";

/// System prompt sent with every suggestion request.
pub(crate) const SYSTEM_PROMPT: &str = "You autocomplete the user's next chat message in a coding assistant app. You see the recent conversation (with suggested prompts) and the text the user has typed so far. Reply with ONLY the text that continues the user's message, without repeating what they typed. If the typed text ends in the middle of a word, start with the rest of that word. If it ends with a complete word, start your reply with a single space. Finish at most the current sentence (under 20 words). If unsure, reply with nothing.";

/// Upstream endpoints and timeout. Production uses [`Default`]; tests point
/// both URLs at a local mock server and shorten the timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutocompleteUpstream {
    /// Kilo Gateway chat-completions URL.
    pub kilo_url: String,
    /// `OpenRouter` chat-completions URL.
    pub openrouter_url: String,
    /// Whole-request timeout.
    pub timeout: Duration,
}

impl Default for AutocompleteUpstream {
    fn default() -> Self {
        Self {
            kilo_url: KILO_URL.to_string(),
            openrouter_url: OPENROUTER_URL.to_string(),
            timeout: UPSTREAM_TIMEOUT,
        }
    }
}

/// Validated `input.autocomplete` params.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Params {
    pub text: String,
    pub context: String,
}

/// Validate `{ text, context }`: both required strings, `text` 1..=2000
/// chars, `context` 0..=4000 chars. Anything else → `InvalidParams`.
pub(crate) fn parse_params(params: &Value) -> Result<Params> {
    let text = required_string(params, "text")?;
    let context = required_string(params, "context")?;
    let text_len = text.chars().count();
    if text_len == 0 || text_len > MAX_TEXT_CHARS {
        return Err(Error::InvalidParams(format!(
            "text must be 1 to {MAX_TEXT_CHARS} characters, got {text_len}"
        )));
    }
    let context_len = context.chars().count();
    if context_len > MAX_CONTEXT_CHARS {
        return Err(Error::InvalidParams(format!(
            "context must be at most {MAX_CONTEXT_CHARS} characters, got {context_len}"
        )));
    }
    Ok(Params { text, context })
}

fn required_string(params: &Value, name: &str) -> Result<String> {
    match params.get(name) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(_) => Err(Error::InvalidParams(format!("{name} must be a string"))),
        None => Err(Error::InvalidParams(format!(
            "Missing required parameter: {name}"
        ))),
    }
}

/// Parse the stored `autocomplete.service` value; unknown → the default.
pub(crate) fn parse_service(value: Option<&str>) -> AutocompleteService {
    match value {
        Some("openrouter") => AutocompleteService::Openrouter,
        _ => AutocompleteService::Kilo,
    }
}

/// Parse the stored `autocomplete.model` value; unknown → the default.
pub(crate) fn parse_model(value: Option<&str>) -> AutocompleteModel {
    match value {
        Some("particle-ai/particle-1") => AutocompleteModel::Particle1,
        _ => AutocompleteModel::Nemotron35Lightning,
    }
}

/// Base model id of `model` (shared by both services).
pub(crate) fn base_model_id(model: AutocompleteModel) -> &'static str {
    match model {
        AutocompleteModel::Nemotron35Lightning => "nvidia/nemotron-3.5-lightning",
        AutocompleteModel::Particle1 => "particle-ai/particle-1",
    }
}

/// Secrets-store account holding the key for `service`.
pub(crate) fn key_path(service: AutocompleteService) -> &'static str {
    match service {
        AutocompleteService::Kilo => KILO_KEY_PATH,
        AutocompleteService::Openrouter => OPENROUTER_KEY_PATH,
    }
}

/// The model id sent upstream, or `None` when no request should be made:
///
/// | Service | Key saved | Model ID |
/// |---|---|---|
/// | kilo | no | `<model>:free` |
/// | kilo | yes | `<model>` |
/// | openrouter | yes | `<model>` |
/// | openrouter | no | no request |
pub(crate) fn resolve_model_id(
    service: AutocompleteService,
    model: AutocompleteModel,
    has_key: bool,
) -> Option<String> {
    let base = base_model_id(model);
    match (service, has_key) {
        (AutocompleteService::Kilo, false) => Some(format!("{base}:free")),
        (AutocompleteService::Kilo | AutocompleteService::Openrouter, true) => {
            Some(base.to_string())
        }
        (AutocompleteService::Openrouter, false) => None,
    }
}

/// The chat-completions request body.
pub(crate) fn build_body(model_id: &str, params: &Params) -> Value {
    json!({
        "model": model_id,
        "max_tokens": 40,
        "temperature": 0.2,
        "stop": ["\n"],
        "reasoning": { "enabled": false },
        "messages": [
            { "role": "system", "content": SYSTEM_PROMPT },
            {
                "role": "user",
                "content": format!(
                    "<conversation>\n{}\n</conversation>\n<typed>{}</typed>",
                    params.context, params.text
                ),
            },
        ],
    })
}

/// Extract `choices[0].message.content` from a chat-completions response.
/// A missing or non-string content (e.g. `null` from a reasoning-only
/// reply) yields `None`.
pub(crate) fn extract_content(response: &Value) -> Option<&str> {
    response
        .get("choices")?
        .get(0)?
        .get("message")?
        .get("content")?
        .as_str()
}

/// Turn a raw model reply into the text to append after `text`:
///
/// 1. take the first line,
/// 2. strip wrapping quotes/backticks and drop a repeated copy of `text` at
///    the start (case-insensitive),
/// 3. cut after the first sentence end (`.`, `?` or `!` followed by
///    whitespace or the end of the reply),
/// 4. cap at [`MAX_COMPLETION_CHARS`] characters,
/// 5. fix the spacing against the draft (see [`fix_leading_space`]),
/// 6. drop junk: `""` when the reply holds a control character, or `<` / `>`
///    while neither `context` nor `text` has any.
pub(crate) fn clean_reply(text: &str, context: &str, reply: &str) -> String {
    let first_line = reply.split(['\n', '\r']).next().unwrap_or_default();
    let unquoted = strip_wrapping_quotes(first_line);
    let without_echo = strip_echo(text, unquoted);
    let unquoted = strip_wrapping_quotes(without_echo);
    let sentence = cut_after_sentence(unquoted);
    let capped: String = sentence.chars().take(MAX_COMPLETION_CHARS).collect();
    let trimmed = capped.trim_end();
    if trimmed.trim_start().is_empty() || is_junk(text, context, trimmed.trim_start()) {
        return String::new();
    }
    fix_leading_space(text, trimmed)
}

/// Spacing between the draft and the suggestion (models usually omit the
/// leading space, which glued words together):
///
/// - `text` ends with whitespace → drop the reply's leading whitespace;
/// - else the reply starts with whitespace → exactly one leading space;
/// - else → as the model gave it (it continues the current word).
fn fix_leading_space(text: &str, reply: &str) -> String {
    let rest = reply.trim_start();
    if text.ends_with(char::is_whitespace) {
        rest.to_string()
    } else if rest.len() == reply.len() {
        reply.to_string()
    } else {
        format!(" {rest}")
    }
}

/// A reply is junk when it holds a control character, or angle brackets
/// while neither the conversation nor the draft has any (Nemotron once answered a
/// mid-word draft with `re>n>n>e>r>s>o>n>s>hots>`).
fn is_junk(text: &str, context: &str, reply: &str) -> bool {
    if reply.chars().any(char::is_control) {
        return true;
    }
    let angle = |s: &str| s.contains(['<', '>']);
    angle(reply) && !angle(context) && !angle(text)
}

/// Strip matching wrapping quote/backtick pairs (repeatedly, so a reply in
/// a code span or nested quotes unwraps fully). Whitespace around the pair
/// is ignored when looking for it; an unwrapped reply is returned unchanged.
fn strip_wrapping_quotes(s: &str) -> &str {
    const PAIRS: &[(char, char)] = &[
        ('"', '"'),
        ('\'', '\''),
        ('`', '`'),
        ('\u{201c}', '\u{201d}'),
        ('\u{2018}', '\u{2019}'),
    ];
    let mut current = s;
    loop {
        let trimmed = current.trim();
        let mut chars = trimmed.chars();
        let (Some(first), Some(last)) = (chars.next(), chars.next_back()) else {
            return current;
        };
        let Some(_) = PAIRS
            .iter()
            .find(|(open, close)| *open == first && *close == last)
        else {
            return current;
        };
        current = &trimmed[first.len_utf8()..trimmed.len() - last.len_utf8()];
    }
}

/// Drop a repeated copy of `text` at the start of `reply`, compared
/// case-insensitively. A copy of `text` without its trailing whitespace also
/// counts; then the whitespace the model put back is dropped too, since the
/// draft already ends with it.
fn strip_echo<'a>(text: &str, reply: &'a str) -> &'a str {
    if text.is_empty() {
        return reply;
    }
    if let Some(rest) = strip_prefix_ci(reply, text) {
        return rest;
    }
    let trimmed = text.trim_end();
    if !trimmed.is_empty() && trimmed.len() != text.len() {
        if let Some(rest) = strip_prefix_ci(reply, trimmed) {
            return rest.trim_start();
        }
    }
    reply
}

/// `reply` without its leading `prefix`, compared char by char in lower
/// case; `None` when `reply` does not start with `prefix`.
fn strip_prefix_ci<'a>(reply: &'a str, prefix: &str) -> Option<&'a str> {
    let mut reply_chars = reply.char_indices();
    for p in prefix.chars() {
        let (_, r) = reply_chars.next()?;
        if r != p && !r.to_lowercase().eq(p.to_lowercase()) {
            return None;
        }
    }
    let offset = reply_chars.next().map_or(reply.len(), |(i, _)| i);
    Some(&reply[offset..])
}

/// Cut after the first sentence end: `.`, `?` or `!` followed by whitespace
/// or the end of the reply (so `file.rs` or `v1.2` do not end a sentence).
fn cut_after_sentence(s: &str) -> &str {
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if matches!(c, '.' | '?' | '!') {
            match chars.peek() {
                None => return s,
                Some((_, next)) if next.is_whitespace() => return &s[..i + c.len_utf8()],
                Some(_) => {}
            }
        }
    }
    s
}

/// One resolved upstream call.
#[derive(Debug, Clone)]
pub(crate) struct UpstreamRequest<'a> {
    pub url: &'a str,
    pub service: AutocompleteService,
    pub api_key: Option<&'a str>,
    pub body: Value,
    pub timeout: Duration,
}

/// Shared HTTP client (connection reuse keeps per-keystroke latency low).
pub(crate) fn http_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// POST the request and return the raw reply content, or `None` on any
/// failure (timeout, network, non-2xx such as 404/429, oversized body, parse
/// error). Failures are logged at debug level without the key or text.
pub(crate) async fn request_completion(
    client: &reqwest::Client,
    request: UpstreamRequest<'_>,
) -> Option<String> {
    let mut builder = client
        .post(request.url)
        .timeout(request.timeout)
        .json(&request.body);
    if let Some(key) = request.api_key {
        builder = builder.bearer_auth(key);
    }
    if request.service == AutocompleteService::Openrouter {
        builder = builder
            .header("HTTP-Referer", "https://intentapp.dev")
            .header("X-Title", "Intent");
    }
    let response = match builder.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(
                timeout = e.is_timeout(),
                connect = e.is_connect(),
                "input.autocomplete upstream request failed"
            );
            return None;
        }
    };
    let status = response.status();
    if !status.is_success() {
        tracing::debug!(
            status = status.as_u16(),
            "input.autocomplete upstream returned an error status"
        );
        return None;
    }
    let bytes = match response.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::debug!(
                timeout = e.is_timeout(),
                "input.autocomplete upstream body read failed"
            );
            return None;
        }
    };
    if bytes.len() > MAX_RESPONSE_BYTES {
        tracing::debug!(
            bytes = bytes.len(),
            "input.autocomplete upstream body too large"
        );
        return None;
    }
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        tracing::debug!("input.autocomplete upstream body is not JSON");
        return None;
    };
    let content = extract_content(&value);
    if content.is_none() {
        tracing::debug!("input.autocomplete upstream reply has no message content");
    }
    content.map(str::to_string)
}

#[cfg(test)]
#[path = "autocomplete_ops_tests.rs"]
mod tests;
