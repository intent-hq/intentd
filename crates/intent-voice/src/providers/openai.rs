//! `OpenAI` transcription provider (`POST /v1/audio/transcriptions`, multipart).
//!
//! Uses the configured model (`voice.openai.model`, default
//! `gpt-4o-transcribe`) and falls back to `whisper-1` when the selected model
//! is unavailable (404 / model-not-found) — unless `whisper-1` itself was
//! selected — so accounts without gpt-4o-transcribe access still work. The
//! composed `prompt` (style hint + vocabulary + request prompt) biases the
//! transcription.
//!
//! GUARDRAIL: the API key is a secret. It is stored only to build the
//! `Authorization` header and is never logged or exposed via `Debug`.

use async_trait::async_trait;
use serde_json::Value;

use crate::engine::{TranscribeRequest, Transcript, VoiceEngine};
use crate::error::{Error, Result};

/// Default `OpenAI` REST base URL.
pub(crate) const OPENAI_API_BASE_URL: &str = "https://api.openai.com";

/// Default transcription model (the `voice.openai.model` catalog default).
const DEFAULT_MODEL: &str = "gpt-4o-transcribe";
/// Fallback model when the selected model is unavailable on the account.
const FALLBACK_MODEL: &str = "whisper-1";

/// `OpenAI` implementation of [`VoiceEngine`].
pub(crate) struct OpenAiEngine {
    http: reqwest::Client,
    /// Secret API key. Never logged, printed, or surfaced via `Debug`.
    api_key: String,
    base_url: String,
    /// Selected transcription model (defaults to [`DEFAULT_MODEL`]).
    model: String,
}

impl std::fmt::Debug for OpenAiEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiEngine")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl OpenAiEngine {
    /// Build an engine from an API key, optionally targeting a custom
    /// endpoint (defaults to [`OPENAI_API_BASE_URL`]) and selecting a
    /// transcription model (defaults to [`DEFAULT_MODEL`]).
    pub fn new(api_key: &str, base_url: Option<&str>, model: Option<&str>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Config(format!("failed to build http client: {e}")))?;
        Ok(Self {
            http,
            api_key: api_key.to_string(),
            base_url: base_url
                .unwrap_or(OPENAI_API_BASE_URL)
                .trim_end_matches('/')
                .to_string(),
            model: match model.map(str::trim) {
                Some(m) if !m.is_empty() => m.to_string(),
                _ => DEFAULT_MODEL.to_string(),
            },
        })
    }

    /// Build the multipart form for one attempt (forms are not cloneable).
    fn build_form(request: &TranscribeRequest, model: &str) -> Result<reqwest::multipart::Form> {
        let file_name = super::elevenlabs::file_name_for(&request.mime_type);
        let part = reqwest::multipart::Part::bytes(request.audio.clone())
            .file_name(file_name)
            .mime_str(&request.mime_type)
            .map_err(|e| Error::Config(format!("invalid mime type: {e}")))?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", model.to_string())
            .text("response_format", "json");
        if let Some(lang) = &request.language {
            form = form.text("language", lang.clone());
        }
        if let Some(prompt) = &request.prompt {
            form = form.text("prompt", prompt.clone());
        }
        Ok(form)
    }

    /// One transcription attempt against `model`.
    async fn attempt(&self, request: &TranscribeRequest, model: &str) -> Result<Transcript> {
        let url = format!("{}/v1/audio/transcriptions", self.base_url);
        let form = Self::build_form(request, model)?;
        let resp = self
            .http
            .post(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.api_key),
            )
            .multipart(form)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let detail: String = body.chars().take(300).collect();
            return Err(match status.as_u16() {
                401 | 403 => Error::Auth(format!("openai returned {status}: {detail}")),
                429 => Error::RateLimited(format!("openai returned {status}: {detail}")),
                404 => Error::ModelUnavailable(format!("openai returned {status}: {detail}")),
                _ => Error::Api(format!("openai returned {status}: {detail}")),
            });
        }
        let body: Value = resp.json().await?;
        let text = body
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Decode("openai response missing 'text'".to_string()))?
            .to_string();
        let duration_ms = body
            .get("duration")
            .and_then(Value::as_f64)
            // Float→int casts saturate; durations are non-negative seconds.
            .map(|secs| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let ms = (secs * 1000.0) as u64;
                ms
            });
        Ok(Transcript { text, duration_ms })
    }
}

#[async_trait]
impl VoiceEngine for OpenAiEngine {
    async fn transcribe(&self, request: TranscribeRequest) -> Result<Transcript> {
        match self.attempt(&request, &self.model).await {
            Ok(t) => Ok(t),
            // Model unavailable on this account → retry once with whisper-1
            // (unless whisper-1 was the selected model).
            Err(Error::ModelUnavailable(_)) if self.model != FALLBACK_MODEL => {
                self.attempt(&request, FALLBACK_MODEL).await
            }
            Err(e) => Err(e),
        }
    }

    fn provider_name(&self) -> &'static str {
        "openai"
    }
}

#[cfg(test)]
mod tests;
