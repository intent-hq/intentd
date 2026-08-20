//! ElevenLabs Scribe provider (`POST /v1/speech-to-text`, multipart).
//!
//! Uses `model_id=scribe_v2` — required for keyterm biasing. Merged keyterms
//! are sent as repeated `keyterms` form fields (the encoding the ElevenLabs
//! SDKs produce for array data parts).
//!
//! GUARDRAIL: the API key is a secret. It is stored only to build the
//! `xi-api-key` header and is never logged or exposed via `Debug`.

use async_trait::async_trait;
use serde_json::Value;

use crate::engine::{TranscribeRequest, Transcript, VoiceEngine};
use crate::error::{Error, Result};

/// Default ElevenLabs REST base URL.
pub(crate) const ELEVENLABS_API_BASE_URL: &str = "https://api.elevenlabs.io";

/// Scribe model used for batch transcription (keyterms require v2).
const MODEL_ID: &str = "scribe_v2";

/// ElevenLabs Scribe implementation of [`VoiceEngine`].
pub(crate) struct ElevenLabsEngine {
    http: reqwest::Client,
    /// Secret API key. Never logged, printed, or surfaced via `Debug`.
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for ElevenLabsEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElevenLabsEngine")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl ElevenLabsEngine {
    /// Build an engine from an API key, optionally targeting a custom
    /// endpoint (defaults to [`ELEVENLABS_API_BASE_URL`]).
    pub fn new(api_key: &str, base_url: Option<&str>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Config(format!("failed to build http client: {e}")))?;
        Ok(Self {
            http,
            api_key: api_key.to_string(),
            base_url: base_url
                .unwrap_or(ELEVENLABS_API_BASE_URL)
                .trim_end_matches('/')
                .to_string(),
        })
    }

    /// Build the multipart form for one attempt (forms are not cloneable).
    fn build_form(&self, request: &TranscribeRequest) -> Result<reqwest::multipart::Form> {
        let file_name = file_name_for(&request.mime_type);
        let part = reqwest::multipart::Part::bytes(request.audio.clone())
            .file_name(file_name)
            .mime_str(&request.mime_type)
            .map_err(|e| Error::Config(format!("invalid mime type: {e}")))?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model_id", MODEL_ID);
        if let Some(lang) = &request.language {
            form = form.text("language_code", lang.clone());
        }
        for term in &request.keyterms {
            form = form.text("keyterms", term.clone());
        }
        Ok(form)
    }
}

/// A representative file name for the multipart `file` part.
pub(crate) fn file_name_for(mime_type: &str) -> String {
    let ext = match mime_type.split(';').next().unwrap_or("").trim() {
        "audio/wav" | "audio/x-wav" | "audio/wave" => "wav",
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/mp4" | "audio/m4a" | "audio/x-m4a" => "m4a",
        "audio/ogg" => "ogg",
        "audio/flac" => "flac",
        _ => "webm",
    };
    format!("audio.{ext}")
}

#[async_trait]
impl VoiceEngine for ElevenLabsEngine {
    async fn transcribe(&self, request: TranscribeRequest) -> Result<Transcript> {
        let url = format!("{}/v1/speech-to-text", self.base_url);
        let form = self.build_form(&request)?;
        let resp = self
            .http
            .post(&url)
            .header("xi-api-key", &self.api_key)
            .multipart(form)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let detail = truncate(&body, 300);
            return Err(match status.as_u16() {
                401 | 403 => Error::Auth(format!("elevenlabs returned {status}: {detail}")),
                429 => Error::RateLimited(format!("elevenlabs returned {status}: {detail}")),
                _ => Error::Api(format!("elevenlabs returned {status}: {detail}")),
            });
        }
        let body: Value = resp.json().await?;
        let text = body
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Decode("elevenlabs response missing 'text'".to_string()))?
            .to_string();
        // Duration: last word's `end` timestamp (seconds), when present.
        let duration_ms = body
            .get("words")
            .and_then(Value::as_array)
            .and_then(|words| words.last())
            .and_then(|w| w.get("end"))
            .and_then(Value::as_f64)
            .map(|secs| (secs * 1000.0) as u64);
        Ok(Transcript { text, duration_ms })
    }

    fn provider_name(&self) -> &'static str {
        "elevenlabs"
    }
}

/// First `max` chars of `s` (provider error bodies can be huge).
fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

#[cfg(test)]
mod tests;
