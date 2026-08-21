//! `VoiceEngine` — the speech-to-text surface backing `voice.transcribe`.
//!
//! One method: [`VoiceEngine::transcribe`] takes decoded audio bytes plus the
//! merged transcription context and returns the transcript text. Providers
//! (`ElevenLabs` Scribe, `OpenAI`) implement this over multipart HTTP; tests
//! inject stubs.

use async_trait::async_trait;

use crate::error::Result;

/// A transcription request handed to a provider: decoded audio bytes plus
/// the already-merged biasing context (see [`crate::context`]).
#[derive(Debug, Clone, Default)]
pub struct TranscribeRequest {
    /// Decoded (raw) audio bytes.
    pub audio: Vec<u8>,
    /// MIME type of `audio` (e.g. `audio/webm`, `audio/wav`).
    pub mime_type: String,
    /// Optional ISO language hint (e.g. `en`).
    pub language: Option<String>,
    /// Merged keyterm vocabulary (static base ⊕ request), deduped and capped
    /// per [`crate::context`]. `ElevenLabs` sends these as `keyterms`; `OpenAI`
    /// folds them into the composed `prompt`.
    pub keyterms: Vec<String>,
    /// Composed `OpenAI` `prompt` (style hint + vocabulary + request prompt).
    /// Ignored by `ElevenLabs`.
    pub prompt: Option<String>,
}

/// A provider transcription result.
#[derive(Debug, Clone, PartialEq)]
pub struct Transcript {
    /// The transcribed text.
    pub text: String,
    /// Audio duration reported by the provider, in milliseconds, when known.
    pub duration_ms: Option<u64>,
}

/// The speech-to-text API consumed by the `voice.transcribe` wire method.
#[async_trait]
pub trait VoiceEngine: Send + Sync {
    /// Transcribe `request.audio` and return the transcript.
    async fn transcribe(&self, request: TranscribeRequest) -> Result<Transcript>;

    /// Wire name of the provider backing this engine (`elevenlabs` | `openai`).
    fn provider_name(&self) -> &'static str;
}
