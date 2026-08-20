//! intent-voice — speech-to-text engine + providers backing the `voice.*` surface.
//!
//! Sibling to `intent-linear` / `intent-sentry`: depends on `intent-core` only
//! among workspace crates. It defines the [`VoiceEngine`] trait (one
//! `transcribe` call), two provider implementations — ElevenLabs Scribe
//! (`scribe_v2`, multipart POST with `keyterms` biasing) and OpenAI
//! (`gpt-4o-transcribe` with `whisper-1` fallback, `prompt` biasing) — plus
//! the vocabulary / context-merging helpers and a
//! [`VoiceRegistry`] that builds the engine from settings with graceful
//! `NotConfigured`.
//!
//! No `voice.*` wire methods or routing live here — those map onto this
//! engine in `intent-services` / `intent-transport`.
//!
//! GUARDRAIL: the provider API keys are secrets. They are read only to
//! authenticate HTTP requests; they are never logged, printed, or returned
//! across the wire.

pub mod context;
pub mod engine;
pub mod error;
pub mod extract;
pub mod providers;
pub mod registry;
pub mod token;

pub use engine::{TranscribeRequest, Transcript, VoiceEngine};
pub use error::{Error, Result};
pub use extract::{extract_vocabulary, SourceKind};
pub use registry::{VoiceProvider, VoiceRegistry, VoiceSettings};
