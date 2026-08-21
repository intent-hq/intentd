//! Engine selection from settings.
//!
//! [`VoiceRegistry::from_settings`] resolves the provider API key (inline,
//! secrets store, or env fallback) and builds a [`VoiceEngine`]. A missing key
//! yields a typed [`Error::NotConfigured`] so the daemon stays up (graceful,
//! mirroring `intent-linear`).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::engine::VoiceEngine;
use crate::error::{Error, Result};
use crate::providers::elevenlabs::ElevenLabsEngine;
use crate::providers::openai::OpenAiEngine;
use crate::token;

/// A speech-to-text provider selectable via `voice.provider` or the per-call
/// `provider` override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VoiceProvider {
    /// `ElevenLabs` Scribe (`scribe_v2`) — the default.
    #[default]
    ElevenLabs,
    /// `OpenAI` (configurable model, `whisper-1` fallback).
    OpenAi,
}

impl VoiceProvider {
    /// Wire spelling (`elevenlabs` | `openai`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            VoiceProvider::ElevenLabs => "elevenlabs",
            VoiceProvider::OpenAi => "openai",
        }
    }

    /// Parse a wire spelling; `None` for anything unknown.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "elevenlabs" => Some(VoiceProvider::ElevenLabs),
            "openai" => Some(VoiceProvider::OpenAi),
            _ => None,
        }
    }
}

/// Voice settings (`voice.*`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoiceSettings {
    /// Selected provider (default `ElevenLabs`).
    #[serde(default)]
    pub provider: VoiceProvider,
    /// Inline API key (already resolved, e.g. read from the secrets store by
    /// the caller). When present and non-empty it takes precedence over the
    /// store/env resolution. SECRET — never logged.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Override for the provider API endpoint (defaults to the public API).
    #[serde(default)]
    pub api_base_url: Option<String>,
    /// `OpenAI` transcription model (`voice.openai.model`); `None` uses the
    /// engine default. Ignored by other providers.
    #[serde(default)]
    pub openai_model: Option<String>,
}

/// Builds the active [`VoiceEngine`] from settings.
pub struct VoiceRegistry;

impl VoiceRegistry {
    /// Construct the engine for the selected provider, or a typed
    /// [`Error::NotConfigured`] when no key is available. Async because the
    /// secrets-store lookup runs on the blocking pool with a bounded timeout
    /// (see [`token::resolve`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotConfigured`] when no API key is available; propagates engine-construction failures.
    pub async fn from_settings(settings: &VoiceSettings) -> Result<Arc<dyn VoiceEngine>> {
        let key = resolve_key(settings).await?;
        let base_url = settings.api_base_url.as_deref();
        Ok(match settings.provider {
            VoiceProvider::ElevenLabs => Arc::new(ElevenLabsEngine::new(&key, base_url)?),
            VoiceProvider::OpenAi => Arc::new(OpenAiEngine::new(
                &key,
                base_url,
                settings.openai_model.as_deref(),
            )?),
        })
    }
}

/// Resolve the API key from inline settings or the store/env chain.
async fn resolve_key(settings: &VoiceSettings) -> Result<String> {
    if let Some(key) = settings.api_key.as_deref() {
        if !key.trim().is_empty() {
            return Ok(key.trim().to_string());
        }
    }
    token::resolve(settings.provider).await.ok_or_else(|| {
        Error::NotConfigured(format!(
            "voice: no API key found for {} (set {} or {})",
            settings.provider.as_str(),
            token::secret_account(settings.provider),
            token::env_var(settings.provider),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_round_trips_wire_spellings() {
        assert_eq!(VoiceProvider::ElevenLabs.as_str(), "elevenlabs");
        assert_eq!(VoiceProvider::OpenAi.as_str(), "openai");
        assert_eq!(
            VoiceProvider::parse("elevenlabs"),
            Some(VoiceProvider::ElevenLabs)
        );
        assert_eq!(VoiceProvider::parse("openai"), Some(VoiceProvider::OpenAi));
        assert_eq!(VoiceProvider::parse("whisper"), None);
        assert_eq!(VoiceProvider::default(), VoiceProvider::ElevenLabs);
    }

    #[tokio::test]
    async fn inline_key_builds_engine() {
        let settings = VoiceSettings {
            api_key: Some("xi-test".to_string()),
            ..VoiceSettings::default()
        };
        let engine = VoiceRegistry::from_settings(&settings).await.unwrap();
        assert_eq!(engine.provider_name(), "elevenlabs");
    }

    #[tokio::test]
    async fn inline_key_selects_openai() {
        let settings = VoiceSettings {
            provider: VoiceProvider::OpenAi,
            api_key: Some("sk-test".to_string()),
            ..VoiceSettings::default()
        };
        let engine = VoiceRegistry::from_settings(&settings).await.unwrap();
        assert_eq!(engine.provider_name(), "openai");
    }
}
