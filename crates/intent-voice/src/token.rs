//! Voice provider API-key resolution.
//!
//! Keys are resolved secrets-store-first, then environment:
//!
//! 1. File-backed secrets store ([`intent_core::FileSecretStore`],
//!    `~/intent/secrets.json`) under the sensitive setting path as account
//!    (`voice.elevenlabs.apiKey` / `voice.openai.apiKey`).
//! 2. Environment fallback: `ELEVENLABS_API_KEY` / `OPENAI_API_KEY`.
//!
//! A missing key is *not* an error here — [`resolve`] returns `None`, and the
//! registry turns that into a graceful `NotConfigured`.
//!
//! GUARDRAIL: the key is a secret. It is only ever read and handed to the
//! HTTP client — never logged, echoed, or returned across the wire.

use std::time::Duration;

use tokio::time::timeout;

use crate::registry::VoiceProvider;

/// Bounded wait for a secrets-store read before treating the entry as absent.
/// Mirrors `intent-linear::token::SECRET_LOAD_TIMEOUT`.
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// Secrets-store account (= sensitive setting path) for a provider's API key.
pub(crate) fn secret_account(provider: VoiceProvider) -> &'static str {
    match provider {
        VoiceProvider::ElevenLabs => "voice.elevenlabs.apiKey",
        VoiceProvider::OpenAi => "voice.openai.apiKey",
    }
}

/// Environment variable fallback for a provider's API key.
pub(crate) fn env_var(provider: VoiceProvider) -> &'static str {
    match provider {
        VoiceProvider::ElevenLabs => "ELEVENLABS_API_KEY",
        VoiceProvider::OpenAi => "OPENAI_API_KEY",
    }
}

/// Resolve the API key for `provider`: secrets store first, then env.
/// `None` when neither yields a non-empty value.
pub async fn resolve(provider: VoiceProvider) -> Option<String> {
    match file_store_key(provider).await {
        Some(v) => Some(v),
        None => env_key(provider),
    }
}

/// Read the key from the file-backed secrets store. A missing or unreadable
/// entry resolves to `None` so resolution can fall through. Runs on the
/// blocking pool with a bounded timeout so a stalled backing store cannot
/// wedge a tokio worker.
async fn file_store_key(provider: VoiceProvider) -> Option<String> {
    let account = secret_account(provider);
    let handle =
        tokio::task::spawn_blocking(move || intent_core::FileSecretStore::new().load(account));
    match timeout(SECRET_LOAD_TIMEOUT, handle).await {
        Ok(Ok(Ok(Some(v)))) => non_empty(&v),
        Ok(Ok(Ok(None))) => None,
        Ok(Ok(Err(e))) => {
            tracing::warn!(
                account = %account,
                error = %e,
                "secrets-store load failed for voice API key (corrupt/unreadable file)"
            );
            None
        }
        Ok(Err(_)) => None,
        Err(_) => {
            tracing::warn!(
                account = %account,
                "secrets-store load timed out for voice API key"
            );
            None
        }
    }
}

/// Read the provider's env fallback.
fn env_key(provider: VoiceProvider) -> Option<String> {
    pick_env_key(std::env::var(env_var(provider)).ok().as_deref())
}

/// Pure selection of the env key (testable), ignoring empty values.
pub(crate) fn pick_env_key(value: Option<&str>) -> Option<String> {
    value.and_then(non_empty)
}

/// `Some(s)` only when `s` is non-empty after trimming.
fn non_empty(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_and_env_vars_per_provider() {
        assert_eq!(
            secret_account(VoiceProvider::ElevenLabs),
            "voice.elevenlabs.apiKey"
        );
        assert_eq!(secret_account(VoiceProvider::OpenAi), "voice.openai.apiKey");
        assert_eq!(env_var(VoiceProvider::ElevenLabs), "ELEVENLABS_API_KEY");
        assert_eq!(env_var(VoiceProvider::OpenAi), "OPENAI_API_KEY");
    }

    #[test]
    fn picks_non_empty_env_key() {
        assert_eq!(pick_env_key(Some("sk-abc")).as_deref(), Some("sk-abc"));
        assert_eq!(pick_env_key(Some("   ")), None);
        assert_eq!(pick_env_key(None), None);
    }
}
