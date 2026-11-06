use std::fmt;
use std::time::Duration;

use intent_core::FileSecretStore;
use serde::Deserialize;

use super::E2bError;

/// Secret-store account that holds the backend's E2B API key.
pub const API_KEY_ACCOUNT: &str = "e2b.apiKey";

/// Bounded wait for a secret-store read, like the other token loaders.
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

/// A secret value whose `Debug` output is redacted. It has no `Display` and
/// no `Serialize`, so it cannot reach logs or the wire by accident.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw value. Only hand it to the HTTP layer.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Read the E2B API key from the backend secret store. An absent or empty
/// entry is `Ok(None)`.
///
/// # Errors
///
/// [`E2bError::SecretStore`] when the store is unreadable, corrupt, or does
/// not answer within a few seconds.
pub async fn load_api_key(store: FileSecretStore) -> Result<Option<SecretString>, E2bError> {
    let read = tokio::task::spawn_blocking(move || store.load(API_KEY_ACCOUNT));
    match tokio::time::timeout(SECRET_LOAD_TIMEOUT, read).await {
        Ok(Ok(Ok(value))) => Ok(value.filter(|v| !v.is_empty()).map(SecretString)),
        Ok(Ok(Err(_))) => Err(E2bError::SecretStore(
            "the secret store is unreadable or corrupt".into(),
        )),
        Ok(Err(_)) => Err(E2bError::SecretStore("the secret store read failed".into())),
        Err(_) => Err(E2bError::SecretStore(
            "the secret store read timed out".into(),
        )),
    }
}
