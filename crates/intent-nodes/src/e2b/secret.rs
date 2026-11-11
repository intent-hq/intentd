use std::time::Duration;

pub use crate::cloud::SecretString;
use intent_core::FileSecretStore;

use super::E2bError;

/// Secret-store account that holds the backend's E2B API key.
pub const API_KEY_ACCOUNT: &str = "cloudMachines.e2b.apiKey";

/// Bounded wait for a secret-store read, like the other token loaders.
const SECRET_LOAD_TIMEOUT: Duration = Duration::from_secs(3);

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
        Ok(Ok(Ok(value))) => Ok(value.filter(|v| !v.is_empty()).map(SecretString::new)),
        Ok(Ok(Err(_))) => Err(E2bError::SecretStore(
            "the secret store is unreadable or corrupt".into(),
        )),
        Ok(Err(_)) => Err(E2bError::SecretStore("the secret store read failed".into())),
        Err(_) => Err(E2bError::SecretStore(
            "the secret store read timed out".into(),
        )),
    }
}
