//! The per-lease bearer token. It never appears in `Debug`, `Display`,
//! serialization or error text.

use std::fmt;

use subtle::ConstantTimeEq;

/// The secret head presents to the node on every link connection.
#[derive(Clone, PartialEq, Eq)]
pub struct LeaseToken(String);

/// Shortest accepted token, in bytes.
const MIN_TOKEN_BYTES: usize = 32;

impl LeaseToken {
    /// Wrap a configured token.
    ///
    /// # Errors
    ///
    /// `node-link-config` when the token is shorter than 32 bytes or holds
    /// characters that cannot travel in an HTTP header.
    pub fn new(token: impl Into<String>) -> intent_core::Result<Self> {
        let token = token.into();
        if token.len() < MIN_TOKEN_BYTES || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(super::config(
                "lease token must be at least 32 printable ASCII bytes",
            ));
        }
        Ok(Self(token))
    }

    /// Generate a random 32-byte token, hex-encoded.
    ///
    /// # Errors
    ///
    /// Returns an error if the OS random source fails.
    pub fn generate() -> intent_core::Result<Self> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)
            .map_err(|_| intent_core::Error::Internal("lease token randomness".into()))?;
        Ok(Self(hex::encode(bytes)))
    }

    /// The token text, for writing it into a config file or a header.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Constant-time comparison against a presented token.
    #[must_use]
    pub fn matches(&self, presented: &str) -> bool {
        self.0.as_bytes().ct_eq(presented.as_bytes()).into()
    }
}

impl fmt::Debug for LeaseToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LeaseToken([redacted])")
    }
}
