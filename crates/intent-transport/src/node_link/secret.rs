//! The executor credential's bearer token as the backend presents it on
//! the link. It never appears in `Debug`, `Display`, serialization or error
//! text.

use std::fmt;

/// The secret the backend presents to the machine on every link
/// connection: the executor credential's token.
#[derive(Clone, PartialEq, Eq)]
pub struct ExecutorToken(String);

/// Shortest accepted token, in bytes.
const MIN_TOKEN_BYTES: usize = 32;

impl ExecutorToken {
    /// Wrap a stored token.
    ///
    /// # Errors
    ///
    /// `node-link-config` when the token is shorter than 32 bytes or holds
    /// characters that cannot travel in an HTTP header.
    pub fn new(token: impl Into<String>) -> intent_core::Result<Self> {
        let token = token.into();
        if token.len() < MIN_TOKEN_BYTES || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(super::config(
                "executor token must be at least 32 printable ASCII bytes",
            ));
        }
        Ok(Self(token))
    }

    /// The token text, for the `Authorization` header.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ExecutorToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExecutorToken([redacted])")
    }
}
