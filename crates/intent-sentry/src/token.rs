//! Sentry credential pair.
//!
//! Sentry needs **both** an organization slug and an API token. The daemon
//! reads the saved account (`accounts.sentry.*`) and passes it inline; each
//! missing half falls back to `SENTRY_ORG` / `SENTRY_API_TOKEN`
//! ([`env_parts`]). A missing pair is *not* an error here — the registry turns
//! it into a graceful `NotConfigured`.
//!
//! GUARDRAIL: the token is a secret. It is only ever read and handed to the
//! HTTP client — never logged, echoed, or returned across the wire.

/// A resolved Sentry credential pair. The `token` field is secret and is
/// only handed to the HTTP client.
#[derive(Clone)]
pub struct Credentials {
    pub token: String,
    pub organization: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("organization", &self.organization)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Possibly-partial credential pair. Halves are non-empty when present. The
/// `token` field is secret.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct CredentialParts {
    pub(crate) token: Option<String>,
    pub(crate) organization: Option<String>,
}

impl std::fmt::Debug for CredentialParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialParts")
            .field("organization", &self.organization)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl CredentialParts {
    /// Fill each missing half from `fallback`.
    #[must_use]
    pub fn or(self, fallback: Self) -> Self {
        Self {
            token: self.token.or(fallback.token),
            organization: self.organization.or(fallback.organization),
        }
    }

    /// `Some` only when both halves are present.
    #[must_use]
    pub fn complete(self) -> Option<Credentials> {
        Some(Credentials {
            token: self.token?,
            organization: self.organization?,
        })
    }
}

/// The `SENTRY_API_TOKEN` / `SENTRY_ORG` halves that are set and non-empty.
pub(crate) fn env_parts() -> CredentialParts {
    CredentialParts {
        token: std::env::var("SENTRY_API_TOKEN")
            .ok()
            .as_deref()
            .and_then(non_empty),
        organization: std::env::var("SENTRY_ORG")
            .ok()
            .as_deref()
            .and_then(non_empty),
    }
}

/// `Some(s)` only when `s` is non-empty after trimming.
pub(crate) fn non_empty(s: &str) -> Option<String> {
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

    fn parts(token: Option<&str>, organization: Option<&str>) -> CredentialParts {
        CredentialParts {
            token: token.map(str::to_string),
            organization: organization.map(str::to_string),
        }
    }

    #[test]
    fn parts_or_fills_only_missing_halves() {
        let merged = parts(None, Some("saved-org")).or(parts(Some("env-tok"), Some("env-org")));
        assert_eq!(merged, parts(Some("env-tok"), Some("saved-org")));

        let merged = parts(Some("saved-tok"), None).or(parts(Some("env-tok"), Some("env-org")));
        assert_eq!(merged, parts(Some("saved-tok"), Some("env-org")));

        let merged = parts(Some("saved-tok"), Some("saved-org")).or(parts(Some("x"), Some("y")));
        assert_eq!(merged, parts(Some("saved-tok"), Some("saved-org")));
    }

    #[test]
    fn parts_complete_needs_both_halves() {
        let c = parts(Some("tok"), Some("acme")).complete().unwrap();
        assert_eq!((c.token.as_str(), c.organization.as_str()), ("tok", "acme"));
        assert!(parts(Some("tok"), None).complete().is_none());
        assert!(parts(None, Some("acme")).complete().is_none());
    }

    #[test]
    fn blank_values_are_absent() {
        assert_eq!(non_empty("  tok "), Some("tok".to_string()));
        assert_eq!(non_empty("   "), None);
        assert_eq!(non_empty(""), None);
    }

    #[test]
    fn parts_debug_redacts_token() {
        let dbg = format!("{:?}", parts(Some("supersecret"), Some("acme")));
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("redacted"));
        assert!(dbg.contains("acme"));
    }

    #[test]
    fn debug_redacts_token() {
        let c = Credentials {
            token: "supersecret".into(),
            organization: "acme".into(),
        };
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("redacted"));
        assert!(dbg.contains("acme"));
    }
}
