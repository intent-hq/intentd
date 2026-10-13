//! Nonsecret, owner-only policy for the daemon's on-demand child Git helper.
//!
//! A binding comes from the original daemon connection. Supplying it cannot
//! grant authority, and a write must compare it with current server-owned facts.

use serde::{Deserialize, Serialize};

use crate::RepositoryConnectionScope;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitlabChildGitPolicyQuery {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GitlabChildGitBinding {
    #[serde(deserialize_with = "instance")]
    pub instance_base_url: String,
    #[serde(deserialize_with = "connection")]
    pub connection: RepositoryConnectionScope,
    /// Opaque server policy revision; compare for equality, never order it.
    #[serde(deserialize_with = "identifier")]
    pub revision: String,
}

fn bounded<'de, D: serde::Deserializer<'de>>(d: D, max: usize) -> Result<String, D::Error> {
    let value = String::deserialize(d)?;
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(serde::de::Error::custom("invalid original policy binding"));
    }
    Ok(value)
}

fn identifier<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    bounded(d, 256)
}

fn instance<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    // The provider parses and compares the complete logical URL. This limit
    // bounds the wire input without reconstructing or guessing that identity.
    bounded(d, 2048)
}

fn connection<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<RepositoryConnectionScope, D::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Original {
        #[serde(deserialize_with = "identifier")]
        connection_id: String,
        #[serde(deserialize_with = "identifier")]
        account_id: String,
        #[serde(with = "crate::repository_context::decimal_u64")]
        connection_generation: u64,
    }
    let original = Original::deserialize(d)?;
    Ok(RepositoryConnectionScope {
        connection_id: original.connection_id,
        account_id: original.account_id,
        connection_generation: original.connection_generation,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GitlabChildGitPolicySet {
    pub binding: GitlabChildGitBinding,
    pub enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GitlabChildGitUnavailable {
    Disconnected,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum GitlabChildGitPolicy {
    /// Current verified account/full instance and actual stored choice. New or
    /// unmatched grants are disabled. This does not report glab API login.
    Connected {
        binding: GitlabChildGitBinding,
        enabled: bool,
        effective: bool,
    },
    /// There is no current bindable account. Do not infer a stored false choice.
    Unavailable { reason: GitlabChildGitUnavailable },
}

/// Scope supplied by the original on-demand helper. The pid is audit metadata,
/// never process authority; the actual local connection supplies admission.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GitlabChildGitCredentialQuery {
    #[serde(default)]
    pub pid: Option<u64>,
    pub protocol: String,
    pub host: String,
    pub path: String,
    #[serde(default)]
    pub username: Option<String>,
}

/// An internal response, serialized only into an original local private output.
/// Deliberately not Debug or Deserialize: this carries actual credential bytes.
#[derive(Serialize)]
pub struct GitlabChildGitCredential {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct GitlabChildGitCredentialResult {
    pub credential: Option<GitlabChildGitCredential>,
}

/// Typed original frame; none of these comparison values establish authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GitlabChildGitFrame {
    Get(GitlabChildGitPolicyQuery),
    Set(GitlabChildGitPolicySet),
    Credential(GitlabChildGitCredentialQuery),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn policy_writes_require_original_binding_and_explicit_choice() {
        let binding = json!({
            "instanceBaseUrl":"https://forge.test:8443/install",
            "connection":{"connectionId":"original","accountId":"account-a","connectionGeneration":"7"},
            "revision":"original-policy"
        });
        let request: GitlabChildGitPolicySet =
            serde_json::from_value(json!({"binding":binding,"enabled":false})).unwrap();
        assert!(!request.enabled);
        assert_eq!(request.binding.connection.connection_generation, 7);
        for invalid in [
            json!({"enabled":true}),
            json!({"binding":binding,"enabled":null}),
            json!({"binding":binding,"enabled":true,"token":"injected"}),
        ] {
            assert!(serde_json::from_value::<GitlabChildGitPolicySet>(invalid).is_err());
        }
        assert!(
            serde_json::from_value::<GitlabChildGitPolicyQuery>(json!({"host":"other.test"}))
                .is_err()
        );
        for path in ["instanceBaseUrl", "revision"] {
            let mut invalid = binding.clone();
            invalid[path] = json!("x".repeat(2049));
            assert!(serde_json::from_value::<GitlabChildGitBinding>(invalid).is_err());
        }
        for invalid in [
            json!({"connectionId":"", "accountId":"account-a", "connectionGeneration":"7"}),
            json!({"connectionId":"a", "accountId":"account-a", "connectionGeneration":"07"}),
            json!({"connectionId":"a", "accountId":"account-a", "connectionGeneration":7}),
            json!({"connectionId":"a", "accountId":"account-a", "connectionGeneration":"7", "token":"injected"}),
        ] {
            let mut value = binding.clone();
            value["connection"] = invalid;
            assert!(serde_json::from_value::<GitlabChildGitBinding>(value).is_err());
        }
    }
}
