//! Portable historical attribution, deliberately separate from local authority.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{lift_from_principal_id, Principal, PrincipalId, PrincipalIdentity};

pub const HUMAN_AUTHOR_KEY: &str = "humanAuthor";

/// Safe source-reported history. The source principal is provenance only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HumanAuthor {
    pub login: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<PrincipalIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_principal_id: Option<PrincipalId>,
}

impl HumanAuthor {
    #[must_use]
    pub fn from_source(id: Option<PrincipalId>, person: Option<&Principal>) -> Self {
        Self {
            login: person.and_then(|p| p.login.clone()),
            display_name: person.and_then(|p| p.display_name.clone()),
            avatar_url: person.and_then(|p| p.avatar_url.clone()),
            identity: person.and_then(Principal::identity_key),
            source_principal_id: id,
        }
    }

    /// Historical display identity never acquires a destination principal.
    #[must_use]
    pub fn to_wire(&self) -> Value {
        let mut row = json!({
            "principalId": null, "login":self.login,
            "displayName":self.display_name, "avatarUrl":self.avatar_url,
        });
        if let Some(identity) = &self.identity {
            row["identity"] = json!(identity);
        }
        row
    }
}

/// Presence suppresses local fallback even if a damaged stored snapshot cannot
/// be decoded. Archive ingress validates strictly before making it trusted.
#[must_use]
pub fn historical_human_author(metadata: Option<&Value>) -> Option<HumanAuthor> {
    metadata?
        .as_object()?
        .get(HUMAN_AUTHOR_KEY)
        .map(|raw| serde_json::from_value(raw.clone()).unwrap_or_default())
}

/// Imported human instructions have attribution, but no local admission.
#[must_use]
pub fn is_unbound_historical_human(metadata: Option<&Value>) -> bool {
    lift_from_principal_id(metadata).is_none() && historical_human_author(metadata).is_some()
}

/// All untrusted live/history entry points must discard the reserved snapshot.
pub fn strip_historical_human_author(metadata: &mut Value) {
    if let Some(object) = metadata.as_object_mut() {
        object.remove(HUMAN_AUTHOR_KEY);
    }
}
