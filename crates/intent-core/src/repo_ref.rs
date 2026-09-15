//! Host-agnostic repository identity (`owner/name` slug) shared by every
//! crate that holds a forge repo slug — the sourcecontrol forge API, the
//! store, the services fold sites, and the slug-bearing domain models.

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Identifies a repository on a forge (host-agnostic).
///
/// Forge owner/repo slugs are case-insensitive (GitHub resolves `Intent-HQ/IntentD`
/// and `intent-hq/intentd` to the same repository), so equality and hashing fold
/// ASCII case: two refs that differ only in case compare equal and hash alike.
/// The `owner` / `name` fields keep the caller's casing for display and
/// serialization; use [`RepoRef::identity_key`] (or
/// [`RepoRef::identity_parts`]) when a folded form is needed for map keys or
/// SQL parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoRef {
    pub owner: String,
    pub name: String,
}

impl RepoRef {
    /// Convenience constructor.
    pub fn new(owner: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            name: name.into(),
        }
    }

    /// Case-folded `(owner, name)` (ASCII lowercase) — the parts of the
    /// identity this ref compares and hashes by.
    #[must_use]
    pub fn identity_parts(&self) -> (String, String) {
        (
            self.owner.to_ascii_lowercase(),
            self.name.to_ascii_lowercase(),
        )
    }

    /// Case-folded `"owner/name"` (ASCII lowercase), suitable as a map key or
    /// SQL parameter wherever refs must collapse across casing.
    #[must_use]
    pub fn identity_key(&self) -> String {
        let (owner, name) = self.identity_parts();
        format!("{owner}/{name}")
    }
}

impl PartialEq for RepoRef {
    fn eq(&self, other: &Self) -> bool {
        self.owner.eq_ignore_ascii_case(&other.owner) && self.name.eq_ignore_ascii_case(&other.name)
    }
}

impl Eq for RepoRef {}

impl Hash for RepoRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (owner, name) = self.identity_parts();
        owner.hash(state);
        name.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::RepoRef;

    #[test]
    fn repo_ref_equality_folds_ascii_case() {
        assert_eq!(
            RepoRef::new("Intent-HQ", "IntentD"),
            RepoRef::new("intent-hq", "intentd")
        );
    }

    #[test]
    fn repo_ref_hash_is_consistent_with_eq() {
        let mut set = HashSet::new();
        set.insert(RepoRef::new("Intent-HQ", "IntentD"));
        set.insert(RepoRef::new("intent-hq", "intentd"));
        assert_eq!(set.len(), 1);
        assert!(set.contains(&RepoRef::new("INTENT-HQ", "INTENTD")));
    }

    #[test]
    fn repo_ref_different_repos_are_unequal() {
        assert_ne!(
            RepoRef::new("intent-hq", "intentd"),
            RepoRef::new("intent-hq", "cloudlands-fe")
        );
        assert_ne!(
            RepoRef::new("intent-hq", "intentd"),
            RepoRef::new("other-org", "intentd")
        );
    }

    #[test]
    fn repo_ref_fields_keep_caller_casing() {
        let repo = RepoRef::new("Intent-HQ", "IntentD");
        assert_eq!(repo.owner, "Intent-HQ");
        assert_eq!(repo.name, "IntentD");
    }

    #[test]
    fn repo_ref_identity_key_and_parts_are_lowercase() {
        let repo = RepoRef::new("Intent-HQ", "IntentD");
        assert_eq!(repo.identity_key(), "intent-hq/intentd");
        assert_eq!(
            repo.identity_parts(),
            ("intent-hq".to_string(), "intentd".to_string())
        );
    }

    #[test]
    fn repo_ref_serde_round_trip_preserves_casing() {
        let repo = RepoRef::new("Intent-HQ", "IntentD");
        let json = serde_json::to_string(&repo).expect("serialize");
        assert_eq!(json, r#"{"owner":"Intent-HQ","name":"IntentD"}"#);
        let back: RepoRef = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.owner, "Intent-HQ");
        assert_eq!(back.name, "IntentD");
        assert_eq!(back, repo);
    }
}
