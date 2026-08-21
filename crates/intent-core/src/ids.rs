//! Strongly-typed entity identifiers (§9.6).
//!
//! Each id is a transparent newtype wrapping a UUID string. `new()` mints a
//! fresh `UUIDv4`; `Display`/`FromStr` and `From` conversions round-trip the
//! underlying string so ids serialize identically to the existing TS wire
//! format (`#[serde(transparent)]`).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            /// Mint a new id backed by a fresh `UUIDv4`.
            pub fn new() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            /// Wrap an existing id string.
            pub fn from_string(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            /// Borrow the underlying string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = std::convert::Infallible;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                Ok(Self(s.to_string()))
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }
    };
}

id_newtype!(
    /// Identifier for a workspace.
    WorkspaceId
);

/// Reserved workspace id for the daemon-known virtual "Chief of Staff"
/// workspace (TS `CHIEF_WORKSPACE_ID` in `shared/types/branded-ids.ts`). Chief
/// is not a real workspace on disk: it has no repository/worktree and never
/// appears in `workspace.list`, but `workspace.get` returns a synthesized
/// [`crate::model::chief_workspace`] shape and `agent.create` accepts it as
/// the workspace scope for Chief-of-Staff agents.
pub const CHIEF_WORKSPACE_ID: &str = "__chief__";

impl WorkspaceId {
    /// The reserved [`CHIEF_WORKSPACE_ID`] as a strongly-typed id.
    pub fn chief() -> Self {
        Self(CHIEF_WORKSPACE_ID.to_string())
    }

    /// Whether this id is the reserved [`CHIEF_WORKSPACE_ID`].
    pub fn is_chief(&self) -> bool {
        self.0 == CHIEF_WORKSPACE_ID
    }
}
id_newtype!(
    /// Identifier for a note.
    NoteId
);
id_newtype!(
    /// Identifier for an agent.
    AgentId
);
id_newtype!(
    /// Identifier for a logical client (stable, client-supplied identity; §16).
    /// Distinct from the ephemeral per-connection id used for transport
    /// bookkeeping — this is the key that disambiguates `drafts.*` (§5.16).
    ClientId
);
id_newtype!(
    /// Identifier for a background hook (an agent-owned scheduled script).
    HookId
);
id_newtype!(
    /// Identifier for a PR monitor (an agent-owned pull-request watch).
    PrMonitorId
);
id_newtype!(
    /// Identifier for a registered workspace git root (a secondary local git
    /// repository tracked for a workspace).
    WorkspaceGitRootId
);
