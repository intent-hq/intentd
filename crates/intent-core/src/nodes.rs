//! Internal node ownership vocabulary. These records are not public inventory
//! projections and confer no principal/agent authority by themselves.

use crate::{AgentId, WorkspaceId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Full-width counters travel as decimal strings, but compare numerically.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeCounter(pub u64);
impl Serialize for NodeCounter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}
impl<'de> Deserialize<'de> for NodeCounter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        let n = s.parse::<u64>().map_err(serde::de::Error::custom)?;
        if n.to_string() != s {
            return Err(serde::de::Error::custom("noncanonical counter"));
        }
        Ok(Self(n))
    }
}

/// A diagnostic node-owned path, deliberately without Path/AsRef<Path> conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodePath(pub String);

macro_rules! node_enum {
    ($name:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($variant),+ }
    };
}
node_enum!(NodeKind { Local, Static });
node_enum!(NodeOs { Linux, Macos });
node_enum!(NodeArch { X86_64, Aarch64 });
node_enum!(NodeState {
    Ready,
    Offline,
    Incompatible,
    Removed
});
node_enum!(LeaseState {
    Acquiring,
    Ready,
    Busy,
    Idle,
    Released,
    Lost
});
node_enum!(LeaseFenceKind {
    ProcessesStopped,
    OfflineBudgetElapsed
});

impl LeaseState {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Released | Self::Lost)
    }

    /// Terminal transitions additionally require durable stop/fence evidence.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Acquiring, Self::Ready | Self::Released | Self::Lost)
                    | (Self::Ready, Self::Busy | Self::Released | Self::Lost)
                    | (
                        Self::Busy,
                        Self::Idle | Self::Ready | Self::Released | Self::Lost
                    )
                    | (
                        Self::Idle,
                        Self::Busy | Self::Ready | Self::Released | Self::Lost
                    )
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCapacity {
    pub max_agents: u32,
    pub reserved_agents: u32,
    pub memory_budget_bytes: u64,
    pub used_memory_bytes: u64,
    pub exclusive_reserved: bool,
}

/// Secrets stay in the secret store; this record contains no bootstrap/lease token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRecord {
    pub id: String,
    pub head_id: String,
    pub node_identity: String,
    pub name: String,
    pub kind: NodeKind,
    pub os: NodeOs,
    pub arch: NodeArch,
    pub state: NodeState,
    pub draining: bool,
    pub capacity: NodeCapacity,
    pub version: String,
    pub endpoint: Option<String>,
    pub certificate_sha256: Option<String>,
    pub last_seen_at: Option<String>,
}

/// Bound by authenticated admission, never constructed from forwarded RPC claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseOwner {
    pub head_id: String,
    pub node_id: String,
    pub node_identity: String,
    pub lease_id: String,
    pub incarnation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseFence {
    pub kind: LeaseFenceKind,
    pub recorded_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeLease {
    pub owner: LeaseOwner,
    pub state: LeaseState,
    pub created_at: String,
    pub last_seen_at: Option<String>,
    pub ack_seq: NodeCounter,
    pub link_generation: NodeCounter,
    pub release_requested: bool,
    pub fence: Option<LeaseFence>,
}

/// A run admission and the exact resource scope it owns. Rows survive deletion
/// as tombstones so a delayed upload cannot recreate ownership or reset epochs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeAssignment {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub owner: LeaseOwner,
    pub run_id: String,
    pub assignment_epoch: NodeCounter,
    pub repo_keys: Vec<String>,
    pub node_path: Option<NodePath>,
    pub active: bool,
    pub tombstoned: bool,
    /// Immutable inherited checkpoint and execution-base mapping lives in the
    /// checkpoint manifest; this pins that baseline for subsequent child work.
    pub inherited_checkpoint_id: Option<String>,
    pub merge_target_agent_id: Option<AgentId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCheckpoint {
    pub id: String,
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub owner: LeaseOwner,
    pub run_id: String,
    pub assignment_epoch: NodeCounter,
    pub capture_revision: NodeCounter,
    pub journal_seq: NodeCounter,
    pub manifest_sha256: String,
    pub captured_at: String,
    pub committed_at: String,
}
node_enum!(CheckpointOutcome {
    Advanced,
    Historical
});
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointReceipt {
    pub checkpoint_id: String,
    pub outcome: CheckpointOutcome,
    pub current_checkpoint_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counters_are_lossless_numeric_and_canonical() {
        for n in [0, 9, 10, u64::MAX] {
            let json = serde_json::to_string(&NodeCounter(n)).unwrap();
            assert_eq!(json, format!("\"{n}\""));
            assert_eq!(
                serde_json::from_str::<NodeCounter>(&json).unwrap(),
                NodeCounter(n)
            );
        }
        assert!(NodeCounter(10) > NodeCounter(9));
        for invalid in ["1", "\"01\"", "\"-1\"", "\"18446744073709551616\""] {
            assert!(serde_json::from_str::<NodeCounter>(invalid).is_err());
        }
    }
    #[test]
    fn lease_transitions_do_not_reacquire_terminal_hosts() {
        use LeaseState::{Acquiring, Busy, Idle, Lost, Ready, Released};
        for state in [Acquiring, Ready, Busy, Idle, Released, Lost] {
            assert!(state.can_transition_to(state));
            assert_eq!(Released.can_transition_to(state), state == Released);
            assert_eq!(Lost.can_transition_to(state), state == Lost);
        }
        assert!(Acquiring.can_transition_to(Ready));
        assert!(!Acquiring.can_transition_to(Busy));
        assert!(Ready.can_transition_to(Busy));
        assert!(Busy.can_transition_to(Idle));
        assert!(Idle.can_transition_to(Busy));
    }
}
