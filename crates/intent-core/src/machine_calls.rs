//! Answers of the calls the head makes to one connected machine.

/// One secret frame for a machine secret call. `Debug`
/// never shows a value.
pub enum MachineSecretOp {
    /// Hold `value` as secret `name`.
    Put {
        name: String,
        value: zeroize::Zeroizing<String>,
    },
    /// Drop secret `name`, or all of the agent's secrets.
    Forget { name: Option<String> },
    /// The held names.
    List,
}

impl std::fmt::Debug for MachineSecretOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put { name, .. } => f
                .debug_struct("Put")
                .field("name", name)
                .finish_non_exhaustive(),
            Self::Forget { name } => f.debug_struct("Forget").field("name", name).finish(),
            Self::List => f.write_str("List"),
        }
    }
}

/// A machine's answer for a machine secret call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineSecretAnswer {
    /// `Put` held the value.
    Stored,
    /// `Forget`: whether anything was dropped.
    Forgotten(bool),
    /// `List`: the held names, sorted.
    Names(Vec<String>),
    /// `Forget` for a machine that is not connected: sent after its next
    /// bind.
    Queued,
    /// No link came up, it is older than nodeProtocol 14, or it did not
    /// answer in time.
    Offline,
    /// The machine refused, with its fixed code.
    Failed(String),
}

/// A machine's answer for a machine skills query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineSkills {
    /// The user skills it listed.
    Listed(Vec<crate::node_link::MachineSkill>),
    /// It is reachable (no query was made).
    Reachable,
    /// No link came up, or it did not answer in time.
    Offline,
    /// Its link speaks a protocol older than
    /// [`crate::node_link::SKILLS_PROTOCOL`].
    UpdateRequired,
}
