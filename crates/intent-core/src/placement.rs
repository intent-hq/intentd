//! Agent placement vocabulary: the documented `placement` object on agent
//! creation and the `placement-unavailable` error data (monorepo
//! `docs/protocol/model-platform-routing.md`, `methods/nodes.md`).
//!
//! A placement is a launch constraint, never authority: matching a node's
//! platform grants nothing. Callers still check spawn authority, the node's
//! link and its capacity before launching.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::nodes::{NodeArch, NodeOs};
use crate::Error;

/// Longest accepted `placement.nodeId`, in UTF-8 bytes.
pub const MAX_NODE_ID_BYTES: usize = 128;

/// `placement.target`: a location filter, not a configured target name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlacementTarget {
    Local,
    Remote,
}

/// `placement.checkout`. Absent on a placement object means `isolated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlacementCheckout {
    Shared,
    Worktree,
    Isolated,
}

/// The closed `placement` request object. Every member is optional and `{}`
/// is a deliberate whole request. Deserialization rejects null members,
/// unknown or duplicate members and contradictory constraints before any
/// effect. Use [`present`] on the containing field so an omitted
/// `placement` stays distinct from an explicit one and `null` is rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireObject", rename_all = "camelCase")]
pub struct AgentPlacement {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<PlacementTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkout: Option<PlacementCheckout>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<NodeOs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arch: Option<NodeArch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive: Option<bool>,
}

/// Wire form of [`AgentPlacement`]: serde's derive rejects unknown and
/// duplicate members (escaped equivalent keys included, since keys are
/// compared after unescaping); [`present`] rejects explicit nulls.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WirePlacement {
    #[serde(default, deserialize_with = "present")]
    target: Option<PlacementTarget>,
    #[serde(default, deserialize_with = "present")]
    checkout: Option<PlacementCheckout>,
    #[serde(default, deserialize_with = "present")]
    os: Option<NodeOs>,
    #[serde(default, deserialize_with = "present")]
    arch: Option<NodeArch>,
    #[serde(default, deserialize_with = "present")]
    node_id: Option<String>,
    #[serde(default, deserialize_with = "present")]
    exclusive: Option<bool>,
}

/// Accepts only a JSON object: a derived struct would also accept an array
/// of members in declaration order.
struct WireObject(WirePlacement);

impl<'de> Deserialize<'de> for WireObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
            type Value = WireObject;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a placement object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<WireObject, A::Error> {
                WirePlacement::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(WireObject)
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

/// Field adapter for an optional member that may be omitted but never set
/// to `null`. Pair it with `#[serde(default)]`.
///
/// # Errors
///
/// The inner type's deserialization error, including for `null`.
pub fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl TryFrom<WireObject> for AgentPlacement {
    type Error = &'static str;

    fn try_from(WireObject(wire): WireObject) -> Result<Self, Self::Error> {
        let placement = Self {
            target: wire.target,
            checkout: wire.checkout,
            os: wire.os,
            arch: wire.arch,
            node_id: wire.node_id,
            exclusive: wire.exclusive,
        };
        placement.validate()?;
        Ok(placement)
    }
}

impl AgentPlacement {
    /// The documented validity rules, also for placements built in Rust.
    ///
    /// # Errors
    ///
    /// A safe description of the first rule the placement breaks.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self
            .node_id
            .as_ref()
            .is_some_and(|id| id.is_empty() || id.len() > MAX_NODE_ID_BYTES)
        {
            return Err("placement.nodeId must contain 1-128 UTF-8 bytes");
        }
        if self.target == Some(PlacementTarget::Remote)
            && self.effective_checkout() != PlacementCheckout::Isolated
        {
            return Err("remote placement requires isolated checkout");
        }
        if self.exclusive == Some(true)
            && (self.target == Some(PlacementTarget::Local)
                || self.effective_checkout() != PlacementCheckout::Isolated)
        {
            return Err("exclusive placement requires remote isolated checkout");
        }
        Ok(())
    }

    /// The checkout this placement asks for: `isolated` when absent.
    #[must_use]
    pub fn effective_checkout(&self) -> PlacementCheckout {
        self.checkout.unwrap_or(PlacementCheckout::Isolated)
    }

    /// Whether `platform` satisfies the requested `os` and `arch`, each
    /// independently. Identity, capacity and authority are separate checks.
    #[must_use]
    pub fn matches_platform(&self, platform: AgentPlatform) -> bool {
        self.os.is_none_or(|os| os == platform.os)
            && self.arch.is_none_or(|arch| arch == platform.arch)
    }

    /// The subset this daemon's remote launch supports: a valid placement
    /// that is not explicitly local and does not ask for an exclusive node.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParams`] naming the unsupported member.
    pub fn check_remote_supported(&self) -> crate::Result<()> {
        self.validate()
            .map_err(|reason| Error::InvalidParams(reason.into()))?;
        if self.target == Some(PlacementTarget::Local) {
            return Err(Error::InvalidParams(
                "remote launch requires placement.target \"remote\", not \"local\"".into(),
            ));
        }
        if self.exclusive == Some(true) {
            return Err(Error::InvalidParams(
                "placement.exclusive is not supported yet; omit it or set it to false".into(),
            ));
        }
        Ok(())
    }
}

/// One `{ os, arch }` pair a node actually advertises. Never built as a
/// cross product of separate OS and architecture lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPlatform {
    pub os: NodeOs,
    pub arch: NodeArch,
}

impl AgentPlatform {
    fn sort_key(self) -> (&'static str, &'static str) {
        (os_name(self.os), arch_name(self.arch))
    }
}

impl fmt::Display for AgentPlatform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", os_name(self.os), arch_name(self.arch))
    }
}

/// The wire name of an OS (`linux`, `macos`).
#[must_use]
pub fn os_name(os: NodeOs) -> &'static str {
    match os {
        NodeOs::Linux => "linux",
        NodeOs::Macos => "macos",
    }
}

/// The wire name of an architecture (`x86_64`, `aarch64`).
#[must_use]
pub fn arch_name(arch: NodeArch) -> &'static str {
    match arch {
        NodeArch::X86_64 => "x86_64",
        NodeArch::Aarch64 => "aarch64",
    }
}

/// Platform discovery shared by tool help and `placement-unavailable`
/// errors. `supported_platforms` are the platforms a configured node runs;
/// `available_platforms` those it can admit an agent on right now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlatformAvailability {
    pub available_platforms: Vec<AgentPlatform>,
    pub supported_platforms: Vec<AgentPlatform>,
    /// The sorted unique OS projection of `available_platforms`.
    pub available_os: Vec<NodeOs>,
    pub observed_at: String,
}

impl PlatformAvailability {
    /// Build discovery data, sorting and de-duplicating both platform lists
    /// and deriving `available_os` from the available ones.
    #[must_use]
    pub fn new(
        supported: impl IntoIterator<Item = AgentPlatform>,
        available: impl IntoIterator<Item = AgentPlatform>,
        observed_at: String,
    ) -> Self {
        let available_platforms = sorted_platforms(available);
        let mut available_os: Vec<NodeOs> = available_platforms.iter().map(|p| p.os).collect();
        available_os.sort_by_key(|os| os_name(*os));
        available_os.dedup();
        Self {
            available_platforms,
            supported_platforms: sorted_platforms(supported),
            available_os,
            observed_at,
        }
    }

    /// Discovery when no node is configured: nothing supported or available.
    #[must_use]
    pub fn none(observed_at: String) -> Self {
        Self::new([], [], observed_at)
    }

    /// The `placement-unavailable` failure for `requested`: `no-capacity`
    /// (retryable) when a supported platform matches the request,
    /// `unsupported-platform` (not retryable) otherwise.
    #[must_use]
    pub fn unavailable(self, requested: AgentPlacement) -> PlacementUnavailable {
        let supported = self
            .supported_platforms
            .iter()
            .any(|platform| requested.matches_platform(*platform));
        let reason = if supported {
            PlacementUnavailableReason::NoCapacity
        } else {
            PlacementUnavailableReason::UnsupportedPlatform
        };
        PlacementUnavailable {
            requested,
            reason,
            availability: self,
        }
    }
}

fn sorted_platforms(platforms: impl IntoIterator<Item = AgentPlatform>) -> Vec<AgentPlatform> {
    let mut platforms: Vec<AgentPlatform> = platforms.into_iter().collect();
    platforms.sort_by_key(|p| p.sort_key());
    platforms.dedup();
    platforms
}

/// `placement-unavailable` `reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlacementUnavailableReason {
    UnsupportedPlatform,
    NoCapacity,
}

impl PlacementUnavailableReason {
    /// `no-capacity` may succeed later; `unsupported-platform` cannot.
    #[must_use]
    pub fn retryable(self) -> bool {
        matches!(self, Self::NoCapacity)
    }
}

/// No node can run the requested placement. Surfaces as JSON-RPC `-32602`
/// with [`PlacementUnavailable::message`] and [`PlacementUnavailable::data`];
/// the `Display` form (the MCP error text) carries both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementUnavailable {
    pub requested: AgentPlacement,
    pub reason: PlacementUnavailableReason,
    pub availability: PlatformAvailability,
}

/// `error.data.code` of [`PlacementUnavailable`].
pub const PLACEMENT_UNAVAILABLE_CODE: &str = "placement-unavailable";

impl PlacementUnavailable {
    /// The human message, for example
    /// `No capacity for x86_64; available: none; supported: linux/x86_64`.
    #[must_use]
    pub fn message(&self) -> String {
        let requested = match (self.requested.os, self.requested.arch) {
            (Some(os), Some(arch)) => format!("{}/{}", os_name(os), arch_name(arch)),
            (Some(os), None) => os_name(os).to_string(),
            (None, Some(arch)) => arch_name(arch).to_string(),
            (None, None) => "any platform".to_string(),
        };
        let lead = match self.reason {
            PlacementUnavailableReason::NoCapacity => "No capacity for",
            PlacementUnavailableReason::UnsupportedPlatform => "Unsupported",
        };
        format!(
            "{lead} {requested}; available: {}; supported: {}",
            platform_list(&self.availability.available_platforms),
            platform_list(&self.availability.supported_platforms),
        )
    }

    /// The documented `error.data` object.
    #[must_use]
    pub fn data(&self) -> serde_json::Value {
        let mut data = serde_json::json!({
            "code": PLACEMENT_UNAVAILABLE_CODE,
            "requested": self.requested,
            "reason": self.reason,
            "retryable": self.reason.retryable(),
        });
        if let (Some(data), Ok(serde_json::Value::Object(availability))) = (
            data.as_object_mut(),
            serde_json::to_value(&self.availability),
        ) {
            data.extend(availability);
        }
        data
    }
}

fn platform_list(platforms: &[AgentPlatform]) -> String {
    if platforms.is_empty() {
        return "none".to_string();
    }
    platforms
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl fmt::Display for PlacementUnavailable {
    /// MCP renders errors as text, so the text keeps the typed data too.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.message(), self.data())
    }
}

#[cfg(test)]
mod tests;
