//! The ordered machine list (monorepo `docs/protocol/methods/nodes.md`,
//! "Ordered machine list"): one user-ordered list of every place this
//! backend can run agents — "This computer" (regular mode only), exposed
//! machines and cloud machine entries.
//!
//! Only the user's saved order is stored (a list of keys). The list itself
//! is derived from it and the live rows each time: saved keys that still
//! name a row keep their place, rows the saved order does not name yet are
//! appended (the backend first, then machines, then entries), and keys
//! naming a removed row drop out. So expose, claim, remove and entry
//! changes never have to touch the order.

use serde::{Deserialize, Serialize};

/// The settings-table key holding the saved order (a JSON array of
/// [`MachineOrderKey`]).
pub const MACHINE_ORDER_STATE_KEY: &str = "remoteMachines.order";

/// The `data.code` of `remoteMachines.setOrder` when its list does not name
/// exactly the current rows.
pub const ORDER_STALE_CODE: &str = "order-stale";

/// One row of the list, without its display name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum MachineOrderKey {
    /// "This computer": the backend itself.
    Local,
    /// An exposed machine, by `nodeId`.
    #[serde(rename_all = "camelCase")]
    Machine { node_id: String },
    /// A cloud machine entry, by `cloudMachineId`.
    #[serde(rename_all = "camelCase")]
    Cloud { cloud_machine_id: String },
}

/// One row of the list as `remoteMachines.order` returns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MachineOrderRow {
    #[serde(flatten)]
    pub key: MachineOrderKey,
    pub name: String,
}

/// The live rows the list is derived from.
#[derive(Debug, Clone, Default)]
pub struct MachineOrderRows {
    /// The backend's own display name, `None` when it has no "This
    /// computer" row (an orchestrator).
    pub local: Option<String>,
    /// Exposed machines `(nodeId, name)`, oldest first.
    pub machines: Vec<(String, String)>,
    /// Cloud machine entries `(cloudMachineId, name)`, in creation order.
    pub entries: Vec<(String, String)>,
}

impl MachineOrderRows {
    fn all(&self) -> Vec<MachineOrderRow> {
        let local = self.local.iter().map(|name| MachineOrderRow {
            key: MachineOrderKey::Local,
            name: name.clone(),
        });
        let machines = self.machines.iter().map(|(id, name)| MachineOrderRow {
            key: MachineOrderKey::Machine {
                node_id: id.clone(),
            },
            name: name.clone(),
        });
        let entries = self.entries.iter().map(|(id, name)| MachineOrderRow {
            key: MachineOrderKey::Cloud {
                cloud_machine_id: id.clone(),
            },
            name: name.clone(),
        });
        local.chain(machines).chain(entries).collect()
    }
}

/// The list: `saved` keys that still name a row, in saved order, then the
/// rows `saved` does not name. A missing "This computer" row goes first,
/// so a backend keeps running unplaced agents locally until the user
/// moves another row above it.
#[must_use]
pub fn ordered(saved: &[MachineOrderKey], rows: &MachineOrderRows) -> Vec<MachineOrderRow> {
    let mut pending = rows.all();
    let mut out = Vec::with_capacity(pending.len());
    if !saved.contains(&MachineOrderKey::Local) {
        if let Some(at) = pending
            .iter()
            .position(|row| row.key == MachineOrderKey::Local)
        {
            out.push(pending.remove(at));
        }
    }
    for key in saved {
        if let Some(at) = pending.iter().position(|row| &row.key == key) {
            out.push(pending.remove(at));
        }
    }
    out.extend(pending);
    out
}

/// Whether `order` names exactly the rows of `current`, each once.
#[must_use]
pub fn names_exactly(order: &[MachineOrderKey], current: &[MachineOrderRow]) -> bool {
    if order.len() != current.len() {
        return false;
    }
    let mut seen = std::collections::HashSet::with_capacity(order.len());
    order
        .iter()
        .all(|key| seen.insert(key) && current.iter().any(|row| &row.key == key))
}

/// Read the saved order. An entry that does not parse (for example a kind
/// a newer build saved) is skipped on its own, so the rest of the user's
/// order stands; an unreadable value is no saved order.
#[must_use]
pub fn parse_saved(raw: Option<&str>) -> Vec<MachineOrderKey> {
    let Some(entries) =
        raw.and_then(|raw| serde_json::from_str::<Vec<serde_json::Value>>(raw).ok())
    else {
        return Vec::new();
    };
    entries
        .into_iter()
        .filter_map(|entry| serde_json::from_value(entry).ok())
        .collect()
}

/// `saved` without `removed`: a removed machine or entry leaves the list,
/// so re-adding it later appends it at the end.
#[must_use]
pub fn without(saved: &[MachineOrderKey], removed: &MachineOrderKey) -> Vec<MachineOrderKey> {
    saved
        .iter()
        .filter(|key| *key != removed)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
