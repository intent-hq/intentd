//! Next-turn model selection carried by a workspace transfer
//! (intent-hq/intent#5815).
//!
//! An `agent_session` row stores its next-turn selection as the
//! `(provider, model, reasoning_effort)` triple, where each NULL means "the
//! daemon's default": a NULL `provider` resolves to `model.defaultProvider`
//! at every spawn ([`crate::agent_session::resolve_provider_id`]), a NULL
//! `model` is the provider's CLI default (Auto), and a NULL
//! `reasoning_effort` is the provider's default effort (Auto). On the
//! source machine that is unambiguous; once the row travels to another
//! daemon a NULL `provider` silently re-resolves to the *destination*
//! default, while the (foreign) `model` keeps riding along — the exact
//! mismatch behind #5815 (an Auggie model id attributed to Codex).
//!
//! Two pure transforms fix the representation without changing the archive
//! layout or touching the source table:
//!
//! - [`pin_source_selection`] (export side): every exported session row
//!   whose `provider` is unset gains the source's effective default
//!   provider, so the archive carries the provider the source would use
//!   for its next turn. `model` / `reasoning_effort` stay as stored — they already
//!   ARE the effective selection: the spawn path (`resolve_spawn`) passes
//!   `session.model` / `session.reasoning_effort` through verbatim, and a
//!   NULL means no `--model` / effort flag, i.e. the provider's CLI default.
//!   The settings defaults (`model.default`, `model.providerDefaults`,
//!   `model.defaultReasoningEffort`) participate only at creation time,
//!   where the resolver persists them into the row; filling a NULL model
//!   from them at export would change what the session runs. Only the
//!   provider is re-resolved lazily at every spawn, so only the provider
//!   needs pinning. The historical `last_turn_*` columns are never
//!   touched: they record the last committed turn, not the next-turn
//!   selection.
//! - [`resolve_imported_selection`] (import side): archives written before
//!   this pin — or by a source with no default provider — can still carry
//!   a provider-less row. Such a row keeps its `model` / `reasoning_effort`
//!   only when the last committed turn is evidence for a provider (the
//!   `last_turn_provider` that ran exactly this `model`); otherwise the
//!   whole selection is cleared so the destination applies its defaults as
//!   a unit. A missing provider is never inferred from the destination
//!   default while a foreign model id is retained.
//!
//! Contract for the rows that leave the import transform: an `agent_session`
//! row either names its `provider` (a concrete selection the destination may
//! reconcile against its own availability) or has all three selection
//! fields unset (the destination's default selection, Auto).

use serde_json::{Map, Value};

/// A JSON column that is absent, `null`, or an empty string reads as unset —
/// the same leniency [`crate::agent_session::resolve_provider_id`] applies.
fn column_str(map: &Map<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Pin the source's effective default provider into every exported
/// `agent_session` row object whose `provider` is unset. Operates on the
/// in-memory archive rows only — the source table is never written — and
/// leaves rows with an explicit provider, and every other column, untouched.
/// With no resolvable source default (`None`) nothing changes: the import
/// side then applies its legacy resolution. Returns the number of rows
/// pinned.
pub(crate) fn pin_source_selection(
    rows: &mut [(String, Vec<Value>)],
    source_default_provider: Option<&str>,
) -> usize {
    let Some(default_provider) = source_default_provider.filter(|p| !p.is_empty()) else {
        return 0;
    };
    let mut pinned = 0;
    for (table, objects) in rows.iter_mut() {
        if table.as_str() != "agent_session" {
            continue;
        }
        for object in objects.iter_mut() {
            let Some(map) = object.as_object_mut() else {
                continue;
            };
            if column_str(map, "provider").is_some() {
                continue;
            }
            map.insert(
                "provider".into(),
                Value::String(default_provider.to_string()),
            );
            pinned += 1;
        }
    }
    pinned
}

/// What [`resolve_imported_selection`] did to one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ImportedSelection {
    /// The row already named its provider (or had nothing to resolve):
    /// nothing changed.
    Kept,
    /// A provider-less row adopted the named provider from its last
    /// committed turn, which ran exactly the stored model.
    RecoveredFromLastTurn(String),
    /// A provider-less row with no usable evidence had its `model` and
    /// `reasoning_effort` cleared: the destination's defaults apply as a
    /// unit.
    ClearedToDestinationDefault,
}

/// Resolve the next-turn selection of one imported `agent_session` row (see
/// the module docs). Rows with an explicit `provider`, and provider-less rows
/// with neither a `model` nor a `reasoning_effort`, pass through unchanged.
/// The `last_turn_*` columns are read as evidence but never modified.
pub(crate) fn resolve_imported_selection(map: &mut Map<String, Value>) -> ImportedSelection {
    if column_str(map, "provider").is_some() {
        return ImportedSelection::Kept;
    }
    let model = column_str(map, "model");
    let effort = column_str(map, "reasoning_effort");
    if model.is_none() && effort.is_none() {
        return ImportedSelection::Kept;
    }
    // Evidence: the last committed turn names a provider AND ran exactly the
    // stored model (both NULL = the provider default ran, which also
    // matches). A turn that ran a different model says nothing about which
    // provider the current selection was made against.
    let last_turn_provider = column_str(map, "last_turn_provider");
    let last_turn_model = column_str(map, "last_turn_model");
    if let Some(provider) = last_turn_provider {
        if last_turn_model == model {
            map.insert("provider".into(), Value::String(provider.clone()));
            return ImportedSelection::RecoveredFromLastTurn(provider);
        }
    }
    map.insert("model".into(), Value::Null);
    map.insert("reasoning_effort".into(), Value::Null);
    ImportedSelection::ClearedToDestinationDefault
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_rows(objects: Vec<Value>) -> Vec<(String, Vec<Value>)> {
        vec![
            (
                "workspace".to_string(),
                vec![serde_json::json!({ "id": "ws" })],
            ),
            ("agent_session".to_string(), objects),
        ]
    }

    /// Missing, NULL, and empty providers are pinned to the source default;
    /// explicit providers and all model / effort / history fields are kept.
    #[test]
    fn pin_fills_only_unset_provider_and_leaves_other_columns() {
        let mut rows = session_rows(vec![
            serde_json::json!({
                "id": "inherited", "provider": null, "model": "gpt6-astra",
                "reasoning_effort": "high",
                "last_turn_provider": "codex", "last_turn_model": "gpt-6-astra"
            }),
            serde_json::json!({
                "id": "explicit", "provider": "claude-code", "model": "claude-fable-5",
                "reasoning_effort": null
            }),
            serde_json::json!({ "id": "auto", "provider": "", "model": null }),
            serde_json::json!({ "id": "missing", "model": null, "reasoning_effort": "low" }),
        ]);
        assert_eq!(pin_source_selection(&mut rows, Some("auggie")), 3);
        let sessions = &rows[1].1;
        assert_eq!(sessions[0]["provider"], "auggie");
        assert_eq!(sessions[0]["model"], "gpt6-astra");
        assert_eq!(sessions[0]["reasoning_effort"], "high");
        assert_eq!(sessions[0]["last_turn_provider"], "codex");
        assert_eq!(sessions[0]["last_turn_model"], "gpt-6-astra");
        assert_eq!(sessions[1]["provider"], "claude-code");
        assert_eq!(sessions[1]["model"], "claude-fable-5");
        assert_eq!(sessions[1]["reasoning_effort"], Value::Null);
        assert_eq!(
            sessions[2]["provider"], "auggie",
            "empty string reads as unset"
        );
        assert_eq!(sessions[2]["model"], Value::Null, "Auto model stays Auto");
        assert_eq!(sessions[3]["provider"], "auggie");
        assert_eq!(sessions[3]["model"], Value::Null);
        assert_eq!(sessions[3]["reasoning_effort"], "low");
        assert_eq!(rows[0].1[0], serde_json::json!({ "id": "ws" }));
    }

    /// No resolvable source default: the rows are left exactly as exported.
    #[test]
    fn pin_without_source_default_is_a_no_op() {
        let original = session_rows(vec![
            serde_json::json!({ "id": "a", "provider": null, "model": "gpt6-astra" }),
        ]);
        for default in [None, Some("")] {
            let mut rows = original.clone();
            assert_eq!(pin_source_selection(&mut rows, default), 0);
            assert_eq!(rows, original);
        }
    }

    /// An explicit provider, and an all-Auto row, pass through untouched.
    #[test]
    fn import_keeps_explicit_and_all_auto_rows() {
        let mut explicit = serde_json::json!({
            "id": "a", "provider": "codex", "model": "gpt-6-astra", "reasoning_effort": "xhigh",
            "last_turn_provider": "auggie", "last_turn_model": "gpt6-astra"
        });
        let before = explicit.clone();
        assert_eq!(
            resolve_imported_selection(explicit.as_object_mut().unwrap()),
            ImportedSelection::Kept
        );
        assert_eq!(explicit, before);

        let mut auto = serde_json::json!({
            "id": "b", "provider": null, "model": null, "reasoning_effort": null,
            "last_turn_provider": "auggie", "last_turn_model": null
        });
        let before = auto.clone();
        assert_eq!(
            resolve_imported_selection(auto.as_object_mut().unwrap()),
            ImportedSelection::Kept
        );
        assert_eq!(auto, before, "no provider is inferred for an all-Auto row");
    }

    /// Legacy row (#5815 shape): provider NULL, a model id, and a last turn
    /// that ran exactly that model on a named provider — the provider is
    /// recovered from that evidence, the selection and history are kept.
    #[test]
    fn import_recovers_provider_from_matching_last_turn() {
        let mut row = serde_json::json!({
            "id": "legacy", "provider": null, "model": "gpt6-astra", "reasoning_effort": "high",
            "last_turn_provider": "auggie", "last_turn_model": "gpt6-astra"
        });
        assert_eq!(
            resolve_imported_selection(row.as_object_mut().unwrap()),
            ImportedSelection::RecoveredFromLastTurn("auggie".to_string())
        );
        assert_eq!(row["provider"], "auggie");
        assert_eq!(row["model"], "gpt6-astra");
        assert_eq!(row["reasoning_effort"], "high");
        assert_eq!(row["last_turn_provider"], "auggie");
        assert_eq!(row["last_turn_model"], "gpt6-astra");

        // Provider-default model on both sides (NULL == NULL) with an
        // explicit effort also counts as evidence.
        let mut row = serde_json::json!({
            "id": "effort-only", "provider": null, "model": null, "reasoning_effort": "low",
            "last_turn_provider": "codex", "last_turn_model": null
        });
        assert_eq!(
            resolve_imported_selection(row.as_object_mut().unwrap()),
            ImportedSelection::RecoveredFromLastTurn("codex".to_string())
        );
        assert_eq!(row["provider"], "codex");
        assert_eq!(row["reasoning_effort"], "low");
    }

    /// Without usable evidence the foreign model id (and its effort) must not
    /// survive next to a destination-inferred provider: the whole selection
    /// is cleared so the destination defaults apply as a unit. History stays.
    #[test]
    fn import_clears_selection_without_provider_evidence() {
        let cases = [
            // never ran a turn
            serde_json::json!({
                "id": "fresh", "provider": null, "model": "gpt6-astra", "reasoning_effort": "high",
                "last_turn_provider": null, "last_turn_model": null
            }),
            // last turn ran a different model — says nothing about this one
            serde_json::json!({
                "id": "switched", "provider": null, "model": "gpt6-astra", "reasoning_effort": null,
                "last_turn_provider": "auggie", "last_turn_model": "other-model"
            }),
            // archive predating the last_turn columns entirely
            serde_json::json!({ "id": "ancient", "provider": null, "model": "gpt6-astra" }),
        ];
        for mut row in cases {
            let id = row["id"].as_str().unwrap().to_string();
            let history = (
                row["last_turn_provider"].clone(),
                row["last_turn_model"].clone(),
            );
            assert_eq!(
                resolve_imported_selection(row.as_object_mut().unwrap()),
                ImportedSelection::ClearedToDestinationDefault,
                "{id}"
            );
            assert_eq!(
                row["provider"],
                Value::Null,
                "{id}: provider never inferred"
            );
            assert_eq!(row["model"], Value::Null, "{id}");
            assert_eq!(row["reasoning_effort"], Value::Null, "{id}");
            assert_eq!(
                (
                    row["last_turn_provider"].clone(),
                    row["last_turn_model"].clone()
                ),
                history,
                "{id}: history untouched"
            );
        }
    }
}
