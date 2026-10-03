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
//!   provider (or its legacy model prefix), so the archive carries the
//!   provider the source would use for its next turn.
//!   `model` / `reasoning_effort` stay as stored — they already
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
//!   only when its legacy model prefix names the provider or the last
//!   committed turn is evidence for a provider (the `last_turn_provider`
//!   that ran exactly this `model`); otherwise the
//!   whole selection is cleared so the destination applies its defaults as
//!   a unit. A missing provider is never inferred from the destination
//!   default while a foreign model id is retained.
//!
//! Contract for the rows that leave the import transform: an `agent_session`
//! row either names its `provider` (a concrete selection the destination may
//! reconcile against its own availability) or has all three selection
//! fields unset (awaiting destination application-default resolution).

use intent_store::normalize_compound_model;
use serde_json::{Map, Value};

use crate::agent_ops::{
    ensure_effort_supported_by_model, ensure_known_provider,
    ensure_provider_available_with_discovery, resolve_agent_default_model_with_source,
    resolve_settings_default_reasoning_effort,
};

const IMPORT_METHOD: &str = "workspace.import.commit";

/// Legacy aliases identify the registry's provider, never the destination's
/// configured default. Unknown ids remain unknown for availability validation.
fn canonical_source_provider(provider: &str) -> &str {
    intent_providers::find_provider_or_legacy_alias(provider).map_or(provider, |config| config.id)
}

/// Only destination evidence participates here. Unknown/expired auth verdicts
/// and missing/stale/failed catalogs stay permissive, as at agent creation.
/// A non-empty fresh catalog can disprove an explicit model or effort. Auto
/// stays Auto; its explicit effort, if any, is checked against the advertised
/// default model when the catalog identifies one.
fn selection_unavailable(
    services: &crate::Services,
    provider: &str,
    model: Option<&str>,
    effort: Option<&str>,
) -> intent_core::Result<()> {
    let cache = services.cached_models();
    let Some(catalog) = cache.fresh_catalog(provider) else {
        return Ok(());
    };
    // Match resolve_spawn's interpretation of old archives: a compound id
    // passes its bare part, while a historical display label means Auto.
    // Validation never rewrites a supported stored selection.
    let explicit_model = model
        .map(|m| m.split_once(':').map_or(m, |(_, bare)| bare))
        .filter(|m| !m.is_empty() && !m.contains(char::is_whitespace) && *m != "default");
    // Reuse spawn's provider-specific legacy effort interpretation, including
    // explicit-field precedence. Other providers may use slash model ids.
    let config = intent_providers::provider_config(provider);
    let effort =
        crate::agent_manager::AgentManager::session_model_effort(config, explicit_model, effort);
    let explicit_model = explicit_model.map(|model| {
        if config.config_option_model_strips_effort {
            crate::agent_manager::AgentManager::split_codex_model_effort(model).0
        } else {
            model
        }
    });
    if let Some(model) = explicit_model {
        if cache.cached_catalog_claims(provider, model) == Some(false) {
            return Err(intent_core::Error::InvalidParams(format!(
                "{IMPORT_METHOD}: model {model} is not available from provider {provider}"
            )));
        }
    }
    if let Some(effort) = effort.as_deref() {
        let default_model = catalog
            .iter()
            .find(|row| row.get("isDefault").and_then(Value::as_bool) == Some(true))
            .and_then(|row| row.get("id"))
            .and_then(Value::as_str);
        // Scope effort evidence to this provider: bare ids may be shared
        // across providers with different effort vocabularies.
        let scoped = explicit_model.or(default_model).map(|model| {
            if model.starts_with(&format!("{provider}:")) {
                model.to_string()
            } else {
                format!("{provider}:{model}")
            }
        });
        ensure_effort_supported_by_model(IMPORT_METHOD, &cache, scoped.as_deref(), effort)?;
    }
    Ok(())
}

impl crate::Services {
    /// Run on the blocking pool before `transfer_import_rows`. Discovery and
    /// catalog reads are destination-local and never spawn a turn or probe.
    pub(crate) fn reconcile_imported_selections(&self, rows: &mut [(String, Vec<Value>)]) {
        let settings = self.effective_settings();
        // One discovery per provider, not per imported agent.
        let mut availability = std::collections::HashMap::new();
        let mut available = |provider: &str| -> Result<(), String> {
            availability
                .entry(provider.to_string())
                .or_insert_with(|| {
                    ensure_known_provider(IMPORT_METHOD, provider)
                        .and_then(|()| {
                            ensure_provider_available_with_discovery(
                                IMPORT_METHOD,
                                provider,
                                &settings.providers,
                                || {
                                    #[cfg(test)]
                                    if let Some(fixture) = &self.import_provider_availability {
                                        return fixture.get(provider).cloned();
                                    }
                                    intent_providers::provider_availability_for(provider, &|key| {
                                        settings.providers.paths.get(key).cloned()
                                    })
                                },
                            )
                        })
                        .map_err(|e| e.to_string())
                })
                .clone()
        };
        let mut destination_default = None;
        let now = intent_core::now_iso();
        for (_, sessions) in rows
            .iter_mut()
            .filter(|(table, _)| table == "agent_session")
        {
            for session in sessions {
                let Some(map) = session.as_object_mut() else {
                    continue;
                };
                // Runtime capabilities were learned on the source machine.
                map.insert("effort_levels".into(), Value::Null);
                // Legacy prefixes override the provider column on store
                // reads; validate exactly the selection the first turn sees.
                let stored_provider = column_str(map, "provider");
                let (model, provider) =
                    normalize_compound_model(column_str(map, "model"), stored_provider.clone());
                if let Some(config) = provider
                    .as_deref()
                    .and_then(intent_providers::find_provider_or_legacy_alias)
                {
                    // Active aliases must not reach consumers that interpret
                    // them as the destination default. Canonicalize the same
                    // effective selection as store reads and spawn; history
                    // and non-alias selections remain untouched.
                    let has_alias = provider.as_deref() != Some(config.id)
                        || stored_provider
                            .as_deref()
                            .is_some_and(|p| canonical_source_provider(p) != p);
                    if has_alias {
                        map.insert("provider".into(), serde_json::json!(config.id));
                        map.insert("model".into(), serde_json::json!(model));
                    }
                }
                let source = provider
                    .as_deref()
                    .map(canonical_source_provider)
                    .map_or_else(
                        || Err("the source provider is unknown".to_string()),
                        |p| {
                            available(p).and_then(|()| {
                                selection_unavailable(
                                    self,
                                    p,
                                    model.as_deref(),
                                    column_str(map, "reasoning_effort").as_deref(),
                                )
                                .map_err(|e| e.to_string())
                            })
                        },
                    );
                let Err(source_reason) = source else { continue };
                let fallback = destination_default.get_or_insert_with(|| {
                    let provider = crate::agent_session::derived_default_provider(&settings)
                        .ok_or_else(|| "no default provider is configured".to_string())?;
                    available(&provider)?;
                    // The same model/effort default chain as plain creation;
                    // imported specialist and runtime state never select it.
                    let (model, model_source) =
                        resolve_agent_default_model_with_source(self, None, None, Some(&provider));
                    let effort = resolve_settings_default_reasoning_effort(
                        self,
                        model_source,
                        model.as_deref(),
                    );
                    selection_unavailable(self, &provider, model.as_deref(), effort.as_deref())
                        .map_err(|e| e.to_string())?;
                    Ok::<_, String>((provider, model, effort))
                });
                match fallback {
                    Ok((provider, model, effort)) => {
                        map.insert("provider".into(), Value::String(provider.clone()));
                        map.insert("model".into(), serde_json::json!(model));
                        map.insert("reasoning_effort".into(), serde_json::json!(effort));
                        tracing::info!(agent = ?map.get("id"), %provider, %source_reason,
                            "import: applied destination selection defaults");
                    }
                    Err(reason) => {
                        // Keep history and the original selection, surface the
                        // actionable failure through existing attention UI.
                        map.insert(
                            "attention_request_kind".into(),
                            serde_json::json!("blocker"),
                        );
                        map.insert("attention_request_reason".into(), serde_json::json!(format!(
                            "Imported agent needs configuration: {source_reason}; destination default cannot be used: {reason}. Choose an available provider, model and effort in Settings > Agents before sending a message."
                        )));
                        map.insert("attention_request_timestamp".into(), serde_json::json!(now));
                    }
                }
            }
        }
    }
}

/// A JSON column that is absent, `null`, or an empty string reads as unset —
/// the same leniency [`crate::agent_session::resolve_provider_id`] applies.
fn column_str(map: &Map<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Pin the source's effective provider into every exported
/// `agent_session` row object whose `provider` is unset. Operates on the
/// in-memory archive rows only — the source table is never written — and
/// leaves rows with an explicit provider, and every other column, untouched.
/// A legacy compound model prefix precedes the source default, matching
/// session reads. Without either, the import side applies its legacy
/// resolution. Returns the number of rows pinned.
pub(crate) fn pin_source_selection(
    rows: &mut [(String, Vec<Value>)],
    source_default_provider: Option<&str>,
) -> usize {
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
            let (_, legacy_provider) = normalize_compound_model(column_str(map, "model"), None);
            let Some(provider) = legacy_provider
                .as_deref()
                .or(source_default_provider)
                .filter(|p| !p.is_empty())
            else {
                continue;
            };
            map.insert(
                "provider".into(),
                Value::String(canonical_source_provider(provider).to_string()),
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
    /// A provider-less legacy row names its provider in the model prefix.
    RecoveredFromModelPrefix(String),
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
    let (model, legacy_provider) = normalize_compound_model(column_str(map, "model"), None);
    if let Some(provider) = legacy_provider {
        let provider = canonical_source_provider(&provider).to_string();
        map.insert("provider".into(), Value::String(provider.clone()));
        return ImportedSelection::RecoveredFromModelPrefix(provider);
    }
    let effort = column_str(map, "reasoning_effort");
    if model.is_none() && effort.is_none() {
        return ImportedSelection::Kept;
    }
    // Evidence: the last committed turn names a provider AND ran exactly the
    // stored model (both NULL = the provider default ran, which also
    // matches). A turn that ran a different model says nothing about which
    // provider the current selection was made against.
    let (last_turn_model, last_turn_provider) = normalize_compound_model(
        column_str(map, "last_turn_model"),
        column_str(map, "last_turn_provider"),
    );
    if let Some(provider) = last_turn_provider {
        if last_turn_model == model {
            let provider = canonical_source_provider(&provider).to_string();
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

    #[test]
    fn legacy_alias_export_pinning_uses_effective_source_identity() {
        for alias in ["acp", "augment", "default"] {
            let mut rows = session_rows(vec![serde_json::json!({
                "provider": null, "model": "gpt6-astra", "reasoning_effort": "low",
                "last_turn_provider": alias, "last_turn_model": "gpt6-astra"
            })]);
            assert_eq!(pin_source_selection(&mut rows, Some(alias)), 1);
            assert_eq!(rows[1].1[0]["provider"], "auggie");
            assert_eq!(rows[1].1[0]["last_turn_provider"], alias);
            let model = format!("{alias}:gpt6-astra");
            let mut prefix = session_rows(vec![serde_json::json!({
                "provider": null, "model": model, "reasoning_effort": "low"
            })]);
            assert_eq!(pin_source_selection(&mut prefix, Some("codex")), 1);
            assert_eq!(prefix[1].1[0]["provider"], "auggie");
            assert_eq!(prefix[1].1[0]["model"], model);
            let mut explicit = session_rows(vec![serde_json::json!({
                "provider": alias, "model": "gpt6-astra", "reasoning_effort": "low"
            })]);
            let original = explicit.clone();
            assert_eq!(pin_source_selection(&mut explicit, Some("codex")), 0);
            assert_eq!(explicit, original);
        }
    }

    #[test]
    fn legacy_prefix_pinning_uses_identity_before_source_default() {
        for default in [Some("codex"), None] {
            let mut rows = session_rows(vec![serde_json::json!({
                "provider": null, "model": "auggie:gpt6-astra", "reasoning_effort": "high"
            })]);
            assert_eq!(pin_source_selection(&mut rows, default), 1);
            assert_eq!(rows[1].1[0]["provider"], "auggie");
            assert_eq!(rows[1].1[0]["model"], "auggie:gpt6-astra");
            assert_eq!(rows[1].1[0]["reasoning_effort"], "high");
        }
    }

    #[test]
    fn legacy_prefix_history_recovers_effective_provider() {
        let mut row = serde_json::json!({
            "provider": null, "model": "gpt6-astra", "reasoning_effort": "high",
            "last_turn_provider": "codex", "last_turn_model": "auggie:gpt6-astra"
        });
        assert_eq!(
            resolve_imported_selection(row.as_object_mut().unwrap()),
            ImportedSelection::RecoveredFromLastTurn("auggie".into())
        );
        assert_eq!(row["provider"], "auggie");
        assert_eq!(row["model"], "gpt6-astra");
        assert_eq!(row["reasoning_effort"], "high");
        assert_eq!(row["last_turn_provider"], "codex");
        assert_eq!(row["last_turn_model"], "auggie:gpt6-astra");
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
