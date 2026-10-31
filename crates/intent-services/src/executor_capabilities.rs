//! The provider half of this machine's executor capabilities report: which
//! providers a remote agent could launch here and their models, from the
//! same launch resolution and `models.list` catalog local agents use.

use intent_core::node_link::{ProviderCapability, ProviderStatus};
use intent_core::Error;
use serde_json::Value;

use crate::agent_manager::{resolve_provider_launch, ProviderLaunchSpec};
use crate::Services;

/// A provider's status from its launch resolution alone: `cli-missing`
/// when nothing would launch it, `prerequisite-missing` when its runtime
/// (npx) is missing, else `unknown` until a sign-in probe or a launch says
/// more.
pub(crate) fn launch_status(resolved: &crate::Result<ProviderLaunchSpec>) -> ProviderStatus {
    match resolved {
        Ok(spec)
            if spec.provider_binary.is_none()
                && spec.npx_fallback_binary.is_none()
                && spec.provider.id != "mock" =>
        {
            ProviderStatus::CliMissing
        }
        Ok(_) => ProviderStatus::Unknown,
        Err(Error::InvalidInput(_)) => ProviderStatus::PrerequisiteMissing,
        Err(_) => ProviderStatus::CliMissing,
    }
}

/// The model ids of a `models.list` response.
pub(crate) fn model_ids(response: &Value) -> Vec<String> {
    response
        .get("models")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            ["id", "modelId", "value"]
                .iter()
                .find_map(|key| model.get(*key).and_then(Value::as_str))
                .map(str::to_owned)
        })
        .collect()
}

impl Services {
    /// Every enabled provider this build knows, with its launch status and
    /// (for a launchable one) its models from `models.list`. Model
    /// discovery may run provider CLIs, so callers cache the result.
    pub async fn executor_provider_capabilities(&self) -> Vec<ProviderCapability> {
        let settings = self.effective_settings();
        let mut providers = Vec::new();
        for provider in intent_providers::ACP_PROVIDERS {
            if crate::agent_ops::provider_is_disabled(
                provider.id,
                settings.providers.enabled.as_ref(),
            ) {
                continue;
            }
            let visible = intent_providers::gated_reason_with_env(provider, &|var| {
                std::env::var_os(var).is_some()
            })
            .is_none();
            let status = launch_status(&resolve_provider_launch(provider.id, &settings));
            let (models, models_stale) = if status == ProviderStatus::Unknown {
                match self
                    .models_list_op(Some(provider.id.to_owned()), false)
                    .await
                {
                    Ok(response) => (
                        model_ids(&response),
                        response.get("stale").and_then(Value::as_bool) == Some(true),
                    ),
                    Err(_) => (Vec::new(), true),
                }
            } else {
                (Vec::new(), false)
            };
            providers.push(ProviderCapability {
                id: provider.id.to_owned(),
                visible,
                status,
                models,
                models_stale,
            });
        }
        providers
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::*;

    fn spec(binary: bool, npx: bool) -> ProviderLaunchSpec {
        ProviderLaunchSpec {
            provider: *intent_providers::find_provider("opencode").unwrap(),
            provider_binary: binary.then(|| "/bin/opencode".into()),
            npx_fallback_binary: npx.then(|| "/bin/npx".into()),
            npx_fallback_package: npx.then_some("pkg"),
            extra_env: BTreeMap::new(),
        }
    }

    #[test]
    fn launch_status_follows_what_a_launch_would_spawn() {
        assert_eq!(
            launch_status(&Ok(spec(true, false))),
            ProviderStatus::Unknown
        );
        assert_eq!(
            launch_status(&Ok(spec(false, true))),
            ProviderStatus::Unknown
        );
        assert_eq!(
            launch_status(&Ok(spec(false, false))),
            ProviderStatus::CliMissing
        );
        assert_eq!(
            launch_status(&Err(Error::InvalidInput("npx not found".into()))),
            ProviderStatus::PrerequisiteMissing
        );
        assert_eq!(
            launch_status(&Err(Error::NotFound("provider".into()))),
            ProviderStatus::CliMissing
        );
    }

    #[test]
    fn model_ids_reads_every_catalog_row_shape() {
        let response = json!({
            "models": [{ "id": "a" }, { "modelId": "b" }, { "value": "c" }, { "name": "no id" }]
        });
        assert_eq!(model_ids(&response), ["a", "b", "c"]);
        assert!(model_ids(&json!({ "models": [] })).is_empty());
        assert!(model_ids(&json!({})).is_empty());
    }
}
