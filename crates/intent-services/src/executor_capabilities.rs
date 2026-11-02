//! The provider half of this machine's executor capabilities report: which
//! providers a remote agent could launch here, their models and their
//! default account's sign-in, from the same launch resolution, `models.list`
//! catalog and sign-in probes local agents use.

use std::collections::HashMap;
use std::time::Instant;

use intent_core::node_link::{ProviderCapability, ProviderStatus};
use intent_core::Error;
use serde_json::Value;

use crate::agent_manager::{resolve_provider_launch, ProviderLaunchSpec};
use crate::provider_auth::DefaultSignIn;
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

/// A launchable provider's reported status: its default account's sign-in,
/// except that a launch without a profile that failed to authenticate at
/// `auth_failed_at` reports `auth-failed` until a conclusive probe observed
/// after it. A provider that cannot launch keeps its launch status.
pub(crate) fn provider_status(
    launch: ProviderStatus,
    sign_in: DefaultSignIn,
    auth_failed_at: Option<Instant>,
) -> ProviderStatus {
    if launch != ProviderStatus::Unknown {
        return launch;
    }
    let current = match (auth_failed_at, sign_in.probed_at) {
        (None, _) => true,
        (Some(failed), Some(probed)) => probed > failed,
        (Some(_), None) => false,
    };
    match sign_in.signed_in {
        Some(true) if current => ProviderStatus::SignedIn,
        Some(false) if current => ProviderStatus::SignedOut,
        _ if auth_failed_at.is_some() => ProviderStatus::AuthFailed,
        _ => ProviderStatus::Unknown,
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
    /// Every enabled provider this build knows with its status from launch
    /// resolution alone: no sign-in probe and no model listing, so it is
    /// cheap. Models are empty and marked stale until a full report
    /// ([`Self::executor_provider_capabilities`]) replaces it.
    #[must_use]
    pub fn executor_provider_launch_statuses(&self) -> Vec<ProviderCapability> {
        let settings = self.effective_settings();
        intent_providers::ACP_PROVIDERS
            .iter()
            .filter(|provider| {
                !crate::agent_ops::provider_is_disabled(
                    provider.id,
                    settings.providers.enabled.as_ref(),
                )
            })
            .map(|provider| ProviderCapability {
                id: provider.id.to_owned(),
                visible: intent_providers::gated_reason_with_env(provider, &|var| {
                    std::env::var_os(var).is_some()
                })
                .is_none(),
                status: launch_status(&resolve_provider_launch(provider.id, &settings)),
                models: Vec::new(),
                models_stale: true,
            })
            .collect()
    }

    /// Every enabled provider this build knows, with its status and (for a
    /// launchable one) its models from `models.list`. A launchable
    /// provider's status is its default account's cached sign-in probe, or
    /// `auth-failed` when `auth_failures` holds a launch without a profile
    /// that failed to authenticate since (see [`provider_status`]). Model
    /// discovery and sign-in probes may run provider CLIs, so callers cache
    /// the result.
    pub async fn executor_provider_capabilities(
        &self,
        auth_failures: &HashMap<String, Instant>,
    ) -> Vec<ProviderCapability> {
        let settings = self.effective_settings();
        let mut launchable = Vec::new();
        for provider in intent_providers::ACP_PROVIDERS {
            if crate::agent_ops::provider_is_disabled(
                provider.id,
                settings.providers.enabled.as_ref(),
            ) {
                continue;
            }
            let status = launch_status(&resolve_provider_launch(provider.id, &settings));
            launchable.push((provider, status));
        }
        // Probes run in parallel and abort with this future.
        let mut probes = tokio::task::JoinSet::new();
        for (index, (provider, status)) in launchable.iter().enumerate() {
            if *status == ProviderStatus::Unknown {
                let id = provider.id;
                let override_path = settings.providers.paths.get(id).cloned();
                probes.spawn(async move {
                    (
                        index,
                        crate::provider_auth::default_sign_in(id, override_path).await,
                    )
                });
            }
        }
        let mut sign_ins = vec![DefaultSignIn::default(); launchable.len()];
        while let Some(joined) = probes.join_next().await {
            if let Ok((index, sign_in)) = joined {
                sign_ins[index] = sign_in;
            }
        }
        let mut providers = Vec::new();
        for ((provider, launch), sign_in) in launchable.into_iter().zip(sign_ins) {
            let visible = intent_providers::gated_reason_with_env(provider, &|var| {
                std::env::var_os(var).is_some()
            })
            .is_none();
            let status = provider_status(launch, sign_in, auth_failures.get(provider.id).copied());
            let (models, models_stale) = if launch == ProviderStatus::Unknown {
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

    fn probed(signed_in: Option<bool>, at: Instant) -> DefaultSignIn {
        DefaultSignIn {
            signed_in,
            probed_at: Some(at),
        }
    }

    #[test]
    fn provider_status_reports_the_default_accounts_sign_in() {
        let now = Instant::now();
        for (sign_in, expected) in [
            (probed(Some(true), now), ProviderStatus::SignedIn),
            (probed(Some(false), now), ProviderStatus::SignedOut),
            (probed(None, now), ProviderStatus::Unknown),
            (DefaultSignIn::default(), ProviderStatus::Unknown),
        ] {
            assert_eq!(
                provider_status(ProviderStatus::Unknown, sign_in, None),
                expected,
                "{sign_in:?}"
            );
        }
    }

    /// A launch auth failure wins until a conclusive probe observed after
    /// it; an older probe, an inconclusive one or an uncached one keeps it.
    #[test]
    fn a_launch_auth_failure_holds_until_a_newer_conclusive_probe() {
        let before = Instant::now();
        let failed = before + std::time::Duration::from_secs(1);
        let after = failed + std::time::Duration::from_secs(1);
        for (sign_in, expected) in [
            (probed(Some(true), before), ProviderStatus::AuthFailed),
            (probed(Some(false), before), ProviderStatus::AuthFailed),
            (probed(Some(true), failed), ProviderStatus::AuthFailed),
            (probed(None, after), ProviderStatus::AuthFailed),
            (
                DefaultSignIn {
                    signed_in: Some(true),
                    probed_at: None,
                },
                ProviderStatus::AuthFailed,
            ),
            (DefaultSignIn::default(), ProviderStatus::AuthFailed),
            (probed(Some(true), after), ProviderStatus::SignedIn),
            (probed(Some(false), after), ProviderStatus::SignedOut),
        ] {
            assert_eq!(
                provider_status(ProviderStatus::Unknown, sign_in, Some(failed)),
                expected,
                "{sign_in:?}"
            );
        }
    }

    /// The cheap provider list names every enabled provider with its launch
    /// status only: no models yet, marked stale.
    #[tokio::test]
    async fn launch_statuses_list_every_enabled_provider_without_models() {
        let dir = crate::test_support::test_tempdir("launch-statuses");
        let store = intent_store::Store::open(&dir.path().join("state.db"))
            .await
            .unwrap();
        let svc = Services::new(store);
        let providers = svc.executor_provider_launch_statuses();
        assert!(
            providers.iter().any(|p| p.id == "claude-code"),
            "{providers:?}"
        );
        for provider in &providers {
            assert!(provider.models.is_empty(), "{provider:?}");
            assert!(provider.models_stale, "{provider:?}");
            assert!(
                !matches!(
                    provider.status,
                    ProviderStatus::SignedIn
                        | ProviderStatus::SignedOut
                        | ProviderStatus::AuthFailed
                ),
                "no sign-in is probed: {provider:?}"
            );
        }
    }

    /// What the installation says applies to every launch, signed in or not.
    #[test]
    fn a_provider_that_cannot_launch_keeps_its_launch_status() {
        let now = Instant::now();
        for launch in [
            ProviderStatus::CliMissing,
            ProviderStatus::PrerequisiteMissing,
        ] {
            assert_eq!(
                provider_status(launch, probed(Some(true), now), Some(now)),
                launch
            );
        }
    }

    /// Each probe-able CLI's real output, run through its sign-in probe and
    /// reported as the provider's status.
    #[cfg(unix)]
    #[tokio::test]
    async fn sign_in_probe_fixtures_map_to_provider_status() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fixtures: &[(&str, &str, i32, ProviderStatus)] = &[
            (
                "claude-code",
                r#"{"loggedIn":true,"authMethod":"claude.ai","email":"a@example.com"}"#,
                0,
                ProviderStatus::SignedIn,
            ),
            (
                "claude-code",
                r#"{"loggedIn":false,"authMethod":"none"}"#,
                1,
                ProviderStatus::SignedOut,
            ),
            ("claude-code", "not json", 0, ProviderStatus::Unknown),
            (
                "codex",
                "Logged in using ChatGPT",
                0,
                ProviderStatus::SignedIn,
            ),
            ("codex", "Not logged in", 1, ProviderStatus::SignedOut),
            (
                "opencode",
                "anthropic/claude-sonnet-4-5",
                0,
                ProviderStatus::SignedIn,
            ),
            ("opencode", "", 0, ProviderStatus::SignedOut),
            (
                "opencode",
                "Error: no providers",
                1,
                ProviderStatus::SignedOut,
            ),
            ("auggie", "", 0, ProviderStatus::SignedIn),
            ("auggie", "", 1, ProviderStatus::SignedOut),
        ];
        for (index, (provider, stdout, exit, expected)) in fixtures.iter().enumerate() {
            let args = intent_providers::find_provider(provider)
                .unwrap()
                .auth_check_args
                .unwrap();
            let stub = dir.path().join(format!("{provider}-{index}"));
            std::fs::write(
                &stub,
                format!(
                    "#!/bin/sh\n[ \"$*\" = '{}' ] || exit 127\nprintf '%s\\n' '{stdout}'\nexit {exit}\n",
                    args.join(" ")
                ),
            )
            .unwrap();
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
            let probe =
                crate::provider_auth::check_provider_auth_cli(provider, stub.as_os_str(), args)
                    .await;
            let sign_in = DefaultSignIn {
                signed_in: probe.auth_status(),
                probed_at: Some(Instant::now()),
            };
            assert_eq!(
                provider_status(ProviderStatus::Unknown, sign_in, None),
                *expected,
                "{provider} {stdout:?} exit {exit}"
            );
        }
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
