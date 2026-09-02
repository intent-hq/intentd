//! `host.providerTestPrompt` (§5.14): live end-to-end provider test prompt.
//!
//! Runs one ephemeral ACP completion ("say hello") against a provider's
//! adapter — the only conclusive auth check: some providers (claude-code)
//! serve local probes uncredentialed and only fail at `session/prompt`. The
//! spawn/launch plumbing is shared with `agent.completeOnce`
//! ([`crate::complete_ops::one_shot_launch`] over
//! [`crate::one_shot_acp::run_one_shot_acp`]); this module owns only the
//! structured result contract and the auth-verdict cache coupling.
//!
//! Unlike `completeOnce` (curated to `ACP_ONE_SHOT_PROVIDERS` because its
//! *answer* is the product), the test prompt covers the whole catalog:
//! every provider is driven through the same ACP adapter and launch args
//! (`build_provider_args`) that real agent sessions spawn, and only turn
//! *completion* matters — so any adapter a live session can use, the probe
//! can exercise. Providers outside the completeOnce set are intentionally
//! best-effort: a provider whose adapter cannot complete a bare one-shot
//! turn surfaces a structured failure, never a wire error. `unsloth` alone
//! opts out (`supports_test_prompt: false`) — its first prompt can trigger
//! a very long model download/load cycle.
//!
//! Result contract (never a wire error once the provider id is known):
//! success is `{ ok: true }`; failure is `{ ok: false, reason, message }`
//! with `reason` ∈ `"unsupported" | "not-installed" | "spawn-failed" |
//! "auth-required" | "busy" | "timeout" | "error"`. `busy` is pre-spawn
//! queueing pressure — the daemon-wide adapter bound never freed a slot, no
//! provider was ever launched, and the client can back off and retry — kept
//! distinct from `timeout` (the provider itself blew a setup/prompt budget),
//! mirroring `agent.completeOnce`'s `adapter-busy` split (monorepo#2062).
//! An auth-required failure demotes
//! the cached `host.providerAuthStatus` verdict to a hard `false`
//! ([`crate::provider_auth::demote_auth_verdict`]); a success promotes it to
//! `true` ([`crate::provider_auth::promote_auth_verdict`]) — a live answer
//! outranks any local probe in both directions.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};

use crate::one_shot_acp::{run_one_shot_acp, OneShotError};

/// The literal prompt the probe sends. The answer is never surfaced — any
/// successfully completed turn is a pass.
const TEST_PROMPT: &str = "say hello";

/// Overall `session/prompt` budget (also the adapter-slot queue budget, per
/// the one-shot contract). Sized to absorb a first-run npx package download
/// on top of a slow first reply; setup (`initialize` + `session/new`) keeps
/// the launch's own npx-aware staged budgets.
const TEST_PROMPT_TIMEOUT: Duration = Duration::from_secs(90);

/// The machine-readable failure `reason` for a one-shot error, per the wire
/// contract above. Pure — the verdict-cache side effects key off the
/// returned reason in [`provider_test_prompt`].
fn failure_reason(err: &OneShotError) -> &'static str {
    match err {
        OneShotError::Spawn(_) => "spawn-failed",
        OneShotError::QueueTimeout { .. } => "busy",
        OneShotError::SetupTimeout | OneShotError::PromptTimeout => "timeout",
        OneShotError::Rpc(rpc)
            if crate::provider_models::is_auth_required_error(rpc.code, &rpc.message) =>
        {
            "auth-required"
        }
        // `Empty` (turn completed, no assistant text) is unreachable here —
        // [`provider_test_prompt`] treats it as a pass before mapping — but
        // maps to a generic error defensively.
        OneShotError::Rpc(_)
        | OneShotError::Transport(_)
        | OneShotError::Exited(_)
        | OneShotError::Empty => "error",
    }
}

/// A structured failure result: `{ ok: false, reason, message }`.
fn failure(reason: &str, message: impl Into<String>) -> Value {
    json!({ "ok": false, "reason": reason, "message": message.into() })
}

/// `host.providerTestPrompt` (§5.14): run one live test prompt against
/// `provider_id`'s adapter and report the structured outcome. `model` is
/// applied exactly like `agent.completeOnce` applies it (launch args or
/// post-`session/new` config option, provider-dependent); `provider_paths`
/// carries the `providers.paths` overrides, threaded by the caller like the
/// discovery/auth-status surfaces (monorepo#1065).
///
/// # Errors
///
/// Returns an error string only when `provider_id` names an unknown provider
/// (the caller maps it to `-32602`). Every runtime failure is a structured
/// `{ ok: false, reason, message }` result, not a wire error.
pub async fn provider_test_prompt<S: std::hash::BuildHasher>(
    provider_id: &str,
    model: Option<&str>,
    provider_paths: &HashMap<String, String, S>,
) -> Result<Value, String> {
    let Some(provider) = intent_providers::find_provider(provider_id) else {
        return Err(format!("Unknown providerId: {provider_id}"));
    };
    if !provider.supports_test_prompt {
        return Ok(failure(
            "unsupported",
            format!("provider \"{provider_id}\" does not support the live test prompt"),
        ));
    }
    // Binary resolution mirrors `agent.completeOnce`: the `providers.paths`
    // override is keyed by the provider that OWNS the primary binary, and an
    // empty value counts as unset.
    let explicit_path = provider_paths
        .get(provider.primary_binary_provider_id())
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty());
    let resolved_bin = (provider.npx_only_package.is_none())
        .then(|| {
            intent_providers::find_provider_binary(
                provider.primary_binary_provider_id(),
                provider.command,
                explicit_path.as_deref(),
            )
        })
        .flatten();
    let npx = intent_providers::find_npx();
    let Some(cmd) = crate::complete_ops::one_shot_launch(provider, resolved_bin, npx, model) else {
        return Ok(failure(
            "not-installed",
            format!(
                "{provider_id}: no adapter could be resolved \
                 (binary not found and npx unavailable)"
            ),
        ));
    };
    // codex loads MCP servers from its inherited CODEX_HOME regardless of the
    // empty ACP `mcpServers` list; the probe child gets the same isolated
    // throwaway home the one-shot completion path uses — a test prompt must
    // never start user-configured MCP servers.
    let (cmd, _codex_home) = if provider_id == "codex" {
        match crate::provider_models::with_isolated_codex_home(cmd) {
            Ok((cmd, home)) => (cmd, Some(home)),
            Err(e) => {
                return Ok(failure(
                    "spawn-failed",
                    format!("codex: failed to create isolated CODEX_HOME: {e}"),
                ))
            }
        }
    } else {
        (cmd, None)
    };
    let outcome = run_one_shot_acp(
        cmd,
        TEST_PROMPT,
        crate::complete_ops::config_option_model(provider, model),
        TEST_PROMPT_TIMEOUT,
    )
    .await;
    Ok(match outcome {
        // Any successfully completed turn is a pass — including one that
        // streamed no assistant text (`Empty`): the adapter accepted the
        // prompt and finished the turn, which is the end-to-end signal the
        // probe exists to observe. The answer itself is never surfaced.
        Ok(_) | Err(OneShotError::Empty) => {
            crate::provider_auth::promote_auth_verdict(provider_id);
            json!({ "ok": true })
        }
        Err(err) => {
            let reason = failure_reason(&err);
            if reason == "auth-required" {
                crate::provider_auth::demote_auth_verdict(provider_id);
            }
            failure(reason, err.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_acp::JsonRpcError;

    fn rpc(code: i64, message: &str) -> OneShotError {
        OneShotError::Rpc(JsonRpcError {
            code,
            message: message.to_string(),
            data: None,
        })
    }

    /// Every one-shot failure maps onto the wire `reason` vocabulary:
    /// auth-required detection rides the same code/message heuristic as the
    /// spawn-feedback seam (`is_auth_required_error`), pre-spawn queueing
    /// pressure is `busy` (distinct from the provider's own phase timeouts,
    /// mirroring completeOnce's adapter-busy split), and everything else is
    /// a generic error.
    #[test]
    fn failure_reason_covers_the_wire_vocabulary() {
        assert_eq!(
            failure_reason(&OneShotError::Spawn("enoent".into())),
            "spawn-failed"
        );
        assert_eq!(
            failure_reason(&OneShotError::QueueTimeout {
                waited_ms: 1,
                limit: 4
            }),
            "busy"
        );
        assert_eq!(failure_reason(&OneShotError::SetupTimeout), "timeout");
        assert_eq!(failure_reason(&OneShotError::PromptTimeout), "timeout");
        // The claude-code shape: -32000 with an auth-pattern message.
        assert_eq!(
            failure_reason(&rpc(-32000, "Authentication required")),
            "auth-required"
        );
        assert_eq!(failure_reason(&rpc(401, "nope")), "auth-required");
        assert_eq!(failure_reason(&rpc(-32603, "model exploded")), "error");
        assert_eq!(
            failure_reason(&OneShotError::Transport("pipe closed".into())),
            "error"
        );
        assert_eq!(
            failure_reason(&OneShotError::Exited("exit 9".into())),
            "error"
        );
    }

    /// unsloth opts out (`supports_test_prompt: false`): the RPC answers the
    /// structured `unsupported` result, never a wire error, and without
    /// resolving or spawning anything.
    #[tokio::test]
    async fn unsupported_provider_returns_structured_unsupported() {
        let paths: HashMap<String, String> = HashMap::new();
        let v = provider_test_prompt("unsloth", None, &paths).await.unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["reason"], "unsupported");
        assert!(v["message"].as_str().unwrap().contains("unsloth"));
    }

    /// An unknown provider id is the caller's `-32602`, mirroring
    /// `host.providerAuthStatus`.
    #[tokio::test]
    async fn unknown_provider_is_an_error() {
        let paths: HashMap<String, String> = HashMap::new();
        let err = provider_test_prompt("not-a-provider", None, &paths)
            .await
            .unwrap_err();
        assert!(err.contains("Unknown providerId"), "{err}");
    }
}
