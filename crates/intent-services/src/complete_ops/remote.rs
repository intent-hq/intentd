//! Quick actions across the executor link (nodeProtocol 18): an
//! orchestrator sends `agent.completeOnce` / `agent.enhancePrompt` to a
//! machine as one `oneShot` frame, and a machine runs a frame it received
//! with its own one-shot route (monorepo `docs/protocol/node-link.md`,
//! "Quick actions on the machine").

use std::sync::Arc;
use std::time::{Duration, Instant};

use intent_core::node_link::{
    EffortCandidates, EnhanceMode, LaunchProfileEnv, OneShot, OneShotCompleteInput,
    OneShotEnhanceInput, OneShotInput, OneShotOperation, OneShotOutcome, OneShotReply,
};
use intent_core::{Error, Result, WorkspaceId};
use serde_json::{json, Value};

use super::{unavailable, AttemptFailure, FailureKind, MIN_BACKUP_BUDGET};
use crate::feature_ports::{MachineOneShotAnswer, MachineOneShotRequest};
use crate::one_shot_acp::OneShotEffort;
use crate::provider_profiles::RemoteTarget;
use crate::Services;

/// Fixed `error.code` values of a `oneShotReply`.
mod codes {
    pub const GATE_CLOSED: &str = "gate-closed";
    pub const PROVIDER_DISABLED: &str = "provider-disabled";
    pub const CLI_MISSING: &str = "cli-missing";
    pub const NO_CAPACITY: &str = "no-capacity";
    pub const TIMEOUT: &str = "timeout";
    pub const ADAPTER_FAILED: &str = "adapter-failed";
    pub const AUTH_REQUIRED: &str = "auth-required";
    pub const QUOTA: &str = "quota";
    pub const EFFORT_UNSUPPORTED: &str = "effort-unsupported";
}

/// What a machine's reply means for the method.
enum Mapped {
    Done(Value),
    /// A quota failure: the backup rotation may try again.
    Quota(Error),
    Failed(Error),
}

fn map_reply(provider: &str, machine: &str, reply: OneShotReply) -> Mapped {
    let error = reply
        .error
        .unwrap_or_else(|| intent_core::node_link::OneShotFailure {
            code: codes::ADAPTER_FAILED.to_owned(),
            message: "the machine gave no reason".to_owned(),
        });
    match reply.outcome {
        OneShotOutcome::Ok => Mapped::Done(json!({ "text": reply.text.unwrap_or_default() })),
        OneShotOutcome::Unavailable => {
            Mapped::Done(unavailable(format!("{machine}: {}", error.message)))
        }
        OneShotOutcome::Cancelled => Mapped::Failed(Error::Internal(format!(
            "{provider}: the quick action was cancelled on {machine}"
        ))),
        OneShotOutcome::Failed => match error.code.as_str() {
            codes::QUOTA => {
                Mapped::Quota(Error::Internal(format!("{provider}: {}", error.message)))
            }
            codes::EFFORT_UNSUPPORTED => Mapped::Failed(Error::InvalidParams(error.message)),
            _ => Mapped::Failed(Error::Internal(format!("{provider}: {}", error.message))),
        },
    }
}

impl Services {
    /// `agent.completeOnce` on an orchestrator: one `oneShot` on the first
    /// machine that can run `selection`, with the backup rotation on that
    /// same machine for a quota failure, all within `timeout_ms`.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn complete_once_on_machine(
        &self,
        selection: &str,
        prompt: &str,
        system_prompt: Option<&str>,
        model: Option<&str>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: u64,
        effort: &OneShotEffort,
    ) -> Result<Value> {
        // What the local route decides before it spawns anything is decided
        // here too, before any machine is picked.
        if let Some(id) = &workspace_id {
            self.store.get_workspace(id).await?;
        }
        let base = intent_providers::base_provider_id(selection);
        if base != "auggie" && !super::ACP_ONE_SHOT_PROVIDERS.contains(&base) {
            return Ok(unavailable(format!(
                "completeOnce is not supported for the resolved provider: {base}"
            )));
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let home = crate::provider_profiles::canonical_selection(selection);
        let mut current = home.clone();
        let mut tried = Vec::new();
        let mut pinned: Option<String> = None;
        let mut last_quota: Option<Error> = None;
        loop {
            tried.push(current.clone());
            let plan = match self
                .remote_launch_plan(&current, RemoteTarget::Machine)
                .await
            {
                Ok(plan) => plan,
                // A backup that cannot run on a machine (an account folder)
                // is passed over; the first configuration's refusal stands.
                Err(error) => match last_quota.take() {
                    None => return Err(error),
                    Some(quota) => match self.quick_action_backup(&current, &home, Some(&tried)) {
                        Some(next) => {
                            last_quota = Some(quota);
                            current = next;
                            continue;
                        }
                        None => return Err(quota),
                    },
                },
            };
            let provider = intent_providers::base_provider_id(&current).to_owned();
            let remaining = deadline.saturating_duration_since(Instant::now());
            let frame = OneShot {
                request_id: uuid::Uuid::new_v4(),
                operation: OneShotOperation::Complete,
                input: OneShotInput::Complete(OneShotCompleteInput {
                    provider_id: provider.clone(),
                    prompt: prompt.to_owned(),
                    system_prompt: system_prompt.map(str::to_owned),
                    model: model.map(str::to_owned),
                    effort: (effort.explicit.is_some() || !effort.saved.is_empty()).then(|| {
                        EffortCandidates {
                            requested: effort.explicit.clone(),
                            saved: effort.saved.clone(),
                        }
                    }),
                }),
                profile_env: plan
                    .env
                    .as_ref()
                    .map(|env| LaunchProfileEnv(env.to_node_wire())),
                timeout_ms: u64::try_from(remaining.as_millis())
                    .unwrap_or(u64::MAX)
                    .max(1),
            };
            let answer = self
                .ports
                .remote_launch()
                .machine_one_shot(
                    self,
                    MachineOneShotRequest {
                        workspace_id: workspace_id.clone(),
                        provider: provider.clone(),
                        with_profile: plan.env.is_some(),
                        node_id: pinned.clone(),
                        frame,
                    },
                )
                .await;
            let (node_id, machine, reply) = match answer {
                MachineOneShotAnswer::NoMachine(reason) => return Ok(unavailable(reason)),
                MachineOneShotAnswer::Lost { machine } => {
                    return Ok(unavailable(format!(
                        "{machine} went away before it answered the quick action"
                    )))
                }
                MachineOneShotAnswer::TimedOut { timeout_ms, .. } => {
                    return Err(Error::Internal(format!(
                        "{provider}: timed out after {timeout_ms}ms"
                    )))
                }
                MachineOneShotAnswer::Replied {
                    node_id,
                    machine,
                    reply,
                } => (node_id, machine, reply),
            };
            let quota_error = match map_reply(&provider, &machine, reply) {
                Mapped::Done(value) => return Ok(value),
                Mapped::Failed(error) => return Err(error),
                Mapped::Quota(error) => error,
            };
            if deadline.saturating_duration_since(Instant::now()) < MIN_BACKUP_BUDGET {
                return Err(quota_error);
            }
            let Some(next) = self.quick_action_backup(&current, &home, Some(&tried)) else {
                return Err(quota_error);
            };
            tracing::info!(from_provider = %current, to_provider = %next, "quick action out of quota on its machine; retrying on a backup profile there");
            pinned = Some(node_id);
            last_quota = Some(quota_error);
            current = next;
        }
    }

    /// `agent.enhancePrompt` on an orchestrator: one `oneShot` (auggie) on
    /// the first machine that can run it.
    pub(crate) async fn enhance_prompt_on_machine(
        &self,
        prompt: String,
        mode: String,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> Result<Value> {
        let timeout = timeout_ms
            .unwrap_or(crate::enhance_ops::DEFAULT_TIMEOUT_MS)
            .min(crate::enhance_ops::MAX_TIMEOUT_MS);
        let frame = OneShot {
            request_id: uuid::Uuid::new_v4(),
            operation: OneShotOperation::Enhance,
            input: OneShotInput::Enhance(OneShotEnhanceInput {
                prompt: prompt.clone(),
                mode: if mode == "layout" {
                    EnhanceMode::Layout
                } else {
                    EnhanceMode::Enhance
                },
                model,
            }),
            profile_env: None,
            timeout_ms: timeout,
        };
        let answer = self
            .ports
            .remote_launch()
            .machine_one_shot(
                self,
                MachineOneShotRequest {
                    workspace_id,
                    provider: "auggie".to_owned(),
                    with_profile: false,
                    node_id: None,
                    frame,
                },
            )
            .await;
        match answer {
            MachineOneShotAnswer::NoMachine(reason) => {
                Ok(json!({ "available": false, "reason": reason }))
            }
            MachineOneShotAnswer::Lost { machine } => Ok(json!({
                "available": false,
                "reason": format!("{machine} went away before it answered the quick action"),
            })),
            MachineOneShotAnswer::TimedOut { timeout_ms, .. } => Err(Error::Internal(format!(
                "auggie: timed out after {timeout_ms}ms"
            ))),
            MachineOneShotAnswer::Replied { machine, reply, .. } => {
                match map_reply("auggie", &machine, reply) {
                    Mapped::Done(value) => match value.get("text").and_then(Value::as_str) {
                        Some(text) => {
                            Ok(json!({ "enhanced": text, "original": prompt, "mode": mode }))
                        }
                        None => Ok(value),
                    },
                    Mapped::Quota(error) | Mapped::Failed(error) => Err(error),
                }
            }
        }
    }

    /// Run a `oneShot` a backend sent this machine with its own one-shot
    /// route, and build the reply. `profile` is the frame's overlay, already
    /// read for this machine.
    pub async fn machine_one_shot(
        &self,
        frame: OneShot,
        profile: Option<intent_acp::ProfileEnv>,
    ) -> OneShotReply {
        let id = frame.request_id;
        let refuse = |outcome, code: &str, message: String| {
            OneShotReply::refused(id, outcome, code, message)
        };
        if !self.may_spawn_provider_locally() {
            return refuse(
                OneShotOutcome::Unavailable,
                codes::GATE_CLOSED,
                "quick actions do not run on this machine".to_owned(),
            );
        }
        let settings = self.effective_settings();
        let provider = match &frame.input {
            OneShotInput::Complete(input) => input.provider_id.clone(),
            OneShotInput::Enhance(_) => "auggie".to_owned(),
        };
        if let Err(error) = crate::agent_ops::ensure_provider_enabled(
            "a quick action",
            &provider,
            settings.providers.enabled.as_ref(),
        ) {
            return refuse(
                OneShotOutcome::Unavailable,
                codes::PROVIDER_DISABLED,
                error.to_string(),
            );
        }
        let timeout = frame.timeout_ms.clamp(1, super::MAX_TIMEOUT_MS);
        match frame.input {
            OneShotInput::Enhance(input) if frame.operation == OneShotOperation::Enhance => {
                let auggie = match self.one_shot_auggie_bin() {
                    Ok(bin) => bin,
                    Err(error) => {
                        return refuse(
                            OneShotOutcome::Unavailable,
                            codes::CLI_MISSING,
                            error.to_string(),
                        )
                    }
                };
                match crate::enhance_ops::enhance_with_auggie(
                    &auggie,
                    &input.prompt,
                    input.mode.as_str(),
                    input.model.as_deref(),
                    None,
                    timeout,
                )
                .await
                {
                    Ok(text) => OneShotReply {
                        request_id: id,
                        outcome: OneShotOutcome::Ok,
                        text: Some(text),
                        error: None,
                    },
                    Err(error) => refuse(
                        OneShotOutcome::Failed,
                        failure_code(&error),
                        bare_message("auggie", &error),
                    ),
                }
            }
            OneShotInput::Complete(input) if frame.operation == OneShotOperation::Complete => {
                let effort = input.effort.clone().unwrap_or_default();
                let result = if input.provider_id == "auggie" {
                    self.machine_auggie_completion(&input, effort.requested.as_deref(), timeout)
                        .await
                        .map_err(AttemptFailure::from)
                } else {
                    crate::acp_adapter::NEVER_QUEUE
                        .scope(
                            (),
                            self.complete_once_attempt(
                                &input.provider_id,
                                &input.prompt,
                                input.system_prompt.as_deref(),
                                input.model.as_deref(),
                                None,
                                timeout,
                                &OneShotEffort {
                                    explicit: effort.requested,
                                    saved: effort.saved,
                                },
                                profile.map(Arc::new),
                            ),
                        )
                        .await
                };
                match result {
                    Ok(value) => match value.get("text").and_then(Value::as_str) {
                        Some(text) => OneShotReply {
                            request_id: id,
                            outcome: OneShotOutcome::Ok,
                            text: Some(text.to_owned()),
                            error: None,
                        },
                        None => refuse(
                            OneShotOutcome::Unavailable,
                            if value["reason"]
                                .as_str()
                                .is_some_and(|r| r.contains("not supported"))
                            {
                                codes::GATE_CLOSED
                            } else {
                                codes::CLI_MISSING
                            },
                            value["reason"]
                                .as_str()
                                .unwrap_or("not available")
                                .to_owned(),
                        ),
                    },
                    Err(failure) if is_slot_refusal(&failure.error) => refuse(
                        OneShotOutcome::Unavailable,
                        codes::NO_CAPACITY,
                        "every adapter slot is busy (agents.maxConcurrentAdapters)".to_owned(),
                    ),
                    Err(failure) => {
                        let code = match failure.kind {
                            FailureKind::Quota => codes::QUOTA,
                            FailureKind::Timeout => codes::TIMEOUT,
                            FailureKind::Other => failure_code(&failure.error),
                        };
                        refuse(
                            OneShotOutcome::Failed,
                            code,
                            bare_message(&input.provider_id, &failure.error),
                        )
                    }
                }
            }
            _ => refuse(
                OneShotOutcome::Failed,
                codes::ADAPTER_FAILED,
                "the operation does not match its inputs".to_owned(),
            ),
        }
    }

    /// A `complete` frame for auggie: the `auggie --print` route.
    async fn machine_auggie_completion(
        &self,
        input: &OneShotCompleteInput,
        requested_effort: Option<&str>,
        timeout: u64,
    ) -> Result<Value> {
        if let Some(requested) = requested_effort {
            return Err(Error::InvalidParams(format!(
                "reasoningEffort {requested:?} is not supported by the auggie print path"
            )));
        }
        let auggie = self.one_shot_auggie_bin()?;
        let full_prompt = super::compose_prompt(&input.prompt, input.system_prompt.as_deref());
        let stdout = super::run_auggie_print(
            &auggie,
            input.model.as_deref(),
            None,
            &full_prompt,
            timeout,
            "One-shot completion",
        )
        .await?;
        Ok(json!({ "text": super::clean_agent_message(&stdout) }))
    }
}

/// A failure's message without the error variant's prefix ("internal
/// error: ") or the provider's ("codex: "): the orchestrator adds the
/// provider again, as the local route does.
fn bare_message(provider: &str, error: &Error) -> String {
    let text = match error {
        Error::Internal(message) | Error::InvalidParams(message) => message.clone(),
        other => other.to_string(),
    };
    text.strip_prefix(&format!("{provider}: "))
        .map_or_else(|| text.clone(), str::to_owned)
}

/// Whether `error` is the adapter bound refusing a slot (no free one now).
fn is_slot_refusal(error: &Error) -> bool {
    matches!(error, Error::AdapterBusy { .. })
}

/// The reply code of a failure that is not quota or a timeout.
fn failure_code(error: &Error) -> &'static str {
    let text = error.to_string().to_ascii_lowercase();
    if matches!(error, Error::InvalidParams(_)) && text.contains("reasoningeffort") {
        codes::EFFORT_UNSUPPORTED
    } else if text.contains("timed out") {
        codes::TIMEOUT
    } else if text.contains("auth") && (text.contains("login") || text.contains("required")) {
        codes::AUTH_REQUIRED
    } else {
        codes::ADAPTER_FAILED
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use intent_core::placement::{AgentPlacement, PlatformAvailability};
    use intent_core::remote_launch::{RemoteLaunchNeeds, RemoteLaunchRequest, RemotePlacementView};
    use intent_core::{AgentId, BoxFuture};
    use intent_store::Store;

    use super::*;
    use crate::feature_ports::{RemoteLaunchPort, RemoteRuntime};
    use crate::test_support::fixtures::TempDb;

    /// A port that answers every quick action with the next queued answer
    /// and records the requests.
    #[derive(Default)]
    struct Machines {
        answers: Mutex<Vec<MachineOneShotAnswer>>,
        requests: Mutex<Vec<MachineOneShotRequest>>,
    }

    impl RemoteLaunchPort for Machines {
        fn machine_one_shot<'a>(
            &'a self,
            _svc: &'a Services,
            request: MachineOneShotRequest,
        ) -> BoxFuture<'a, MachineOneShotAnswer> {
            self.requests.lock().unwrap().push(request);
            let answer = self.answers.lock().unwrap().remove(0);
            Box::pin(async move { answer })
        }
        fn placements<'a>(
            &'a self,
            _svc: &'a Services,
        ) -> BoxFuture<'a, Result<PlatformAvailability>> {
            Box::pin(async { Ok(PlatformAvailability::none(intent_core::now_iso())) })
        }
        fn placement_help<'a>(
            &'a self,
            _svc: &'a Services,
            _workspace_id: &'a WorkspaceId,
            _render: intent_core::machine_selection::PlacementHelpRenderer,
        ) -> BoxFuture<'a, Option<String>> {
            Box::pin(async { None })
        }
        fn admit<'a>(
            &'a self,
            _svc: &'a Services,
            _workspace_id: &'a WorkspaceId,
            placement: &'a AgentPlacement,
            _needs: &'a RemoteLaunchNeeds,
        ) -> BoxFuture<'a, Result<RemotePlacementView>> {
            let failure =
                PlatformAvailability::none(intent_core::now_iso()).unavailable(placement.clone());
            Box::pin(async move { Err(Error::PlacementUnavailable(Box::new(failure))) })
        }
        fn launch<'a>(
            &'a self,
            _svc: &'a Services,
            _r: RemoteLaunchRequest,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn runtime<'a>(
            &'a self,
            _svc: &'a Services,
            _agent_id: &'a AgentId,
        ) -> BoxFuture<'a, Result<Arc<RemoteRuntime>>> {
            Box::pin(async { Err(Error::Internal("no runtime".into())) })
        }
    }

    async fn orchestrator(answers: Vec<MachineOneShotAnswer>) -> (TempDb, Services, Arc<Machines>) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let registry = Arc::new(
            crate::SettingsRegistry::load(tmp.path.with_extension("config.toml"))
                .expect("registry"),
        );
        registry
            .apply(&[("model.defaultProvider".to_string(), json!("codex"))])
            .expect("set model.defaultProvider");
        let machines = Arc::new(Machines {
            answers: Mutex::new(answers),
            ..Default::default()
        });
        let services = Services::new_isolated(store)
            .with_settings_registry(registry)
            .with_orchestrator_mode(true)
            .with_remote_launch_port(machines.clone());
        (tmp, services, machines)
    }

    fn replied(
        outcome: OneShotOutcome,
        text: Option<&str>,
        code: Option<&str>,
    ) -> MachineOneShotAnswer {
        MachineOneShotAnswer::Replied {
            node_id: "node-1".into(),
            machine: "Mac Studio".into(),
            reply: OneShotReply {
                request_id: uuid::Uuid::nil(),
                outcome,
                text: text.map(str::to_owned),
                error: code.map(|code| intent_core::node_link::OneShotFailure {
                    code: code.to_owned(),
                    message: format!("{code} happened"),
                }),
            },
        }
    }

    async fn complete(services: &Services) -> Result<Value> {
        services
            .agent_complete_once_op("hi".into(), None, None, None, None, Some(5_000), None)
            .await
    }

    /// The completion runs on a machine, with the provider and no local
    /// process; the machine's text is the result.
    #[tokio::test]
    async fn a_quick_action_runs_on_a_machine() {
        let (_tmp, services, machines) =
            orchestrator(vec![replied(OneShotOutcome::Ok, Some("hello"), None)]).await;
        assert_eq!(
            complete(&services).await.unwrap(),
            json!({ "text": "hello" })
        );
        let requests = machines.requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("expected one request, got {requests:?}");
        };
        assert_eq!(request.provider, "codex");
        assert!(request.node_id.is_none());
        assert_eq!(request.frame.operation, OneShotOperation::Complete);
        let OneShotInput::Complete(input) = &request.frame.input else {
            panic!("expected a complete input");
        };
        assert_eq!(input.prompt, "hi");
        assert!(request.frame.profile_env.is_none());
    }

    /// No machine, a lost link and a machine's `unavailable` all answer with
    /// the gate shape, naming the cause; a failure is the local route's
    /// error.
    #[tokio::test]
    async fn machine_answers_map_to_the_methods_results() {
        let (_tmp, services, _machines) = orchestrator(vec![
            MachineOneShotAnswer::NoMachine("no machine for quick actions is online".into()),
            MachineOneShotAnswer::Lost {
                machine: "Mac Studio".into(),
            },
            replied(OneShotOutcome::Unavailable, None, Some("cli-missing")),
            replied(OneShotOutcome::Failed, None, Some("timeout")),
            replied(OneShotOutcome::Failed, None, Some("effort-unsupported")),
        ])
        .await;
        let none = complete(&services).await.unwrap();
        assert_eq!(none["available"], false);
        assert_eq!(none["reason"], "no machine for quick actions is online");
        let lost = complete(&services).await.unwrap();
        assert_eq!(lost["available"], false);
        assert!(lost["reason"]
            .as_str()
            .unwrap()
            .starts_with("Mac Studio went away"));
        let missing = complete(&services).await.unwrap();
        assert_eq!(missing["reason"], "Mac Studio: cli-missing happened");
        let timeout = complete(&services).await.unwrap_err();
        assert!(
            matches!(&timeout, Error::Internal(m) if m == "codex: timeout happened"),
            "{timeout:?}"
        );
        let effort = complete(&services).await.unwrap_err();
        assert!(matches!(effort, Error::InvalidParams(_)), "{effort:?}");
    }

    /// A machine handed a frame while its own gate is closed (it runs as an
    /// orchestrator itself) refuses without running anything.
    #[tokio::test]
    async fn an_orchestrator_refuses_a_frame_sent_to_it() {
        let (_tmp, services, _machines) = orchestrator(Vec::new()).await;
        let reply = services
            .machine_one_shot(
                OneShot {
                    request_id: uuid::Uuid::nil(),
                    operation: OneShotOperation::Complete,
                    input: OneShotInput::Complete(OneShotCompleteInput {
                        provider_id: "codex".into(),
                        prompt: "hi".into(),
                        system_prompt: None,
                        model: None,
                        effort: None,
                    }),
                    profile_env: None,
                    timeout_ms: 1_000,
                },
                None,
            )
            .await;
        assert_eq!(reply.outcome, OneShotOutcome::Unavailable);
        assert_eq!(reply.error.unwrap().code, codes::GATE_CLOSED);
    }

    /// A real frame through the machine's own route, then its reply through
    /// the orchestrator's mapping: the text comes back unchanged, and a
    /// failure's message carries one prefix, never two.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_machine_reply_maps_back_with_the_local_routes_shape() {
        let (_dir, bin) = super::super::tests::fake_acp_adapter("machine", "from the machine");
        let (_tmp, machine) = super::super::tests::services_with_settings(&[
            ("model.defaultProvider", json!("codex")),
            ("providers.paths", json!({ "codex": bin.to_string_lossy() })),
        ])
        .await;
        let machine = machine.with_one_shot_npx(Some(bin));
        let frame = |effort: Option<EffortCandidates>| OneShot {
            request_id: uuid::Uuid::new_v4(),
            operation: OneShotOperation::Complete,
            input: OneShotInput::Complete(OneShotCompleteInput {
                provider_id: "codex".into(),
                prompt: "make a slug".into(),
                system_prompt: None,
                model: None,
                effort,
            }),
            profile_env: None,
            timeout_ms: 30_000,
        };
        let ok = machine.machine_one_shot(frame(None), None).await;
        assert_eq!(ok.outcome, OneShotOutcome::Ok, "{ok:?}");
        let failed = machine
            .machine_one_shot(
                frame(Some(EffortCandidates {
                    requested: Some("ultra-max".into()),
                    saved: Vec::new(),
                })),
                None,
            )
            .await;
        assert_eq!(failed.outcome, OneShotOutcome::Failed, "{failed:?}");
        let message = failed.error.as_ref().unwrap().message.clone();
        assert!(!message.starts_with("invalid params"), "{message}");
        assert!(!message.starts_with("internal error"), "{message}");
        assert!(!message.starts_with("codex: "), "{message}");

        let answer = |reply| MachineOneShotAnswer::Replied {
            node_id: "node-1".into(),
            machine: "Mac Studio".into(),
            reply,
        };
        let (_t, orchestrator, _machines) = orchestrator(vec![answer(ok), answer(failed)]).await;
        assert_eq!(
            complete(&orchestrator).await.unwrap(),
            json!({ "text": "from the machine" })
        );
        let error = complete(&orchestrator).await.unwrap_err().to_string();
        for prefix in ["invalid params", "internal error", "codex: "] {
            assert!(
                error.matches(prefix).count() <= 1,
                "{prefix} repeated: {error}"
            );
        }
    }
}
