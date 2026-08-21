//! `ws.app.question.ask` binding — structured clarifying questions.
//!
//! DELIBERATE deviation from the other `app/*` submodules: there is NO
//! chief-workspace gate here — any TOP-LEVEL workspace agent may ask the
//! user structured questions mid-task. Sub-agents (a session with a
//! `parent_agent_id` or `is_background`) don't own a user-facing chat turn,
//! so their bridges prune this binding from the description and prelude and
//! the dispatch host denies their frames with a redirect to
//! `ws.agent.requestDiscussion` / `ws.agent.reportToParent` (see
//! `dispatch::SUB_AGENT_QUESTION_DENIED`) — the gate never reaches this
//! module. One question per `ask` call; the model
//! calls it once per question. Each call registers ONE turn attachment under
//! [`AttachmentPolicy::AtTurnEnd`], so every question asked during a turn
//! lands as a trailing `application/vnd.intent.question+json` resource block
//! on the turn's final assistant message. Answers come back as ordinary
//! plain-text user messages (`Q:`/`A:` pairs flattened by the FE) — there is
//! no daemon-side answer intake and no id correlation; the resource URI
//! reuses the minted attachment id (`intent-question://<attachmentId>`).
//!
//! Hard validation only (missing/empty `question`/`header`, missing
//! `options`, fewer than 2 options, an option with an empty `label`). The
//! ~4-questions-per-turn and 2–4-options guidance lives in the tool
//! description as advice to the model and is never enforced. One consequence
//! of the un-capped per-turn count: [`TurnAttachmentRegistry`] holds at most
//! `MAX_PER_AGENT` (32) attachments per agent and silently evicts the oldest
//! on overflow, so an agent that asks more than 32 questions in a single turn
//! still gets `{ ok: true }` for every call but the earliest questions are
//! dropped from the turn-end drain.

use std::sync::Arc;

use intent_core::{
    new_attachment_id, AgentId, AttachmentPolicy, TurnAttachment, TurnAttachmentRegistry,
    ATTACHMENT_ID_KEY,
};
use serde_json::{json, Map, Value};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.question = {
        // spec: { header, question, options: [{ label, description? }], explanation?, multiSelect? }
        ask: (spec) => host({ method: 'app.question.ask', args: { question: spec } }),
    };
";

/// MCP resource MIME type for questions (FE renders these as QuestionCards).
pub const QUESTION_RESOURCE_MIME_TYPE: &str = "application/vnd.intent.question+json";

/// Unlike the other `app/*` submodules there is no chief gate — see the
/// module docs. Registration needs the turn-attachment registry and the
/// calling agent, threaded in from the dispatch layer.
pub(crate) fn dispatch(
    registry: Option<&Arc<TurnAttachmentRegistry>>,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "ask" => ask(registry, caller, args),
        other => Err(format!("host: unknown method `app.question.{other}`")),
    }
}

/// `ws.app.question.ask(question)` — validate ONE question, register it as an
/// `AtTurnEnd` attachment, and confirm to the model that it will be shown to
/// the user when the turn ends.
fn ask(
    registry: Option<&Arc<TurnAttachmentRegistry>>,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let mut payload = validate_question(args.get("question").unwrap_or(&Value::Null))?;
    let (Some(registry), Some(caller)) = (registry, caller) else {
        return Err(
            "ws.app.question.ask is only available from a live agent turn (no turn-attachment \
             registry or caller agent is wired), so the question could not be queued"
                .to_string(),
        );
    };
    let attachment_id = new_attachment_id();
    payload.insert(
        ATTACHMENT_ID_KEY.to_string(),
        Value::String(attachment_id.clone()),
    );
    let name = payload
        .get("header")
        .and_then(Value::as_str)
        .expect("validate_question guarantees a non-empty `header`")
        .to_string();
    let text = serde_json::to_string(&Value::Object(payload))
        .map_err(|e| format!("failed to serialize question payload: {e}"))?;
    registry.register(
        caller,
        TurnAttachment {
            id: attachment_id.clone(),
            policy: AttachmentPolicy::AtTurnEnd,
            mime_type: QUESTION_RESOURCE_MIME_TYPE.to_string(),
            uri: format!("intent-question://{attachment_id}"),
            name,
            text,
        },
    );
    Ok(json!({
        "ok": true,
        "attachmentId": attachment_id,
        "message": "Question queued — it will be shown to the user when this turn ends. Ask any remaining questions now (one ask() call each), then finish the turn. The user's answers arrive as the next user message as plain-text Q:/A: pairs, with (skipped) for questions the user skipped."
    }))
}

/// Hard validation for one question. Returns the canonical payload object
/// (without `attachmentId` — the caller stamps the minted nonce). No upper
/// caps: any number of options ≥ 2 is accepted.
fn validate_question(question: &Value) -> Result<Map<String, Value>, String> {
    let Some(q) = question.as_object() else {
        return Err(
            "`question` is required and must be an object: { header, question, options, \
             explanation?, multiSelect? }"
                .to_string(),
        );
    };
    let text = req_trimmed(q, "question")?;
    let header = req_trimmed(q, "header")?;
    let options = q.get("options").and_then(Value::as_array).ok_or_else(|| {
        "`options` is required and must be an array of { label, description? }".to_string()
    })?;
    if options.len() < 2 {
        return Err(format!(
            "`options` must contain at least 2 options (got {}) — a free-form \"Other\" answer \
             is always offered to the user automatically and must not be listed as an option",
            options.len()
        ));
    }
    let mut out_options = Vec::with_capacity(options.len());
    for (i, option) in options.iter().enumerate() {
        let label = option
            .get("label")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .ok_or_else(|| {
                format!("`options[{i}].label` is required and must be a non-empty string")
            })?;
        let mut out = Map::new();
        out.insert("label".to_string(), Value::String(label.to_string()));
        if let Some(description) = opt_trimmed(option, "description") {
            out.insert("description".to_string(), Value::String(description));
        }
        out_options.push(Value::Object(out));
    }
    let mut payload = Map::new();
    payload.insert("header".to_string(), Value::String(header));
    payload.insert("question".to_string(), Value::String(text));
    if let Some(explanation) = opt_trimmed(question, "explanation") {
        payload.insert("explanation".to_string(), Value::String(explanation));
    }
    payload.insert("options".to_string(), Value::Array(out_options));
    payload.insert(
        "multiSelect".to_string(),
        Value::Bool(
            q.get("multiSelect")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
    );
    Ok(payload)
}

/// Required non-empty trimmed string field of the question object.
fn req_trimmed(q: &Map<String, Value>, key: &str) -> Result<String, String> {
    q.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("`{key}` is required and must be a non-empty string"))
}

/// Optional trimmed string field; empty/whitespace-only values are dropped.
fn opt_trimmed(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent() -> AgentId {
        AgentId::from_string("agent-test")
    }

    fn valid_question() -> Value {
        json!({
            "header": "Auth method",
            "question": "Which authentication method should the new endpoint use?",
            "options": [
                { "label": "OAuth", "description": "Standard OAuth 2.0 flow" },
                { "label": "API key", "description": "Static key in header" }
            ]
        })
    }

    fn ask_args(question: Value) -> Value {
        json!({ "question": question })
    }

    fn dispatch_ok(registry: &Arc<TurnAttachmentRegistry>, question: Value) -> Value {
        dispatch(Some(registry), Some(&agent()), "ask", &ask_args(question))
            .expect("ask should succeed")
    }

    fn queued_payload(registry: &Arc<TurnAttachmentRegistry>) -> (TurnAttachment, Value) {
        let mut drained = registry.finish_turn(&agent());
        assert_eq!(drained.len(), 1);
        let attachment = drained.remove(0);
        let payload: Value = serde_json::from_str(&attachment.text).expect("payload is JSON");
        (attachment, payload)
    }

    #[test]
    fn ask_registers_one_at_turn_end_attachment() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let result = dispatch_ok(&registry, valid_question());
        assert_eq!(result["ok"], true);
        let attachment_id = result["attachmentId"].as_str().expect("attachmentId");

        let (attachment, payload) = queued_payload(&registry);
        assert_eq!(attachment.policy, AttachmentPolicy::AtTurnEnd);
        assert_eq!(attachment.id, attachment_id);
        assert_eq!(attachment.mime_type, QUESTION_RESOURCE_MIME_TYPE);
        assert_eq!(attachment.uri, format!("intent-question://{attachment_id}"));
        assert_eq!(attachment.name, "Auth method");
        assert_eq!(payload[ATTACHMENT_ID_KEY], attachment_id);
        assert_eq!(payload["header"], "Auth method");
        assert_eq!(
            payload["question"],
            "Which authentication method should the new endpoint use?"
        );
        assert_eq!(payload["multiSelect"], false);
        assert_eq!(payload["options"][0]["label"], "OAuth");
        assert_eq!(payload["options"][1]["description"], "Static key in header");
        assert!(payload.get("explanation").is_none());
    }

    #[test]
    fn ask_carries_explanation_and_multi_select() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let mut q = valid_question();
        q["explanation"] = json!("  longer context  ");
        q["multiSelect"] = json!(true);
        dispatch_ok(&registry, q);
        let (_, payload) = queued_payload(&registry);
        assert_eq!(payload["explanation"], "longer context");
        assert_eq!(payload["multiSelect"], true);
    }

    #[test]
    fn multiple_asks_queue_multiple_attachments() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        // More than 4 questions is soft guidance only — never enforced.
        for i in 0..6 {
            let mut q = valid_question();
            q["header"] = json!(format!("Q{i}"));
            dispatch_ok(&registry, q);
        }
        let drained = registry.finish_turn(&agent());
        assert_eq!(drained.len(), 6);
        assert_eq!(drained[0].name, "Q0");
        assert_eq!(drained[5].name, "Q5");
    }

    #[test]
    fn ask_accepts_more_than_four_options() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let mut q = valid_question();
        q["options"] = json!((0..7)
            .map(|i| json!({ "label": format!("opt{i}") }))
            .collect::<Vec<_>>());
        dispatch_ok(&registry, q);
        let (_, payload) = queued_payload(&registry);
        assert_eq!(payload["options"].as_array().unwrap().len(), 7);
    }

    #[test]
    fn ask_rejects_missing_question_object() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let err = dispatch(Some(&registry), Some(&agent()), "ask", &json!({})).unwrap_err();
        assert!(err.contains("`question` is required"));
    }

    #[test]
    fn ask_rejects_empty_question_and_header() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        for key in ["question", "header"] {
            let mut q = valid_question();
            q[key] = json!("   ");
            let err = dispatch(Some(&registry), Some(&agent()), "ask", &ask_args(q)).unwrap_err();
            assert!(
                err.contains(&format!("`{key}` is required")),
                "expected `{key}` error, got: {err}"
            );
        }
        assert!(registry.finish_turn(&agent()).is_empty());
    }

    #[test]
    fn ask_rejects_missing_or_single_option() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let mut q = valid_question();
        q.as_object_mut().unwrap().remove("options");
        let err = dispatch(Some(&registry), Some(&agent()), "ask", &ask_args(q)).unwrap_err();
        assert!(err.contains("`options` is required"));

        let mut q = valid_question();
        q["options"] = json!([{ "label": "Only one" }]);
        let err = dispatch(Some(&registry), Some(&agent()), "ask", &ask_args(q)).unwrap_err();
        assert!(err.contains("at least 2 options"));
        assert!(registry.finish_turn(&agent()).is_empty());
    }

    #[test]
    fn ask_rejects_option_with_empty_label() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let mut q = valid_question();
        q["options"] = json!([{ "label": "OAuth" }, { "label": "  " }]);
        let err = dispatch(Some(&registry), Some(&agent()), "ask", &ask_args(q)).unwrap_err();
        assert!(err.contains("`options[1].label`"));
        assert!(registry.finish_turn(&agent()).is_empty());
    }

    #[test]
    fn ask_fails_without_registry_or_caller() {
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let args = ask_args(valid_question());
        assert!(dispatch(None, Some(&agent()), "ask", &args).is_err());
        assert!(dispatch(Some(&registry), None, "ask", &args).is_err());
        assert!(registry.finish_turn(&agent()).is_empty());
    }

    #[test]
    fn unknown_method_is_rejected() {
        let err = dispatch(None, None, "nope", &json!({})).unwrap_err();
        assert!(err.contains("unknown method `app.question.nope`"));
    }

    /// The one deliberate deviation from the rest of `ws.app.*`: routed
    /// through the real `app::try_dispatch`, a NON-chief agent's ask succeeds
    /// (no chief gate) while its other `app.*` calls stay gated.
    #[tokio::test]
    async fn non_chief_agent_can_ask_through_app_dispatch() {
        use intent_core::{WorkspaceApi, WorkspaceId};

        struct FakeApi;
        impl WorkspaceApi for FakeApi {}

        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi);
        let non_chief = WorkspaceId::from_string("amber-forest");
        let registry = Arc::new(TurnAttachmentRegistry::new());
        let caller = agent();

        let result = super::super::try_dispatch(
            &api,
            &non_chief,
            Some(&caller),
            Some(&registry),
            "question.ask",
            &ask_args(valid_question()),
        )
        .await
        .expect("ask should succeed")
        .expect("question.ask is a known subnamespace");
        assert_eq!(result["ok"], true);
        assert_eq!(registry.finish_turn(&caller).len(), 1);

        // The sibling namespaces remain chief-gated for the same caller.
        let err = super::super::try_dispatch(
            &api,
            &non_chief,
            Some(&caller),
            Some(&registry),
            "ui.targets",
            &json!({}),
        )
        .await
        .unwrap_err();
        assert!(err.contains("only available in the Chief of Staff workspace"));
    }
}
