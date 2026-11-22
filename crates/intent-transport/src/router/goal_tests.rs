//! Conversation goal (PROTOCOL §5.5 "Conversation goal") at the RPC seam:
//! which `agent.sendMessage` / `agent.queueMessage` requests are `/goal`
//! commands, the attachment refusal, `agent.setGoal` params and the coded
//! error shapes. A recording stub stands in for the services layer.

use std::sync::Mutex;

use futures::future::BoxFuture;
use intent_core::conversation_goal::GoalError;
use intent_core::{AgentId, Error, MessageOrigin, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

#[derive(Default)]
struct Recording {
    calls: Mutex<Vec<Value>>,
}

impl Recording {
    fn take(&self) -> Vec<Value> {
        std::mem::take(&mut self.calls.lock().unwrap())
    }
}

impl WorkspaceApi for Recording {
    fn agent_goal_command(
        &self,
        workspace_id: Option<WorkspaceId>,
        agent_id: AgentId,
        statement: String,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.lock().unwrap().push(json!({
            "call": "goal", "workspaceId": workspace_id, "agentId": agent_id, "statement": statement,
        }));
        Box::pin(async {
            Ok(json!({ "success": true, "queued": false, "command": { "name": "goal" } }))
        })
    }

    fn agent_set_goal(
        &self,
        workspace_id: Option<WorkspaceId>,
        agent_id: AgentId,
        goal: Option<String>,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.lock().unwrap().push(json!({
            "call": "setGoal", "workspaceId": workspace_id, "agentId": agent_id, "goal": goal,
        }));
        Box::pin(async move {
            if goal.as_deref().is_some_and(|g| g.len() > 5) {
                return Err(Error::ConversationGoal(GoalError::TooLong));
            }
            if agent_id.0 == "agent-retired" {
                return Err(Error::ConversationGoal(GoalError::AgentRetired));
            }
            Ok(json!({ "action": "set", "goal": goal }))
        })
    }

    fn agent_send_message(
        &self,
        _workspace_id: WorkspaceId,
        _agent_id: AgentId,
        content: String,
        _message_id: Option<String>,
        _image_blocks: Option<Value>,
        _file_blocks: Option<Value>,
        _priority: Option<String>,
        _note_ids: Option<Value>,
        _stdin_context: Option<String>,
        _context_references: Option<Value>,
        _message_metadata: Option<Value>,
        _origin: MessageOrigin,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(json!({ "call": "send", "content": content }));
        Box::pin(async { Ok(json!({ "success": true, "messageId": "m-1" })) })
    }

    fn agent_queue_submission(
        &self,
        _agent_id: AgentId,
        _message_id: Option<String>,
        content: String,
        _image_blocks: Option<Value>,
        _file_blocks: Option<Value>,
        _message_metadata: Option<Value>,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(json!({ "call": "queue", "content": content }));
        Box::pin(async { Ok(json!({ "success": true, "queued": true })) })
    }
}

async fn rpc(api: &Recording, method: &str, params: Value) -> Value {
    let request = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let response = intent_core::with_caller(
        intent_core::Caller::Daemon,
        Box::pin(super::handle_message(api, &request.to_string())),
    )
    .await
    .expect("response");
    serde_json::from_str(&response).expect("json")
}

fn send_params(content: &str, extra: &Value) -> Value {
    let mut params = json!({ "workspaceId": "ws-1", "agentId": "agent-1", "content": content });
    params
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    params
}

#[tokio::test]
async fn only_interpreted_untyped_goal_sends_are_commands() {
    let api = Recording::default();
    for method in ["agent.sendMessage", "agent.queueMessage"] {
        // (content, extra params, expected statement when it is a command)
        let cases: Vec<(&str, Value, Option<&str>)> = vec![
            (
                "/goal Ship it",
                json!({ "interpretCommands": true }),
                Some("Ship it"),
            ),
            (
                "  /goal\n  a\nb  ",
                json!({ "interpretCommands": true }),
                Some("a\nb"),
            ),
            (
                "/goal",
                json!({ "interpretCommands": true, "messageMetadata": null }),
                Some(""),
            ),
            (
                "/goal x",
                json!({ "interpretCommands": true, "messageMetadata": { "mentionTokens": [] }, "imageBlocks": [], "priority": "interrupt" }),
                Some("x"),
            ),
            ("/goal x", json!({}), None),
            ("/goal x", json!({ "interpretCommands": false }), None),
            ("/goal x", json!({ "interpretCommands": "true" }), None),
            ("/Goal x", json!({ "interpretCommands": true }), None),
            ("/goals x", json!({ "interpretCommands": true }), None),
            ("/goal: x", json!({ "interpretCommands": true }), None),
            ("please /goal x", json!({ "interpretCommands": true }), None),
            (
                "/goal x",
                json!({ "interpretCommands": true, "messageMetadata": { "type": "question_answers" } }),
                None,
            ),
        ];
        for (content, extra, statement) in cases {
            let response = rpc(&api, method, send_params(content, &extra)).await;
            assert!(
                response.get("error").is_none(),
                "{method} {content:?} {extra}: {response}"
            );
            let calls = api.take();
            assert_eq!(calls.len(), 1, "{calls:?}");
            if let Some(statement) = statement {
                assert_eq!(calls[0]["call"], "goal", "{method} {content:?} {extra}");
                assert_eq!(calls[0]["statement"], statement);
                assert_eq!(calls[0]["agentId"], "agent-1");
                assert_eq!(calls[0]["workspaceId"], "ws-1");
                assert_eq!(response["result"]["command"]["name"], "goal");
            } else {
                assert_ne!(calls[0]["call"], "goal", "{method} {content:?} {extra}");
                assert_eq!(calls[0]["content"], content);
            }
        }
    }
}

#[tokio::test]
async fn a_goal_command_with_attachments_is_refused_before_any_call() {
    let api = Recording::default();
    for method in ["agent.sendMessage", "agent.queueMessage"] {
        for blocks in [
            json!({ "imageBlocks": [{ "type": "image", "data": "x", "mimeType": "image/png" }] }),
            json!({ "fileBlocks": [{ "name": "a.txt" }] }),
        ] {
            let mut extra = blocks.clone();
            extra["interpretCommands"] = json!(true);
            let response = rpc(&api, method, send_params("/goal x", &extra)).await;
            assert_eq!(
                response["error"],
                json!({
                    "code": -32602,
                    "message": "/goal does not take attachments",
                    "data": { "code": "goal-attachments-not-allowed" },
                }),
                "{method} {blocks}"
            );
            assert!(api.take().is_empty());
        }
    }
}

#[tokio::test]
async fn set_goal_params_and_coded_errors() {
    let api = Recording::default();
    let ok = rpc(
        &api,
        "agent.setGoal",
        json!({ "agentId": "agent-1", "goal": "Ship" }),
    )
    .await;
    assert_eq!(ok["result"], json!({ "action": "set", "goal": "Ship" }));
    assert_eq!(
        api.take(),
        vec![
            json!({ "call": "setGoal", "workspaceId": null, "agentId": "agent-1", "goal": "Ship" })
        ]
    );
    rpc(
        &api,
        "agent.setGoal",
        json!({ "agentId": "agent-1", "workspaceId": "ws-1", "goal": null }),
    )
    .await;
    assert_eq!(api.take()[0]["goal"], Value::Null);
    assert_eq!(
        rpc(
            &api,
            "agent.setGoal",
            json!({ "agentId": "agent-1", "workspaceId": "ws-1" })
        )
        .await["error"]["code"],
        -32602
    );
    assert_eq!(
        rpc(
            &api,
            "agent.setGoal",
            json!({ "agentId": "agent-1", "goal": 3 })
        )
        .await["error"]["code"],
        -32602
    );
    assert!(api.take().is_empty());

    let too_long = rpc(
        &api,
        "agent.setGoal",
        json!({ "agentId": "agent-1", "goal": "too long" }),
    )
    .await;
    assert_eq!(
        too_long["error"],
        json!({
            "code": -32602,
            "message": "goal is too long (max 1000 characters)",
            "data": { "code": "goal-too-long", "maxLength": 1000 },
        })
    );
    let retired = rpc(
        &api,
        "agent.setGoal",
        json!({ "agentId": "agent-retired", "goal": "x" }),
    )
    .await;
    assert_eq!(
        retired["error"],
        json!({ "code": -32602, "message": "agent is retired", "data": { "code": "agent-retired" } })
    );
}
