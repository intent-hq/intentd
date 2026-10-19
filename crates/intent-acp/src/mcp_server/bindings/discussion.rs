//! Caller-bound explicit discussion context and replies; no subscription surface.
use super::{map_err, req_str};
use intent_core::{AgentId, Caller, WorkspaceApi, WorkspaceId};
use serde_json::Value;
use std::sync::Arc;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.discussion = {
        conversations: (options) => host({ method: 'discussion.conversations', args: options || {} }),
        list: (conversationId, options) => host({ method: 'discussion.list', args: { ...(options || {}), conversationId } }),
        send: (conversationId, body, options) => host({ method: 'discussion.send', args: { ...(options || {}), conversationId, body } }),
    };
";

fn optional_string(args: &Value, key: &str) -> Result<Option<String>, String> {
    args.get(key)
        .map(|v| {
            v.as_str()
                .filter(|v| !v.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("{key} must be a nonblank string"))
        })
        .transpose()
}
fn cursor(args: &Value, key: &str) -> Result<Option<i64>, String> {
    args.get(key)
        .map(|v| {
            v.as_i64()
                .ok_or_else(|| format!("{key} must be an integer"))
        })
        .transpose()
}

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    let agent = caller.ok_or("discussion tools require a bound agent")?;
    // Never accept author or workspace identity from JavaScript arguments.
    intent_core::with_caller(
        Caller::Agent {
            agent_id: agent.clone(),
        },
        async {
            let limit = match args.get("limit") {
                None => 50,
                Some(v) => u32::try_from(
                    v.as_u64()
                        .filter(|n| (1..=100).contains(n))
                        .ok_or("limit must be an integer from 1 to 100")?,
                )
                .map_err(|_| "invalid limit")?,
            };
            match method {
                "conversations" => api
                    .discussion_conversations(ws.clone(), optional_string(args, "beforeId")?, limit)
                    .await
                    .map_err(map_err),
                "list" => api
                    .discussion_list(
                        ws.clone(),
                        req_str(args, "conversationId")?,
                        cursor(args, "beforeSequence")?,
                        cursor(args, "afterSequence")?,
                        limit,
                    )
                    .await
                    .map_err(map_err),
                "send" => {
                    let recipients = args
                        .get("recipientAgentIds")
                        .map(|v| {
                            serde_json::from_value::<Vec<AgentId>>(v.clone()).map_err(|_| {
                                "recipientAgentIds must be an array of strings".to_string()
                            })
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let result = api
                        .discussion_send(
                            ws.clone(),
                            req_str(args, "messageId")?,
                            req_str(args, "body")?,
                            intent_core::discussion::DiscussionTarget {
                                conversation_id: Some(req_str(args, "conversationId")?),
                                recipient_agent_ids: recipients,
                                reply_to_message_id: optional_string(args, "replyToMessageId")?,
                                ..Default::default()
                            },
                        )
                        .await
                        .map_err(map_err)?;
                    serde_json::to_value(result).map_err(|e| e.to_string())
                }
                _ => Err(format!("host: unknown method discussion.{method}")),
            }
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Error, Result};
    use serde_json::json;

    struct BoundApi;
    impl WorkspaceApi for BoundApi {
        fn discussion_list(
            &self,
            ws: WorkspaceId,
            conversation: String,
            before: Option<i64>,
            after: Option<i64>,
            limit: u32,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                assert_eq!(
                    intent_core::current_caller(),
                    Some(Caller::Agent {
                        agent_id: AgentId::from("agent-bound")
                    })
                );
                assert_eq!(ws.as_str(), "workspace-bound");
                assert_eq!(conversation, "default");
                assert_eq!(before, None);
                assert_eq!(after, Some(2));
                assert_eq!(limit, 3);
                Ok(json!({"messages":[],"hasMore":false}))
            })
        }
        fn discussion_send(
            &self,
            ws: WorkspaceId,
            key: String,
            body: String,
            target: intent_core::discussion::DiscussionTarget,
        ) -> BoxFuture<'_, Result<intent_core::discussion::DiscussionSendResult>> {
            Box::pin(async move {
                assert_eq!(
                    intent_core::current_caller(),
                    Some(Caller::Agent {
                        agent_id: AgentId::from("agent-bound")
                    })
                );
                assert_eq!(ws.as_str(), "workspace-bound");
                assert_eq!(key, "retry");
                assert_eq!(body, "reply");
                assert_eq!(target.conversation_id.as_deref(), Some("default"));
                assert_eq!(
                    target.recipient_agent_ids,
                    vec![AgentId::from("agent-target")]
                );
                assert_eq!(target.recipient_principal_ids, None);
                Err(Error::NotFound("binding reached".into()))
            })
        }
    }

    #[tokio::test]
    async fn discussion_bindings_pin_caller_workspace_and_preserve_explicit_recipients() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(BoundApi);
        let ws = WorkspaceId::from("workspace-bound");
        let agent = AgentId::from("agent-bound");
        let args = json!({"conversationId":"default","workspaceId":"forged","agentId":"forged","author":{"agentId":"forged"},"afterSequence":2,"limit":3});
        assert!(dispatch(&api, &ws, Some(&agent), "list", &args)
            .await
            .is_ok());
        assert!(dispatch(&api, &ws, None, "list", &args)
            .await
            .unwrap_err()
            .contains("bound agent"));
        let args = json!({"conversationId":"default","workspaceId":"forged","agentId":"forged","author":{"agentId":"forged"},"messageId":"retry","body":"reply","recipientAgentIds":["agent-target"],"recipientPrincipalIds":["forged"]});
        assert!(dispatch(&api, &ws, Some(&agent), "send", &args)
            .await
            .unwrap_err()
            .contains("binding reached"));
        for value in [json!(null), json!("agent-target"), json!([1])] {
            let mut invalid = args.clone();
            invalid["recipientAgentIds"] = value;
            assert!(dispatch(&api, &ws, Some(&agent), "send", &invalid)
                .await
                .unwrap_err()
                .contains("array of strings"));
        }
    }
}
