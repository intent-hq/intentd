//! Routing metadata must stop at the transport boundary for raw integration requests.
use intent_core::{BoxFuture, Result, WorkspaceApi};
use serde_json::{json, Value};

struct RecordingApi;
impl WorkspaceApi for RecordingApi {
    fn linear_create_issue(&self, request: Value) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(request) })
    }
    fn linear_update_issue(&self, request: Value) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(request) })
    }
}

#[tokio::test]
async fn integration_context_strips_only_routing_metadata_from_raw_requests() {
    for (method, payload) in [
        (
            "linear.createIssue",
            json!({"title":"Title", "teamId":"team", "description":"workspaceId is ordinary text", "futureField":{"workspaceId":"nested-provider-data"}}),
        ),
        (
            "linear.updateIssue",
            json!({"issueId":"issue", "assigneeId":null, "labelIds":[], "futureField":true}),
        ),
    ] {
        for context in [None, Some("workspace-a"), Some("workspace-b")] {
            let mut params = payload.clone();
            if let Some(context) = context {
                params["workspaceId"] = json!(context);
            }
            let request = json!({"jsonrpc":"2.0","id":7,"method":method,"params":params});
            let response = super::handle_message(&RecordingApi, &request.to_string())
                .await
                .unwrap();
            let response: Value = serde_json::from_str(&response).unwrap();
            assert_eq!(
                response,
                json!({"jsonrpc":"2.0","id":7,"result":payload}),
                "{method}: {context:?}"
            );
        }
    }
}
