use super::*;
use intent_core::{BoxFuture, Result};

struct Api;
impl WorkspaceApi for Api {
    fn settings_get(&self, _: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(json!({"value":{}})) })
    }
}

async fn call(connection: &mut Connection, method: &str, params: Value, tcp: bool) -> Value {
    let request =
        classify(&json!({"jsonrpc":"2.0","id":7,"method":method,"params":params})).unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let frame = crate::context::with_connection_context(
        tcp,
        connection.handle(request, &Api, &ReverseChannel::new(tx)),
    )
    .await
    .unwrap();
    assert!(
        rx.try_recv().is_err(),
        "status/errors never issue browser requests"
    );
    serde_json::from_str(&frame).unwrap()
}

#[tokio::test]
async fn every_action_rejects_remote_and_non_app_connections_before_work() {
    for method in [
        "providers.setup.status",
        "providers.setup.start",
        "providers.setup.login",
        "providers.setup.cancel",
    ] {
        for (tcp, authorized) in [(true, true), (true, false), (false, false)] {
            let mut connection = Connection {
                authorized,
                operation: None,
            };
            let result = call(
                &mut connection,
                method,
                json!({"providerId":"antigravity"}),
                tcp,
            )
            .await;
            assert_eq!(result["id"], 7);
            assert_eq!(result["jsonrpc"], "2.0");
            assert_eq!(result["error"]["code"], -32001);
            assert!(connection.operation.is_none());
        }
    }
}

#[tokio::test]
async fn status_has_no_side_effects_and_operation_ids_cannot_cross_connections() {
    let mut connection = Connection {
        authorized: true,
        operation: None,
    };
    let result = call(
        &mut connection,
        "providers.setup.status",
        json!({"providerId":"antigravity"}),
        false,
    )
    .await;
    assert_eq!(result["result"]["phase"], "idle");
    assert!(result["result"]["operationId"].is_null());
    assert!(connection.operation.is_none());
    for action in ["providers.setup.login", "providers.setup.cancel"] {
        let result = call(
            &mut connection,
            action,
            json!({"providerId":"antigravity","operationId":"another-connection"}),
            false,
        )
        .await;
        assert_eq!(result["error"]["code"], -32602);
    }
}

#[tokio::test]
async fn notifications_do_not_start_setup() {
    let mut connection = Connection {
        authorized: true,
        operation: None,
    };
    let req=classify(&json!({"jsonrpc":"2.0","method":"providers.setup.start","params":{"providerId":"antigravity"}})).unwrap();
    let (tx, _) = tokio::sync::mpsc::channel(1);
    let result = crate::context::with_connection_context(
        false,
        connection.handle(req, &Api, &ReverseChannel::new(tx)),
    )
    .await;
    assert!(result.is_none());
    assert!(connection.operation.is_none());
}
