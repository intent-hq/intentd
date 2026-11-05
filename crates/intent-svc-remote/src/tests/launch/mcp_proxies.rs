//! The user MCP servers of a remote agent (nodeProtocol 11): a launch on a
//! protocol 11 link names them (names and ids only); on an older link it
//! names none, and the agent's instructions say why.

use intent_core::node_link::{McpProxy, MCP_PROXY_PROTOCOL};
use intent_core::WorkspaceApi;
use serde_json::json;

use super::*;

impl Fixture {
    async fn mcp_launch(&self, link: &mut NodeEnd, agent: &AgentId) -> LaunchCommand {
        let _runtime = self.port().runtime(&self.services, agent).await.unwrap();
        link.expect(|frame| match frame {
            LinkFrame::Launch(command) => Some(command),
            _ => None,
        })
        .await
    }
}

#[tokio::test]
async fn a_protocol_11_launch_names_the_user_mcp_servers_and_an_older_link_says_why() {
    let (f, mut link, _machine) = fixture().await;
    WorkspaceApi::mcp_servers_create(
        &f.services,
        json!({
            "id": "srv-1", "name": "Echo", "transport": "stdio",
            "command": "intent-test-missing-mcp-server", "env": { "TOKEN": "s4-secret" },
            "enabled": true,
        }),
    )
    .await
    .unwrap();

    f.node
        .shared_ref()
        .set_protocol_for_test(MCP_PROXY_PROTOCOL);
    let (agent, _view) = f.placed().await;
    let launch = f.mcp_launch(&mut link, &agent).await;
    assert_eq!(
        launch.mcp_proxies,
        vec![McpProxy {
            name: "Echo".into(),
            server_id: "srv-1".into(),
        }]
    );
    let wire = serde_json::to_string(&launch).unwrap();
    assert!(!wire.contains("s4-secret"), "{wire}");
    assert!(!wire.contains("intent-test-missing-mcp-server"), "{wire}");
    assert!(f
        .port()
        .mcp_servers_notice(&f.services, &agent)
        .await
        .is_none());

    f.node
        .shared_ref()
        .set_protocol_for_test(MCP_PROXY_PROTOCOL - 1);
    let (older, _view) = f.placed().await;
    let launch = f.mcp_launch(&mut link, &older).await;
    assert!(launch.mcp_proxies.is_empty());
    assert_eq!(
        f.port()
            .mcp_servers_notice(&f.services, &older)
            .await
            .as_deref(),
        Some("Your MCP servers are not available on Test machine until Intent there is updated.")
    );
}
