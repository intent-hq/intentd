//! The Assistant reaches only backends an owner app reports. A guest's
//! connection can never report backends, so the daemon never relays an
//! Assistant call through a guest's app or to a guest-only session.
use super::*;

fn hello(client_id: &str) -> Value {
    json!({
        "clientId": client_id,
        "name": format!("Intent Desktop @ {client_id}"),
        "capabilities": { "backendRelay": 1 },
    })
}

fn report() -> Value {
    json!({ "backends": [{ "name": "Guest laptop", "host": "guest.example", "port": 8843,
        "fingerprint": "AA:BB" }] })
}

#[tokio::test]
async fn guest_connection_cannot_report_backends_for_the_assistant_over_wss() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, OWNER_FORGE).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let workspace = create_workspace(&mut owner, 700, "Shared with a guest").await;
    let (id, secret, _) = create_invite(&mut owner, 701, &workspace, json!({})).await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    let nonce = challenge_nonce(&mut join, 702, &id, &secret).await;
    script_proof(&mock, "gitlab", &nonce);
    let guest = result(
        &prove_gitlab(&mut join, 703, &id, &secret, &nonce, "901", GUEST_GL_LOGIN).await,
        703,
    );
    assert_eq!(guest["hostRole"], "guest", "{guest}");
    let token = guest["token"].as_str().unwrap();

    let mut device = connect_ws(host.port, host.cfg.clone(), token).await;
    let said = wss_rpc(&mut device, 704, "client.hello", hello("guest-app")).await;
    assert!(said.get("error").is_none(), "guest hello: {said}");
    let refused = wss_rpc(&mut device, 705, "backends.report", report()).await;
    assert_eq!(refused["error"]["code"], -32003, "guest report: {refused}");

    // Control: the owner's app on the same daemon may report.
    let said = wss_rpc(&mut owner, 706, "client.hello", hello("owner-app")).await;
    assert!(said.get("error").is_none(), "owner hello: {said}");
    let accepted = wss_rpc(&mut owner, 707, "backends.report", report()).await;
    assert!(accepted.get("error").is_none(), "owner report: {accepted}");
}
