//! Capability snapshots follow wire order independently of notification consumers.
use intent_acp::{Connection, ConnectionHooks};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

async fn send(peer: &mut DuplexStream, value: Value) {
    peer.write_all(format!("{value}\n").as_bytes())
        .await
        .unwrap();
}

#[tokio::test]
async fn session_config_options_follow_response_and_notification_order_without_router() {
    let (client_write, peer_read) = tokio::io::duplex(8192);
    let (mut peer_write, client_read) = tokio::io::duplex(8192);
    let conn = Connection::new(client_write, client_read, None, ConnectionHooks::default());
    let peer = tokio::spawn(async move {
        let mut requests = BufReader::new(peer_read).lines();
        let mut next = 0;
        while let Some(line) = requests.next_line().await.unwrap() {
            let req: Value = serde_json::from_str(&line).unwrap();
            let id = req["id"].clone();
            let response = match next {
                0 => {
                    json!({"sessionId":"first","configOptions":[{"id":"fast","currentValue":"on"}]})
                }
                1 => {
                    // An older notification must not overwrite a later response.
                    send(&mut peer_write, json!({"method":"session/update","params":{
                        "sessionId":"first","update":{"sessionUpdate":"config_option_update","configOptions":[]}
                    }})).await;
                    json!({"configOptions":[{"id":"fast","currentValue":"off"}]})
                }
                2 => {
                    // The newer notification replaces metadata even though this
                    // connection has no notification hook at all.
                    send(&mut peer_write, json!({"method":"session/update","params":{
                        "sessionId":"first","update":{"sessionUpdate":"config_option_update","configOptions":[]}
                    }})).await;
                    json!({})
                }
                3 => json!({"sessionId":"second","configOptions":[{"id":"fast"}]}),
                4 => {
                    for update in [
                        json!({"sessionUpdate":"config_option_update"}),
                        json!({"sessionUpdate":"config_option_update","configOptions":null}),
                        json!({"sessionUpdate":"agent_message_chunk","configOptions":[{"id":"wrong"}]}),
                    ] {
                        send(&mut peer_write, json!({"method":"session/update","params":{"sessionId":"first","update":update}})).await;
                    }
                    json!({})
                }
                _ => unreachable!(),
            };
            send(&mut peer_write, json!({"id":id,"result":response})).await;
            next += 1;
            if next == 5 {
                break;
            }
        }
    });
    conn.request("session/new", json!({})).await.unwrap();
    assert_eq!(
        conn.session_config_options("first"),
        Some(json!([{"id":"fast","currentValue":"on"}]))
    );
    conn.request(
        "session/set_config_option",
        json!({"sessionId":"first","configId":"fast","value":"off"}),
    )
    .await
    .unwrap();
    assert_eq!(
        conn.session_config_options("first"),
        Some(json!([{"id":"fast","currentValue":"off"}]))
    );
    conn.request("session/prompt", json!({"sessionId":"first"}))
        .await
        .unwrap();
    assert_eq!(conn.session_config_options("first"), Some(json!([])));
    conn.request("session/new", json!({})).await.unwrap();
    assert_eq!(
        conn.session_config_options("second"),
        Some(json!([{"id":"fast"}]))
    );
    conn.request("session/prompt", json!({"sessionId":"first"}))
        .await
        .unwrap();
    assert_eq!(conn.session_config_options("first"), Some(json!([])));
    assert_eq!(conn.session_config_options("unknown"), None);
    peer.await.unwrap();
}
