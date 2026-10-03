//! Client operations shared by the invitation fixture and its liveness regressions.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::{
    tungstenite::{Error, Message},
    WebSocketStream,
};

pub(crate) type RawWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
/// Keep these fixture peers responsive while another peer waits on the invite
/// throttle. Application frames remain FIFO until the original RPC/event waiter
/// consumes them. This is deliberately invitation-specific: its finite scripted
/// traffic permits an unbounded receive queue, so a paused assertion cannot
/// backpressure the reader and prevent heartbeat replies.
pub(crate) struct Ws {
    commands: mpsc::UnboundedSender<(Message, oneshot::Sender<Result<(), Error>>)>,
    incoming: mpsc::UnboundedReceiver<Result<Message, Error>>,
    task: JoinHandle<()>,
}

impl Ws {
    pub(crate) async fn send(&mut self, message: Message) -> Result<(), Error> {
        let (done, result) = oneshot::channel();
        self.commands
            .send((message, done))
            .map_err(|_| Error::ConnectionClosed)?;
        result.await.unwrap_or(Err(Error::ConnectionClosed))
    }

    pub(crate) async fn next(&mut self) -> Option<Result<Message, Error>> {
        self.incoming.recv().await
    }
}

impl Drop for Ws {
    fn drop(&mut self) {
        // A JoinHandle otherwise detaches on drop. Abort releases the owned
        // socket even when the fixture unwinds or never consumes another frame.
        self.task.abort();
    }
}

pub(crate) fn from_raw(mut ws: RawWs) -> Ws {
    let (commands, mut outbound) =
        mpsc::unbounded_channel::<(Message, oneshot::Sender<Result<(), Error>>)>();
    let (incoming, receiver) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                command = outbound.recv() => {
                    let Some((message, done)) = command else { break };
                    let result = ws.send(message).await;
                    let failed = result.is_err();
                    let _ = done.send(result);
                    if failed { break; }
                }
                frame = ws.next() => {
                    match frame {
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(error) = ws.send(Message::Pong(payload)).await {
                                let _ = incoming.send(Err(error));
                                break;
                            }
                        }
                        Some(frame) => {
                            let failed = frame.is_err();
                            if incoming.send(frame).is_err() || failed { break; }
                            // A Close frame is forwarded intact. Keep polling so
                            // tungstenite flushes the close reply and reaches EOF.
                        }
                        None => break,
                    }
                }
            }
        }
    });
    Ws {
        commands,
        incoming: receiver,
        task,
    }
}

/// One WSS JSON-RPC round-trip returning the full envelope (so callers can
/// assert on `result` OR `error`). Out-of-band notifications are skipped.
pub(crate) async fn wss_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(30), ws.next())
            .await
            .unwrap_or_else(|_| panic!("wss rpc {method} timed out"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("{method}: expected text frame, got {other:?}"),
        }
    }
}

/// Pump a subscriber until a `workspace:updated` event whose `changes`
/// satisfy `pred` arrives (bounded).
pub(crate) async fn await_workspace_updated(
    ws: &mut Ws,
    what: &str,
    pred: impl Fn(&Value) -> bool,
) -> Value {
    eprintln!("await workspace:updated ({what})");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| panic!("timed out waiting for workspace:updated ({what})"));
        let next = timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for workspace:updated ({what})"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == json!("events.event")
                    && v["params"]["event"]["type"] == json!("workspace:updated")
                    && pred(&v["params"]["event"]["data"]["changes"])
                {
                    return v["params"]["event"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("workspace:updated ({what}): expected text frame, got {other:?}"),
        }
    }
}
