#![allow(dead_code)]

#[cfg(target_os = "linux")]
pub mod soft_key;

use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
};

use intent_passkey::authenticator::Authenticator;

/// A main-process stand-in talking to an in-process sidecar over a duplex pipe.
pub struct Client {
    writer: WriteHalf<DuplexStream>,
    lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
}

impl Client {
    pub fn start<A: Authenticator>(auth: A) -> Self {
        let (client, server) = tokio::io::duplex(1 << 16);
        let (server_read, server_write) = tokio::io::split(server);
        tokio::spawn(intent_passkey::server::serve(
            auth,
            server_read,
            server_write,
        ));
        let (read, writer) = tokio::io::split(client);
        Self {
            writer,
            lines: BufReader::new(read).lines(),
        }
    }

    pub async fn send_raw(&mut self, line: &str) {
        self.writer.write_all(line.as_bytes()).await.unwrap();
        self.writer.write_all(b"\n").await.unwrap();
    }

    pub async fn send(&mut self, id: &str, method: &str, params: Value) {
        self.send_raw(&json!({ "id": id, "method": method, "params": params }).to_string())
            .await;
    }

    pub async fn next(&mut self) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(60), self.lines.next_line())
            .await
            .expect("sidecar answered in time")
            .unwrap()
            .expect("sidecar kept stdout open");
        serde_json::from_str(&line).unwrap()
    }

    /// Reads lines until the response for `id`, returning it and the events before it;
    /// replies to other requests are skipped.
    pub async fn response(&mut self, id: &str) -> (Value, Vec<Value>) {
        let mut events = Vec::new();
        loop {
            let line = self.next().await;
            if line.get("event").is_some() {
                events.push(line);
            } else if line["id"] == id {
                return (line, events);
            }
        }
    }

    /// Reads lines until an event named `name`.
    pub async fn event(&mut self, name: &str) -> Value {
        loop {
            let line = self.next().await;
            if line["event"] == name {
                return line;
            }
            assert!(
                line.get("event").is_some(),
                "unexpected response while waiting for {name}: {line}"
            );
        }
    }

    pub async fn call(&mut self, id: &str, method: &str, params: Value) -> Value {
        self.send(id, method, params).await;
        self.response(id).await.0
    }
}

pub fn error_code(response: &Value) -> &str {
    response["error"]["code"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an error, got {response}"))
}

pub fn create_params(transport: &str, origin: &str, rp_id: &str) -> Value {
    json!({
        "transport": transport,
        "origin": origin,
        "crossOrigin": false,
        "rpId": rp_id,
        "challenge": "Y2hhbGxlbmdlLWNoYWxsZW5nZS1jaGFsbGVuZ2U",
        "rp": { "id": rp_id, "name": "Example" },
        "user": { "id": "dXNlci0x", "name": "ada@example.com", "displayName": "Ada" },
        "pubKeyCredParams": [{ "type": "public-key", "alg": -7 }],
        "excludeCredentials": [],
        "authenticatorSelection": { "residentKey": "required", "userVerification": "required" },
        "attestation": "none",
        "extensions": { "credProps": true },
        "timeoutMs": 60000,
    })
}

pub fn get_params(transport: &str, origin: &str, rp_id: &str) -> Value {
    json!({
        "transport": transport,
        "origin": origin,
        "crossOrigin": false,
        "rpId": rp_id,
        "challenge": "Z2V0LWNoYWxsZW5nZS1nZXQtY2hhbGxlbmdl",
        "allowCredentials": [],
        "userVerification": "required",
        "extensions": {},
        "timeoutMs": 60000,
    })
}
