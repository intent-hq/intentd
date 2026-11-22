//! A local self-hosted DERP relay for tests: a DERP map served over HTTP
//! whose one node answers HTTP, as the relay check in intentd's tunnel
//! supervisor probes it (`server.tunnel.derpUrl`).

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A local relay: a DERP map at `url` whose one node is a local listener.
pub struct FakeRelay {
    pub url: String,
    pub map_hits: Arc<AtomicUsize>,
    pub node_hits: Arc<AtomicUsize>,
}

/// A relay whose node answers the relay check's HTTP probe (the map marks
/// it plain HTTP, `InsecureForTests`).
pub async fn fake_relay() -> FakeRelay {
    relay(true).await
}

/// A relay whose node takes each connection and closes it without a byte,
/// as every port looks from inside an E2B sandbox: the relay check must
/// find it unreachable.
pub async fn closing_relay() -> FakeRelay {
    relay(false).await
}

async fn relay(answers: bool) -> FakeRelay {
    let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_port = node.local_addr().unwrap().port();
    let node_hits = Arc::new(AtomicUsize::new(0));
    let hits = node_hits.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = node.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            if answers {
                tokio::spawn(answer(stream, String::new()));
            }
        }
    });
    let map = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://127.0.0.1:{}/derpmap.json",
        map.local_addr().unwrap().port()
    );
    let body = json!({ "Regions": { "900": {
        "RegionID": 900, "RegionCode": "self", "RegionName": "Self-hosted",
        "Nodes": [{
            "Name": "900a", "RegionID": 900, "HostName": "127.0.0.1", "DERPPort": node_port,
            "InsecureForTests": true,
        }],
    } } })
    .to_string();
    let map_hits = Arc::new(AtomicUsize::new(0));
    let hits = map_hits.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = map.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(answer(stream, body.clone()));
        }
    });
    FakeRelay {
        url,
        map_hits,
        node_hits,
    }
}

/// Read one HTTP request from `stream` and answer 200 with `body`.
async fn answer(mut stream: tokio::net::TcpStream, body: String) {
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
        }
    }
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

/// A map URL whose server takes connections and never answers them, so a
/// relay check waits out its map timeout.
pub async fn silent_relay_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    format!("http://127.0.0.1:{port}/derpmap.json")
}

/// A map URL nothing answers.
pub async fn dead_relay_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("http://127.0.0.1:{port}/derpmap.json")
}
