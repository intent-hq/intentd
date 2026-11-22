//! A local self-hosted DERP relay for tests: a DERP map served over HTTP
//! whose one node is a TCP listener, as the relay check in intentd's tunnel
//! supervisor expects (`server.tunnel.derpUrl`).

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A local relay: a DERP map at `url` whose one node is a TCP listener.
pub struct FakeRelay {
    pub url: String,
    pub map_hits: Arc<AtomicUsize>,
    pub node_hits: Arc<AtomicUsize>,
}

pub async fn fake_relay() -> FakeRelay {
    let node = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_port = node.local_addr().unwrap().port();
    let node_hits = Arc::new(AtomicUsize::new(0));
    let hits = node_hits.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = node.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });
    let map = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://127.0.0.1:{}/derpmap.json",
        map.local_addr().unwrap().port()
    );
    let body = json!({ "Regions": { "900": {
        "RegionID": 900, "RegionCode": "self", "RegionName": "Self-hosted",
        "Nodes": [{ "Name": "900a", "RegionID": 900, "HostName": "127.0.0.1", "DERPPort": node_port }],
    } } })
    .to_string();
    let map_hits = Arc::new(AtomicUsize::new(0));
    let hits = map_hits.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = map.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            let body = body.clone();
            tokio::spawn(async move {
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
            });
        }
    });
    FakeRelay {
        url,
        map_hits,
        node_hits,
    }
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
