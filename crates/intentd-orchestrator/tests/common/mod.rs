//! A client of the orchestrator's listeners, as the desktop and web clients
//! connect: WSS with the pinned certificate and a bearer token, or the local
//! socket with newline-delimited JSON-RPC.
#![allow(dead_code, reason = "each test binary uses part of it")]

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub type Wss = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

pub fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(30))
}

/// Accept only the certificate whose SHA-256 is `fingerprint` (colon-separated
/// upper-case hex, as the listener reports it).
#[derive(Debug)]
struct Pinned {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use sha2::Digest as _;
        let seen = sha2::Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if seen == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "fingerprint {seen} is not {}",
                self.fingerprint
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Open `/ws` on `port` over TLS pinned to `fingerprint`, with `token` (the
/// `?token=` form) when given. `Err` carries the refused upgrade.
pub async fn wss(
    port: u16,
    fingerprint: &str,
    token: Option<&str>,
) -> Result<Wss, tokio_tungstenite::tungstenite::Error> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned {
            fingerprint: fingerprint.to_owned(),
            provider,
        }))
        .with_no_client_auth();
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .unwrap();
    let url = match token {
        Some(token) => format!("wss://localhost:{port}/ws?token={token}"),
        None => format!("wss://localhost:{port}/ws"),
    };
    tokio_tungstenite::client_async(url, tls)
        .await
        .map(|(ws, _)| ws)
}

/// The HTTP status of a refused upgrade.
pub fn refused_status(error: &tokio_tungstenite::tungstenite::Error) -> Option<u16> {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => Some(response.status().as_u16()),
        _ => None,
    }
}

/// Call `method` over a WebSocket and return the whole response.
pub async fn ws_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send");
    loop {
        let next = tokio::time::timeout(wait(), ws.next())
            .await
            .unwrap_or_else(|_| panic!("{method} timed out"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["id"] == json!(id) {
                    return value;
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                let _ = ws.send(Message::Pong(payload)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("{method}: the connection ended: {other:?}"),
        }
    }
}

/// The connection gets a close frame or ends.
pub async fn ws_ends<S>(ws: &mut WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = tokio::time::timeout(wait(), ws.next())
            .await
            .expect("the connection was not closed");
        match next {
            None | Some(Err(_) | Ok(Message::Close(_))) => return,
            Some(Ok(_)) => {}
        }
    }
}

/// A local-socket client.
pub struct Uds(BufReader<UnixStream>);

impl Uds {
    pub async fn connect(path: &std::path::Path) -> Self {
        Self(BufReader::new(UnixStream::connect(path).await.unwrap()))
    }

    /// Call `method` and return the whole response.
    pub async fn rpc(&mut self, id: i64, method: &str, params: Value) -> Value {
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line = frame.to_string();
        line.push('\n');
        self.0.get_mut().write_all(line.as_bytes()).await.unwrap();
        loop {
            let mut reply = String::new();
            let read = tokio::time::timeout(wait(), self.0.read_line(&mut reply))
                .await
                .unwrap_or_else(|_| panic!("{method} timed out"))
                .unwrap();
            assert_ne!(read, 0, "{method}: the connection ended");
            let value: Value = serde_json::from_str(&reply).unwrap();
            if value["id"] == json!(id) {
                return value;
            }
        }
    }

    /// The socket is closed.
    pub async fn ends(&mut self) {
        let mut buf = [0u8; 4096];
        loop {
            let read = tokio::time::timeout(wait(), self.0.read(&mut buf))
                .await
                .expect("the connection was not closed");
            if matches!(read, Ok(0) | Err(_)) {
                return;
            }
        }
    }
}

/// `response`'s result, failing on an error response.
pub fn result(response: &Value) -> &Value {
    assert!(response.get("error").is_none(), "{response}");
    &response["result"]
}

/// Wait until `done`.
pub async fn until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(wait(), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}
