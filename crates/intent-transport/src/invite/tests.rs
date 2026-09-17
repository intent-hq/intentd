//! Unit tests for the invite fast paths: link construction, classify, the
//! `/invite` dispatch and throttle, and the invite-error `data.code` mapping.

use std::sync::Arc;

use intent_core::{InviteErrorKind, WorkspaceApi};
use serde_json::{json, Value};

use super::*;

#[test]
fn invite_uri_carries_envelope_minus_token_plus_invite_fields() {
    let uri = build_invite_uri(
        &["192.168.1.10".to_string(), "10.0.0.5".to_string()],
        7443,
        "AB:CD",
        "inv-1",
        "s3cret",
        None,
    );
    assert_eq!(
        uri,
        "intent://invite?v=1&host=192.168.1.10,10.0.0.5&port=7443&fp=AB:CD&inviteId=inv-1&secret=s3cret"
    );
    assert!(!uri.contains("token="));
    let with_tc = build_invite_uri(&[], 7443, "AB", "i", "s", Some("tc-abc"));
    assert!(with_tc.ends_with("&tc=tc-abc"), "{with_tc}");
    let encoded = build_invite_uri(&[], 1, "AB", "a&b", "x=y", None);
    assert!(encoded.contains("inviteId=a%26b&secret=x%3Dy"), "{encoded}");
}

#[test]
fn classify_picks_the_invite_methods_only() {
    let create = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .expect("classified");
    assert_eq!(create.method, InviteMethod::Create);
    assert!(create.id_present);
    assert!(!create.method.on_invite_endpoint());
    let inspect =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.inspect" })).expect("classified");
    assert_eq!(inspect.method, InviteMethod::Inspect);
    assert!(!inspect.id_present);
    assert!(inspect.method.on_invite_endpoint());
    let accept = classify(&json!({ "jsonrpc": "2.0", "id": 3, "method": "invite.accept" }))
        .expect("classified");
    assert_eq!(accept.method, InviteMethod::Accept);
    assert!(accept.method.on_invite_endpoint());
    let challenge = classify(&json!({ "jsonrpc": "2.0", "id": 4, "method": "invite.challenge" }))
        .expect("classified");
    assert_eq!(challenge.method, InviteMethod::Challenge);
    assert!(challenge.method.on_invite_endpoint());
    let prove = classify(&json!({ "jsonrpc": "2.0", "id": 5, "method": "invite.prove" }))
        .expect("classified");
    assert_eq!(prove.method, InviteMethod::Prove);
    assert!(prove.method.on_invite_endpoint());
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.list" }))
            .is_none()
    );
    // The retired host-side device flow is not an invite method any more:
    // it falls through to the `-32001` refusal on `/invite`.
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 6, "method": "invite.redeem" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "invite.inspect" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": {}, "method": "invite.inspect" })).is_none());
}

/// Records which `/invite` service method ran; each answers its canned
/// payload for the `GOOD_*` inputs and a canned invite error otherwise.
struct RedeemStub {
    calls: std::sync::Mutex<Vec<String>>,
}

const GOOD_SECRET: &str = "good";

impl WorkspaceApi for RedeemStub {
    fn invite_inspect(
        &self,
        invite_id: String,
        secret: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("inspect:{invite_id}:{secret}"));
        Box::pin(async move {
            if secret == GOOD_SECRET {
                Ok(json!({ "workspaceId": "ws-1", "workspaceTitle": "Shared" }))
            } else {
                Err(intent_core::Error::Invite(InviteErrorKind::Revoked))
            }
        })
    }
    fn invite_accept(
        &self,
        invite_id: String,
        secret: String,
        credential: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("accept:{invite_id}:{secret}:{credential}"));
        Box::pin(async move {
            if credential == GOOD_CREDENTIAL {
                Ok(json!({
                    "status": "authorized",
                    "token": "fresh",
                    "principalId": "p-1",
                    "login": "guest",
                    "workspaceId": "ws-1",
                }))
            } else {
                Err(intent_core::Error::Invite(
                    InviteErrorKind::CredentialInvalid,
                ))
            }
        })
    }
    fn invite_challenge(
        &self,
        invite_id: String,
        secret: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("challenge:{invite_id}:{secret}"));
        Box::pin(async move {
            if secret == GOOD_SECRET {
                Ok(json!({
                    "workspaceId": "ws-1",
                    "workspaceTitle": "Shared",
                    "nonce": GOOD_NONCE,
                    "nonceExpiresAt": "2026-01-01T00:10:00Z",
                }))
            } else {
                Err(intent_core::Error::Invite(InviteErrorKind::Expired))
            }
        })
    }
    fn invite_prove(
        &self,
        invite_id: String,
        secret: String,
        nonce: String,
        gist_id: String,
        login: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.lock().unwrap().push(format!(
            "prove:{invite_id}:{secret}:{nonce}:{gist_id}:{login}"
        ));
        Box::pin(async move {
            match nonce.as_str() {
                GOOD_NONCE => Ok(json!({
                    "status": "authorized",
                    "token": "fresh",
                    "principalId": "p-1",
                    "login": "guest",
                    "workspaceId": "ws-1",
                })),
                "expired" => Err(intent_core::Error::Invite(InviteErrorKind::ProofExpired)),
                "down" => Err(intent_core::Error::Invite(
                    InviteErrorKind::GithubUnreachable,
                )),
                _ => Err(intent_core::Error::Invite(InviteErrorKind::ProofInvalid)),
            }
        })
    }
}

const GOOD_CREDENTIAL: &str = "cred";
const GOOD_NONCE: &str = "nonce-1";

/// `invite.challenge`: `{ inviteId, secret }` reaches the service as-is, the
/// result carries the inspect payload plus `nonce` / `nonceExpiresAt` and
/// the host identity decoration, an invite refusal maps to
/// `error.data.code`, and a missing param is `-32602` before any service
/// call.
#[tokio::test]
async fn challenge_decorates_like_an_inspect_and_maps_invite_errors() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "invite.challenge",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_challenge(req, &api).await.unwrap()).unwrap();
    let result = &frame["result"];
    assert_eq!(frame["id"], json!(1));
    assert_eq!(result["workspaceId"], json!("ws-1"), "{frame}");
    assert_eq!(result["workspaceTitle"], json!("Shared"));
    assert_eq!(result["nonce"], json!(GOOD_NONCE));
    assert_eq!(result["nonceExpiresAt"], json!("2026-01-01T00:10:00Z"));
    assert_eq!(result["hostname"], json!(crate::local_hostname()));
    assert_eq!(result["prettyHostname"], json!(crate::pretty_hostname()));
    assert!(result.get("flowId").is_none(), "no device flow: {frame}");
    assert!(result.get("userCode").is_none(), "no device flow: {frame}");

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 2, "method": "invite.challenge",
        "params": { "inviteId": "inv", "secret": "stale" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_challenge(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["data"]["code"], json!("invite-expired"));
    assert_eq!(frame["error"]["code"], json!(-32602));

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 3, "method": "invite.challenge",
        "params": { "secret": GOOD_SECRET }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_challenge(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32602));

    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec![
            format!("challenge:inv:{GOOD_SECRET}"),
            "challenge:inv:stale".to_string()
        ],
        "the missing-param request never reached the service"
    );
}

/// `invite.prove`: `{ inviteId, secret, nonce, gistId, login }` reaches the
/// service as-is and the `authorized` shape comes back undecorated;
/// the three proof refusals surface as `error.data.code` `proof-invalid` /
/// `proof-expired` / `github-unreachable`; a missing param is `-32602`
/// before any service call.
#[tokio::test]
async fn prove_returns_the_authorized_shape_and_maps_proof_errors() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let prove = |id: u64, nonce: &str| {
        classify(&json!({
            "jsonrpc": "2.0", "id": id, "method": "invite.prove",
            "params": {
                "inviteId": "inv", "secret": GOOD_SECRET, "nonce": nonce,
                "gistId": "abc123", "login": "guest",
            }
        }))
        .unwrap()
    };

    let frame: Value =
        serde_json::from_str(&handle_prove(prove(1, GOOD_NONCE), &api).await.unwrap()).unwrap();
    assert_eq!(
        frame["result"],
        json!({
            "status": "authorized",
            "token": "fresh",
            "principalId": "p-1",
            "login": "guest",
            "workspaceId": "ws-1",
        }),
        "{frame}"
    );
    for (nonce, code, rpc) in [
        ("bogus", "proof-invalid", -32602),
        ("expired", "proof-expired", -32602),
        (
            "down",
            "github-unreachable",
            intent_core::Error::Invite(InviteErrorKind::GithubUnreachable).code(),
        ),
    ] {
        let frame: Value =
            serde_json::from_str(&handle_prove(prove(2, nonce), &api).await.unwrap()).unwrap();
        assert_eq!(frame["error"]["data"]["code"], json!(code), "{frame}");
        assert_eq!(frame["error"]["code"], json!(rpc), "{frame}");
    }

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 3, "method": "invite.prove",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET, "nonce": GOOD_NONCE, "login": "guest" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_prove(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32602));
    assert!(
        frame["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("gistId")),
        "{frame}"
    );

    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec![
            format!("prove:inv:{GOOD_SECRET}:{GOOD_NONCE}:abc123:guest"),
            format!("prove:inv:{GOOD_SECRET}:bogus:abc123:guest"),
            format!("prove:inv:{GOOD_SECRET}:expired:abc123:guest"),
            format!("prove:inv:{GOOD_SECRET}:down:abc123:guest"),
        ]
    );
}

/// `invite.inspect`: `{ inviteId, secret }` reaches the service as-is, the
/// result carries the workspace hint plus the host identity (the same
/// decoration as a challenge), an invite refusal maps to `error.data.code`,
/// and a missing param is `-32602` before any service call.
#[tokio::test]
async fn inspect_decorates_like_a_challenge_and_maps_invite_errors() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "invite.inspect",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_inspect(req, &api).await.unwrap()).unwrap();
    let result = &frame["result"];
    assert_eq!(frame["id"], json!(1));
    assert_eq!(result["workspaceId"], json!("ws-1"), "{frame}");
    assert_eq!(result["workspaceTitle"], json!("Shared"));
    assert_eq!(result["hostname"], json!(crate::local_hostname()));
    assert_eq!(result["prettyHostname"], json!(crate::pretty_hostname()));
    assert!(result.get("flowId").is_none(), "no device flow: {frame}");
    assert!(result.get("userCode").is_none(), "no device flow: {frame}");

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 2, "method": "invite.inspect",
        "params": { "inviteId": "inv", "secret": "stale" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_inspect(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["data"]["code"], json!("invite-revoked"));
    assert_eq!(
        frame["error"]["code"],
        json!(intent_core::Error::Invite(InviteErrorKind::Revoked).code())
    );

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 3, "method": "invite.inspect",
        "params": { "inviteId": "inv" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_inspect(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32602));

    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec![
            format!("inspect:inv:{GOOD_SECRET}"),
            "inspect:inv:stale".to_string()
        ],
        "the missing-param request never reached the service"
    );
}

/// `invite.accept`: `{ inviteId, secret, credential }` reaches the service
/// as-is and the `authorized` shape comes back undecorated (no host
/// identity); an unknown credential is `credential-invalid`; a missing
/// `credential` is `-32602` before any service call.
#[tokio::test]
async fn accept_returns_the_authorized_shape_and_maps_credential_invalid() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "invite.accept",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET, "credential": GOOD_CREDENTIAL }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_accept(req, &api).await.unwrap()).unwrap();
    assert_eq!(
        frame["result"],
        json!({
            "status": "authorized",
            "token": "fresh",
            "principalId": "p-1",
            "login": "guest",
            "workspaceId": "ws-1",
        }),
        "{frame}"
    );

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 2, "method": "invite.accept",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET, "credential": "revoked" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_accept(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["data"]["code"], json!("credential-invalid"));
    assert_eq!(frame["error"]["code"], json!(-32602));

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 3, "method": "invite.accept",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_accept(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32602));
    assert!(
        frame["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("credential")),
        "{frame}"
    );

    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec![
            format!("accept:inv:{GOOD_SECRET}:{GOOD_CREDENTIAL}"),
            format!("accept:inv:{GOOD_SECRET}:revoked"),
        ]
    );
}

/// The `/invite` dispatcher routes every endpoint method to its handler and
/// answers `workspace.invite.create` — never served there — with the same
/// `-32001` as any non-invite method.
#[tokio::test]
async fn invite_endpoint_dispatch_routes_by_method() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    for (method, params) in [
        (
            "invite.inspect",
            json!({ "inviteId": "inv", "secret": GOOD_SECRET }),
        ),
        (
            "invite.accept",
            json!({ "inviteId": "inv", "secret": GOOD_SECRET, "credential": GOOD_CREDENTIAL }),
        ),
        (
            "invite.challenge",
            json!({ "inviteId": "inv", "secret": GOOD_SECRET }),
        ),
        (
            "invite.prove",
            json!({
                "inviteId": "inv", "secret": GOOD_SECRET, "nonce": GOOD_NONCE,
                "gistId": "abc123", "login": "guest",
            }),
        ),
    ] {
        let req =
            classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
                .unwrap();
        let frame: Value =
            serde_json::from_str(&handle_invite_endpoint(req, &api).await.unwrap()).unwrap();
        assert!(frame.get("result").is_some(), "{method}: {frame}");
    }
    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec![
            format!("inspect:inv:{GOOD_SECRET}"),
            format!("accept:inv:{GOOD_SECRET}:{GOOD_CREDENTIAL}"),
            format!("challenge:inv:{GOOD_SECRET}"),
            format!("prove:inv:{GOOD_SECRET}:{GOOD_NONCE}:abc123:guest"),
        ]
    );
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 4, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .unwrap();
    let frame: Value =
        serde_json::from_str(&handle_invite_endpoint(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32001));
    assert_eq!(
        frame["error"]["message"],
        json!(INVITE_ENDPOINT_ONLY_MESSAGE)
    );
    let note = classify(&json!({ "jsonrpc": "2.0", "method": "workspace.invite.create" })).unwrap();
    assert!(handle_invite_endpoint(note, &api).await.is_none());
}

/// A `/invite` notification (no id) is handled without producing a frame.
#[tokio::test]
async fn invite_endpoint_notification_yields_no_frame() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let req = classify(&json!({
        "jsonrpc": "2.0", "method": "invite.challenge",
        "params": { "inviteId": "inv", "secret": GOOD_SECRET }
    }))
    .unwrap();
    assert!(
        handle_invite_endpoint(req, &api).await.is_none(),
        "notification: no frame"
    );
    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec![format!("challenge:inv:{GOOD_SECRET}")]
    );
}

#[tokio::test]
async fn create_without_a_pairing_provider_is_unsupported_and_mints_nothing() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .unwrap();
    let frame: Value =
        serde_json::from_str(&handle_create(req, &api, None).await.unwrap()).unwrap();
    assert_eq!(
        frame["error"]["code"],
        json!(intent_core::Error::Unsupported(String::new()).code())
    );
    assert!(stub.calls.lock().unwrap().is_empty());
}

/// Minimal pairing provider for the resolver: a fixed port / tunnel address
/// over a fresh data dir (the TLS certificate is generated on first use).
struct StubPairingInfo {
    port: Option<u16>,
    tc_address: Option<String>,
    data_dir: std::path::PathBuf,
    token_store: crate::AsyncTokenStore,
}

struct NoToken;

impl crate::auth::TokenStore for NoToken {
    fn load_token(&self) -> Option<String> {
        None
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

impl crate::server::ServerPairingInfo for StubPairingInfo {
    fn pairing_snapshot(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::server::PairingSnapshot> + Send + '_>,
    > {
        let port = self.port;
        let tc_address = self.tc_address.clone();
        Box::pin(async move {
            crate::server::PairingSnapshot {
                port,
                bind_addresses: Some(vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)]),
                tc_address,
            }
        })
    }
    fn host_environment(&self) -> crate::host_env::HostEnvironment {
        crate::host_env::HostEnvironment {
            hostname: "test".to_string(),
            pretty_hostname: "test".to_string(),
            device_kind: None,
            hardware_model: None,
        }
    }
    fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
    fn token_store(&self) -> &crate::AsyncTokenStore {
        &self.token_store
    }
}

fn stub_provider(
    port: Option<u16>,
    tc_address: Option<&str>,
) -> (Arc<dyn crate::server::ServerPairingInfo>, tempfile::TempDir) {
    let mut tmpdir = tempfile::Builder::new()
        .prefix("intentd-test-invite-links-")
        .tempdir()
        .expect("create test temp dir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        tmpdir.disable_cleanup(true);
    }
    let provider = Arc::new(StubPairingInfo {
        port,
        tc_address: tc_address.map(str::to_string),
        data_dir: tmpdir.path().to_path_buf(),
        token_store: crate::AsyncTokenStore::new(Arc::new(NoToken)),
    });
    (provider, tmpdir)
}

/// The services-facing resolver rebuilds exactly the link `create` mints —
/// same envelope, same formatter — and answers `None` rather than an error
/// when the listener is down or nothing is dialable (loopback bind, no
/// tunnel), so `workspace.invite.list` never fails for it.
#[tokio::test]
async fn link_resolver_rebuilds_the_minted_link_and_is_none_when_undialable() {
    let (provider, _dir) = stub_provider(Some(7443), Some("tc-abc"));
    let minted = link_envelope(Some(&provider)).await.expect("envelope");
    let resolved = InviteLinkResolver::new(provider.clone())
        .invite_link_envelope()
        .await
        .expect("resolves");
    let url = resolved.invite_url("inv-1", "s3cret");
    assert_eq!(url, minted.invite_url("inv-1", "s3cret"));
    assert_eq!(
        url,
        format!(
            "intent://invite?v=1&host=&port=7443&fp={}&inviteId=inv-1&secret=s3cret&tc=tc-abc",
            encode_query_value(&minted.fingerprint)
        )
    );

    let (down, _dir) = stub_provider(None, Some("tc-abc"));
    assert!(matches!(
        link_envelope(Some(&down)).await,
        Err(Error::ListenerDown)
    ));
    assert!(InviteLinkResolver::new(down)
        .invite_link_envelope()
        .await
        .is_none());

    let (undialable, _dir) = stub_provider(Some(7443), None);
    assert!(matches!(
        link_envelope(Some(&undialable)).await,
        Err(Error::Unsupported(_))
    ));
    assert!(InviteLinkResolver::new(undialable)
        .invite_link_envelope()
        .await
        .is_none());
}

/// Service half of `workspace.invite.create`: answers `{ invite, secret }`
/// with no `url` (the transport stamps it) and counts its calls.
struct CreateStub {
    calls: std::sync::atomic::AtomicUsize,
}

impl WorkspaceApi for CreateStub {
    fn workspace_invite_create(
        &self,
        workspace_id: WorkspaceId,
        _pin_login: Option<String>,
        _expires_in_secs: Option<u64>,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            Ok(json!({
                "invite": { "id": "inv-1", "workspaceId": workspace_id },
                "secret": "s3cret",
            }))
        })
    }
}

/// `create` resolves the envelope once and stamps the one link it formats
/// as both the top-level `url` and `invite.url`; when no link can be built
/// (listener down) the create is refused before the service mints anything,
/// so neither `url` can exist without the other.
#[tokio::test]
async fn create_stamps_the_same_link_as_url_and_invite_url() {
    let stub = Arc::new(CreateStub {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let create_req = || {
        classify(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
            "params": { "workspaceId": "ws" }
        }))
        .unwrap()
    };

    let (provider, _dir) = stub_provider(Some(7443), Some("tc-abc"));
    let frame: Value = serde_json::from_str(
        &handle_create(create_req(), &api, Some(&provider))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(frame.get("error").is_none(), "{frame}");
    let r = &frame["result"];
    let url = r["url"].as_str().expect("url");
    assert_eq!(r["invite"]["url"], json!(url), "{r}");
    assert_eq!(
        url,
        link_envelope(Some(&provider))
            .await
            .expect("envelope")
            .invite_url("inv-1", "s3cret")
    );
    assert_eq!(r["secret"], json!("s3cret"));
    assert_eq!(r["invite"]["id"], json!("inv-1"));
    assert_eq!(stub.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

    let (down, _dir) = stub_provider(None, Some("tc-abc"));
    let frame: Value = serde_json::from_str(
        &handle_create(create_req(), &api, Some(&down))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        frame["error"]["data"]["code"],
        json!("listener-down"),
        "{frame}"
    );
    assert!(frame.get("result").is_none(), "{frame}");
    assert_eq!(
        stub.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "nothing minted without an envelope"
    );
}

#[test]
fn non_invite_method_on_invite_endpoint_is_unauthorized() {
    let frame =
        refuse_non_invite(&json!({ "jsonrpc": "2.0", "id": 3, "method": "workspace.list" }))
            .expect("frame");
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["error"]["code"], json!(-32001));
    assert_eq!(v["error"]["message"], json!(INVITE_ENDPOINT_ONLY_MESSAGE));
    assert!(refuse_non_invite(&json!({ "jsonrpc": "2.0", "method": "workspace.list" })).is_none());
    // The retired `invite.redeem` is refused exactly like any other
    // non-invite method.
    let frame = refuse_non_invite(&json!({ "jsonrpc": "2.0", "id": 4, "method": "invite.redeem" }))
        .expect("frame");
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["error"]["code"], json!(-32001));
    assert_eq!(v["error"]["message"], json!(INVITE_ENDPOINT_ONLY_MESSAGE));
}

fn challenge_req(id: i64) -> InviteRequest {
    classify(&json!({
        "jsonrpc": "2.0", "id": id, "method": "invite.challenge",
        "params": { "inviteId": "inv", "secret": format!("guess-{id}") }
    }))
    .unwrap()
}

/// The throttle is a token bucket: the burst is admitted, the next serial
/// attempt is refused with `invite-flow-busy` until a whole refill interval
/// has elapsed, one token comes back per interval, and the bucket never
/// overfills past the burst.
#[test]
fn throttle_refuses_after_the_burst_until_refill() {
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    for i in 0..INVITE_START_BURST {
        assert!(
            admit_redeem(&challenge_req(i64::from(i)), &throttle, t0).is_ok(),
            "burst attempt {i} admitted"
        );
    }
    let refused = admit_redeem(&challenge_req(100), &throttle, t0).expect_err("burst exhausted");
    let v: Value = serde_json::from_str(&refused.expect("frame")).unwrap();
    assert_eq!(v["id"], json!(100));
    assert_eq!(v["error"]["data"]["code"], json!("invite-flow-busy"));
    let just_short = t0 + INVITE_START_REFILL.saturating_sub(Duration::from_millis(1));
    assert!(
        admit_redeem(&challenge_req(101), &throttle, just_short).is_err(),
        "still refused just before the refill interval"
    );
    let refilled = t0 + INVITE_START_REFILL;
    assert!(
        admit_redeem(&challenge_req(102), &throttle, refilled).is_ok(),
        "one token back after one interval"
    );
    assert!(
        admit_redeem(&challenge_req(103), &throttle, refilled).is_err(),
        "exactly one token per interval"
    );
    let much_later = t0 + INVITE_START_REFILL * (INVITE_START_BURST * 4);
    for i in 0..INVITE_START_BURST {
        assert!(
            admit_redeem(&challenge_req(200 + i64::from(i)), &throttle, much_later).is_ok(),
            "refilled burst attempt {i}"
        );
    }
    assert!(
        admit_redeem(&challenge_req(300), &throttle, much_later).is_err(),
        "never overfills past the burst"
    );
    // A throttled notification is dropped without a frame.
    let note =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.challenge", "params": { "inviteId": "inv", "secret": "s" } }))
            .unwrap();
    assert_eq!(admit_redeem(&note, &throttle, much_later), Err(None));
}

/// A throttled request reaches neither the store nor GitHub: the refusal is
/// produced before the handler runs, so the service sees no call.
#[tokio::test]
async fn throttled_requests_make_no_service_calls() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    for i in 0..INVITE_START_BURST {
        assert!(admit_redeem(&challenge_req(i64::from(i)), &throttle, t0).is_ok());
    }
    let mut refusals = 0;
    for i in 0..20 {
        match admit_redeem(&challenge_req(500 + i), &throttle, t0) {
            Ok(()) => {
                handle_challenge(challenge_req(500 + i), &api).await;
            }
            Err(_) => refusals += 1,
        }
    }
    assert_eq!(refusals, 20, "every post-burst request refused");
    assert!(
        stub.calls.lock().unwrap().is_empty(),
        "no store/upstream call while throttled: {:?}",
        stub.calls.lock().unwrap()
    );
    // Once a token is back, the admitted request does reach the service.
    let later = t0 + INVITE_START_REFILL;
    assert_eq!(admit_redeem(&challenge_req(9), &throttle, later), Ok(()));
    let frame: Value =
        serde_json::from_str(&handle_challenge(challenge_req(9), &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["data"]["code"], json!("invite-expired"));
    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec!["challenge:inv:guess-9".to_string()]
    );
}

/// `invite.inspect`, `invite.accept`, `invite.challenge` and `invite.prove`
/// all hash the secret against the store, so they draw from the same
/// listener-wide bucket: once the burst is spent they are refused
/// `invite-flow-busy` alike, and a mixed stream shares one budget.
#[test]
fn every_invite_method_shares_the_throttle() {
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    let inspect = |id: i64| {
        classify(&json!({
            "jsonrpc": "2.0", "id": id, "method": "invite.inspect",
            "params": { "inviteId": "inv", "secret": format!("guess-{id}") }
        }))
        .unwrap()
    };
    let accept = |id: i64| {
        classify(&json!({
            "jsonrpc": "2.0", "id": id, "method": "invite.accept",
            "params": { "inviteId": "inv", "secret": format!("guess-{id}"), "credential": "c" }
        }))
        .unwrap()
    };
    let challenge = |id: i64| {
        classify(&json!({
            "jsonrpc": "2.0", "id": id, "method": "invite.challenge",
            "params": { "inviteId": "inv", "secret": format!("guess-{id}") }
        }))
        .unwrap()
    };
    let prove = |id: i64| {
        classify(&json!({
            "jsonrpc": "2.0", "id": id, "method": "invite.prove",
            "params": {
                "inviteId": "inv", "secret": format!("guess-{id}"), "nonce": "n",
                "gistId": "g", "login": "guest",
            }
        }))
        .unwrap()
    };
    assert!(hashes_secret(&inspect(0)) && hashes_secret(&accept(0)));
    assert!(hashes_secret(&challenge(0)) && hashes_secret(&prove(0)));
    let mut admitted = 0;
    for i in 0..i64::from(INVITE_START_BURST) {
        let req = match i % 4 {
            0 => challenge(i),
            1 => inspect(i),
            2 => prove(i),
            _ => accept(i),
        };
        if admit_redeem(&req, &throttle, t0).is_ok() {
            admitted += 1;
        }
    }
    assert_eq!(admitted, INVITE_START_BURST, "the burst admits any mix");
    for req in [inspect(100), accept(101), challenge(103), prove(104)] {
        let refused = admit_redeem(&req, &throttle, t0).expect_err("bucket empty");
        let v: Value = serde_json::from_str(&refused.expect("frame")).unwrap();
        assert_eq!(v["error"]["data"]["code"], json!("invite-flow-busy"), "{v}");
    }
}
