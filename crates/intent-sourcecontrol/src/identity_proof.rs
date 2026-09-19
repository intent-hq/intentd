//! Guest-side gist identity proof (multiplayer gist-proof join flow).
//!
//! A guest proves its GitHub identity to a workspace host from its *own*
//! daemon: the host hands out a nonce, the guest publishes it in a **secret
//! gist** with its own token, and the host reads the gist back to verify the
//! owner. This module is the guest half — create / delete the proof gist.
//! The host never issues a device code or holds the guest's token.
//!
//! The gist is created only after `GET /user` confirms the token carries the
//! `gist` OAuth scope (read from the `X-OAuth-Scopes` response header); a
//! token without it — or one whose scopes GitHub does not report, e.g. a
//! fine-grained PAT, which cannot create gists at all — fails with
//! [`IdentityProofError::ScopeMissing`] before anything is written. Delete
//! is bounded the same way: it reads the gist back first (`GET
//! /gists/{id}`, whose response carries the same scope header), refuses a
//! token without the scope, and refuses to delete anything that is not a
//! proof gist — exactly one file named [`PROOF_FILE_NAME`] — so the RPC can
//! never be turned against the account's other gists
//! ([`IdentityProofError::NotProofGist`]).

use serde_json::{json, Value};

use crate::error::Error;
use crate::github::GitHubSourceControl;

/// The OAuth scope gist creation requires.
pub const REQUIRED_SCOPE: &str = "gist";

/// The single file of a proof gist.
pub const PROOF_FILE_NAME: &str = "intent-join-proof.txt";

/// Response header GitHub reports an OAuth token's granted scopes in.
const OAUTH_SCOPES_HEADER: &str = "x-oauth-scopes";

/// A created proof gist: its id plus the login of the token's owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofGist {
    pub gist_id: String,
    pub login: String,
}

/// Failure modes of the proof-gist operations, split so the service layer
/// can map each onto its bounded wire code.
#[derive(Debug, thiserror::Error)]
pub enum IdentityProofError {
    /// The token lacks the `gist` scope (or GitHub reported no scopes).
    #[error("github token lacks the `{REQUIRED_SCOPE}` oauth scope (granted: {granted:?})")]
    ScopeMissing { granted: String },
    /// The gist named for deletion exists but is not an Intent proof gist
    /// (its files are not exactly one [`PROOF_FILE_NAME`]); nothing deleted.
    #[error("gist {gist_id:?} is not an Intent identity-proof gist")]
    NotProofGist { gist_id: String },
    /// GitHub rejected the token (`401` / `403`).
    #[error("github rejected the token: {0}")]
    Unauthorized(String),
    /// GitHub could not be reached (connect / read / TLS failure).
    #[error("github unreachable: {0}")]
    Unreachable(String),
    /// Any other forge / decode failure.
    #[error(transparent)]
    Other(#[from] Error),
}

/// Result alias for this module.
pub type Result<T> = std::result::Result<T, IdentityProofError>;

/// The proof gist's file content: the nonce on the first line, a human
/// explanation on the second.
#[must_use]
pub fn proof_content(nonce: &str, host_label: &str) -> String {
    format!("{nonce}\nProof of GitHub identity for Intent host {host_label}; safe to delete.\n")
}

/// Whether a `X-OAuth-Scopes` header value (comma-separated scopes) grants
/// [`REQUIRED_SCOPE`]. `None` (header absent) never does.
#[must_use]
pub fn has_gist_scope(header: Option<&str>) -> bool {
    header.is_some_and(|scopes| scopes.split(',').any(|s| s.trim() == REQUIRED_SCOPE))
}

fn map_octocrab(err: octocrab::Error) -> IdentityProofError {
    match err {
        octocrab::Error::Http { .. }
        | octocrab::Error::Hyper { .. }
        | octocrab::Error::Service { .. } => IdentityProofError::Unreachable(err.to_string()),
        other => match Error::from(other) {
            Error::Auth(msg) => IdentityProofError::Unauthorized(msg),
            e => IdentityProofError::Other(e),
        },
    }
}

fn client(token: &str, api_base_url: Option<&str>) -> Result<GitHubSourceControl> {
    GitHubSourceControl::new(token, api_base_url).map_err(IdentityProofError::Other)
}

/// `GET /user`: the token owner's login plus the granted-scopes header.
async fn user_and_scopes(crab: &octocrab::Octocrab) -> Result<(String, Option<String>)> {
    let response = crab._get("/user").await.map_err(map_octocrab)?;
    let response = octocrab::map_github_error(response)
        .await
        .map_err(map_octocrab)?;
    let scopes = response
        .headers()
        .get(OAUTH_SCOPES_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = crab.body_to_string(response).await.map_err(map_octocrab)?;
    let user: Value = serde_json::from_str(&body).map_err(Error::from)?;
    let login = user
        .get("login")
        .and_then(Value::as_str)
        .filter(|l| !l.is_empty())
        .ok_or_else(|| Error::Decode("GET /user response missing `login`".to_string()))?
        .to_string();
    Ok((login, scopes))
}

/// Create the secret proof gist for `nonce` with the guest's own `token`
/// (`api_base_url` `None` = api.github.com). Checks the `gist` scope first.
///
/// # Errors
///
/// [`IdentityProofError::ScopeMissing`] when the token lacks `gist`;
/// [`IdentityProofError::Unauthorized`] when GitHub rejects the token;
/// [`IdentityProofError::Unreachable`] on transport failure.
pub async fn create_proof_gist(
    token: &str,
    api_base_url: Option<&str>,
    nonce: &str,
    host_label: &str,
) -> Result<ProofGist> {
    let sc = client(token, api_base_url)?;
    let crab = sc.client();
    let (login, scopes) = user_and_scopes(crab).await?;
    if !has_gist_scope(scopes.as_deref()) {
        return Err(IdentityProofError::ScopeMissing {
            granted: scopes.unwrap_or_default(),
        });
    }
    let body = json!({
        "description": format!("Intent identity proof for {host_label} (safe to delete)"),
        "public": false,
        "files": { PROOF_FILE_NAME: { "content": proof_content(nonce, host_label) } },
    });
    let gist: Value = crab
        .post("/gists", Some(&body))
        .await
        .map_err(map_octocrab)?;
    let gist_id = gist
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| Error::Decode("POST /gists response missing `id`".to_string()))?
        .to_string();
    Ok(ProofGist { gist_id, login })
}

/// Whether a gist body (as `GET /gists/{id}` returns it) is an Intent proof
/// gist: exactly one file, named [`PROOF_FILE_NAME`].
#[must_use]
pub fn is_proof_gist(gist: &Value) -> bool {
    gist.get("files")
        .and_then(Value::as_object)
        .is_some_and(|files| files.len() == 1 && files.contains_key(PROOF_FILE_NAME))
}

/// `GET /gists/{id}`: the gist body plus the granted-scopes header, or
/// `None` when the gist no longer exists (`404`).
async fn gist_and_scopes(
    crab: &octocrab::Octocrab,
    gist_id: &str,
) -> Result<Option<(Value, Option<String>)>> {
    let response = crab
        ._get(format!("/gists/{gist_id}"))
        .await
        .map_err(map_octocrab)?;
    let response = match octocrab::map_github_error(response)
        .await
        .map_err(map_octocrab)
    {
        Ok(response) => response,
        Err(IdentityProofError::Other(Error::NotFound(_))) => return Ok(None),
        Err(e) => return Err(e),
    };
    let scopes = response
        .headers()
        .get(OAUTH_SCOPES_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = crab.body_to_string(response).await.map_err(map_octocrab)?;
    let gist: Value = serde_json::from_str(&body).map_err(Error::from)?;
    Ok(Some((gist, scopes)))
}

/// Delete a proof gist. Reads the gist back first: an already-deleted gist
/// (`404`) is `Ok` (idempotent); otherwise the token must carry the `gist`
/// scope and the gist must be a proof gist ([`is_proof_gist`]) before the
/// `DELETE` is sent, so the call can never remove another gist of the
/// account.
///
/// # Errors
///
/// [`IdentityProofError::ScopeMissing`] when the token lacks `gist`;
/// [`IdentityProofError::NotProofGist`] when `gist_id` names a gist that is
/// not an Intent proof gist (nothing deleted);
/// [`IdentityProofError::Unauthorized`] when GitHub rejects the token;
/// [`IdentityProofError::Unreachable`] on transport failure.
pub async fn delete_proof_gist(
    token: &str,
    api_base_url: Option<&str>,
    gist_id: &str,
) -> Result<()> {
    let sc = client(token, api_base_url)?;
    let crab = sc.client();
    let Some((gist, scopes)) = gist_and_scopes(crab, gist_id).await? else {
        return Ok(());
    };
    if !has_gist_scope(scopes.as_deref()) {
        return Err(IdentityProofError::ScopeMissing {
            granted: scopes.unwrap_or_default(),
        });
    }
    if !is_proof_gist(&gist) {
        return Err(IdentityProofError::NotProofGist {
            gist_id: gist_id.to_string(),
        });
    }
    match crab.gists().delete(gist_id).await.map_err(map_octocrab) {
        Ok(()) | Err(IdentityProofError::Other(Error::NotFound(_))) => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// One canned answer: status, extra response headers, JSON body.
    struct Answer {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: String,
    }

    /// Request line (`METHOD /path`) → body, for every request the mock saw.
    type Seen = Arc<Mutex<Vec<(String, String)>>>;

    /// Loopback GitHub API stub answering by request line. Records every
    /// request so tests can assert the gist payload that was sent.
    async fn spawn_mock(
        respond: impl Fn(&str) -> Answer + Send + Sync + 'static,
    ) -> (String, Seen) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind mock github host");
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let seen: Seen = Arc::default();
        let recorded = seen.clone();
        let respond = Arc::new(respond);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let respond = respond.clone();
                let recorded = recorded.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 1024];
                    let body_start = loop {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..body_start]).to_string();
                    let content_length = head
                        .lines()
                        .find_map(|l| {
                            let (name, value) = l.split_once(':')?;
                            name.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < body_start + content_length {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    let request_line = head
                        .lines()
                        .next()
                        .map(|l| {
                            l.rsplit_once(' ')
                                .map_or(l, |(target, _)| target)
                                .to_string()
                        })
                        .unwrap_or_default();
                    let body = String::from_utf8_lossy(&buf[body_start..]).to_string();
                    let answer = respond(&request_line);
                    recorded.lock().unwrap().push((request_line, body));
                    let mut resp = format!(
                        "HTTP/1.1 {} Status\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
                        answer.status,
                        answer.body.len()
                    );
                    for (name, value) in &answer.headers {
                        let _ = write!(resp, "{name}: {value}\r\n");
                    }
                    resp.push_str("\r\n");
                    resp.push_str(&answer.body);
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.flush().await;
                });
            }
        });
        (base, seen)
    }

    fn user_answer(scopes: Option<&str>) -> Answer {
        Answer {
            status: 200,
            headers: scopes
                .map(|s| vec![("X-OAuth-Scopes", s.to_string())])
                .unwrap_or_default(),
            body: json!({ "login": "octocat", "id": 1 }).to_string(),
        }
    }

    fn json_answer(status: u16, body: &Value) -> Answer {
        Answer {
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    #[test]
    fn proof_content_is_nonce_then_explanation() {
        assert_eq!(
            proof_content("n0nce", "Clement's Mac Studio"),
            "n0nce\nProof of GitHub identity for Intent host Clement's Mac Studio; safe to delete.\n"
        );
    }

    #[test]
    fn gist_scope_detection() {
        assert!(has_gist_scope(Some("repo, read:org, workflow, gist")));
        assert!(has_gist_scope(Some("gist")));
        assert!(!has_gist_scope(Some("repo, read:org, workflow")));
        assert!(!has_gist_scope(Some("")));
        assert!(!has_gist_scope(None));
    }

    #[tokio::test]
    async fn create_checks_scope_then_posts_a_secret_gist() {
        let (base, seen) = spawn_mock(|line| match line {
            "GET /user" => user_answer(Some("repo, read:org, workflow, gist")),
            "POST /gists" => json_answer(201, &json!({ "id": "abc123", "public": false })),
            other => json_answer(500, &json!({ "message": format!("unexpected {other}") })),
        })
        .await;
        let gist = create_proof_gist("tok", Some(&base), "the-nonce", "host-1")
            .await
            .expect("create proof gist");
        assert_eq!(
            gist,
            ProofGist {
                gist_id: "abc123".to_string(),
                login: "octocat".to_string(),
            }
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].0, "GET /user");
        assert_eq!(seen[1].0, "POST /gists");
        let posted: Value = serde_json::from_str(&seen[1].1).expect("gist body is json");
        assert_eq!(posted["public"], json!(false));
        assert_eq!(
            posted["files"][PROOF_FILE_NAME]["content"],
            json!(proof_content("the-nonce", "host-1"))
        );
        assert_eq!(
            posted["files"].as_object().map(serde_json::Map::len),
            Some(1)
        );
    }

    #[tokio::test]
    async fn create_without_gist_scope_fails_before_posting() {
        let (base, seen) = spawn_mock(|line| match line {
            "GET /user" => user_answer(Some("repo, read:org, workflow")),
            other => json_answer(500, &json!({ "message": format!("unexpected {other}") })),
        })
        .await;
        let err = create_proof_gist("tok", Some(&base), "n", "h")
            .await
            .expect_err("scope missing");
        assert!(
            matches!(&err, IdentityProofError::ScopeMissing { granted } if granted == "repo, read:org, workflow"),
            "{err:?}"
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "no gist request after the scope check"
        );
    }

    #[tokio::test]
    async fn create_with_unreported_scopes_is_scope_missing() {
        let (base, _seen) = spawn_mock(|line| match line {
            "GET /user" => user_answer(None),
            other => json_answer(500, &json!({ "message": format!("unexpected {other}") })),
        })
        .await;
        let err = create_proof_gist("tok", Some(&base), "n", "h")
            .await
            .expect_err("scope missing");
        assert!(
            matches!(&err, IdentityProofError::ScopeMissing { granted } if granted.is_empty()),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn create_with_rejected_token_is_unauthorized() {
        let (base, _seen) =
            spawn_mock(|_| json_answer(401, &json!({ "message": "Bad credentials" }))).await;
        let err = create_proof_gist("tok", Some(&base), "n", "h")
            .await
            .expect_err("unauthorized");
        assert!(
            matches!(err, IdentityProofError::Unauthorized(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn create_against_a_dead_host_is_unreachable() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        drop(listener);
        let err = create_proof_gist("tok", Some(&base), "n", "h")
            .await
            .expect_err("unreachable");
        assert!(matches!(err, IdentityProofError::Unreachable(_)), "{err:?}");
    }

    /// `GET /gists/{id}` answer: a gist with the given files, reporting the
    /// given scopes (header absent when `None`).
    fn gist_answer(files: &[&str], scopes: Option<&str>) -> Answer {
        let files: serde_json::Map<String, Value> = files
            .iter()
            .map(|name| ((*name).to_string(), json!({ "filename": name })))
            .collect();
        Answer {
            status: 200,
            headers: scopes
                .map(|s| vec![("X-OAuth-Scopes", s.to_string())])
                .unwrap_or_default(),
            body: json!({ "id": "live", "public": false, "files": files }).to_string(),
        }
    }

    fn no_content() -> Answer {
        Answer {
            status: 204,
            headers: Vec::new(),
            body: String::new(),
        }
    }

    #[test]
    fn proof_gist_shape_is_exactly_the_one_proof_file() {
        assert!(is_proof_gist(
            &json!({ "files": { PROOF_FILE_NAME: { "filename": PROOF_FILE_NAME } } })
        ));
        assert!(!is_proof_gist(
            &json!({ "files": { PROOF_FILE_NAME: {}, "notes.md": {} } })
        ));
        assert!(!is_proof_gist(&json!({ "files": { "notes.md": {} } })));
        assert!(!is_proof_gist(&json!({ "files": {} })));
        assert!(!is_proof_gist(&json!({ "id": "x" })));
    }

    #[tokio::test]
    async fn delete_reads_the_gist_back_then_deletes_and_is_idempotent_on_404() {
        let (base, seen) = spawn_mock(|line| match line {
            "GET /gists/live" => gist_answer(&[PROOF_FILE_NAME], Some("repo, gist")),
            "DELETE /gists/live" => no_content(),
            "GET /gists/gone" => json_answer(404, &json!({ "message": "Not Found" })),
            other => json_answer(500, &json!({ "message": format!("unexpected {other}") })),
        })
        .await;
        delete_proof_gist("tok", Some(&base), "live")
            .await
            .expect("delete live gist");
        delete_proof_gist("tok", Some(&base), "gone")
            .await
            .expect("404 is ok");
        let seen = seen.lock().unwrap();
        let lines: Vec<&str> = seen.iter().map(|(line, _)| line.as_str()).collect();
        assert_eq!(
            lines,
            ["GET /gists/live", "DELETE /gists/live", "GET /gists/gone"],
            "no DELETE for a gist that is already gone"
        );
    }

    /// The RPC cannot be turned against the account's other gists: a gist
    /// whose files are not exactly the proof file is refused before any
    /// `DELETE` is sent.
    #[tokio::test]
    async fn delete_refuses_a_gist_that_is_not_a_proof_gist() {
        let (base, seen) = spawn_mock(|line| match line {
            "GET /gists/notes" => gist_answer(&["notes.md"], Some("gist")),
            "GET /gists/mixed" => gist_answer(&[PROOF_FILE_NAME, "notes.md"], Some("gist")),
            other => json_answer(500, &json!({ "message": format!("unexpected {other}") })),
        })
        .await;
        for id in ["notes", "mixed"] {
            let err = delete_proof_gist("tok", Some(&base), id)
                .await
                .expect_err("not a proof gist");
            assert!(
                matches!(&err, IdentityProofError::NotProofGist { gist_id } if gist_id == id),
                "{err:?}"
            );
        }
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter().all(|(line, _)| line.starts_with("GET ")),
            "nothing deleted: {seen:?}"
        );
    }

    /// Like create, delete needs the `gist` scope: a token without it (or
    /// one whose scopes GitHub does not report) is `ScopeMissing`, not a
    /// silent success or a `403` misreported as a rejected token.
    #[tokio::test]
    async fn delete_without_gist_scope_is_scope_missing_before_deleting() {
        let (base, seen) = spawn_mock(|line| match line {
            "GET /gists/live" => gist_answer(&[PROOF_FILE_NAME], Some("repo, read:org")),
            "GET /gists/pat" => gist_answer(&[PROOF_FILE_NAME], None),
            other => json_answer(500, &json!({ "message": format!("unexpected {other}") })),
        })
        .await;
        let err = delete_proof_gist("tok", Some(&base), "live")
            .await
            .expect_err("scope missing");
        assert!(
            matches!(&err, IdentityProofError::ScopeMissing { granted } if granted == "repo, read:org"),
            "{err:?}"
        );
        let err = delete_proof_gist("tok", Some(&base), "pat")
            .await
            .expect_err("scope missing");
        assert!(
            matches!(&err, IdentityProofError::ScopeMissing { granted } if granted.is_empty()),
            "{err:?}"
        );
        assert_eq!(seen.lock().unwrap().len(), 2, "no DELETE without the scope");
    }

    #[tokio::test]
    async fn delete_with_rejected_token_is_unauthorized() {
        let (base, _seen) =
            spawn_mock(|_| json_answer(401, &json!({ "message": "Bad credentials" }))).await;
        let err = delete_proof_gist("tok", Some(&base), "x")
            .await
            .expect_err("unauthorized");
        assert!(
            matches!(err, IdentityProofError::Unauthorized(_)),
            "{err:?}"
        );
    }
}
