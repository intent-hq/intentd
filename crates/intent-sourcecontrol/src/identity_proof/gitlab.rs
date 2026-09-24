//! GitLab snippet identity proof — the `gitlab` implementation behind
//! [`super::provider::ProofProvider`].
//!
//! Guest half: [`create_proof_snippet`] publishes the host-issued nonce in a
//! **public personal snippet** (`POST /api/v4/snippets`, `visibility:
//! "public"`, a single [`super::PROOF_FILE_NAME`] file whose first line is
//! the nonce — the same [`super::proof_content`] the gist proof writes) with
//! the guest's own token; [`delete_proof_snippet`] removes it again
//! (`DELETE /api/v4/snippets/:id`), idempotent on `404` and refusing any
//! snippet that is not a proof snippet ([`is_proof_snippet`]) so the RPC can
//! never be turned against the account's other snippets. Unlike the gist, a
//! public snippet is readable by anyone who knows its id — which is exactly
//! what lets a host with **no** GitLab connection verify it.
//!
//! Host half: [`verify_proof_snippet`] reads the snippet metadata
//! (`GET /api/v4/snippets/:id`) and raw content (`…/raw`) **anonymously**
//! first. A self-managed instance that restricts anonymous API reads answers
//! `401` / `403` (or `404`) — then the read is retried with the host's own
//! credential for the same instance when it has one, and otherwise fails
//! with [`IdentityProofError::Unverifiable`] ("cannot verify identity on
//! <host>"): the host cannot tell a hidden snippet from a missing one. The
//! identity comes from the snippet's `author` (`id`, `username`,
//! `avatar_url`) — no follow-up user call.

use serde_json::{json, Value};

use super::{IdentityProofError, Result, PROOF_FILE_NAME};
use crate::error::Error;
use crate::gitlab_auth::{http_client, GitlabHost};

/// The `author` of a proof snippet, as `GET /api/v4/snippets/:id` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnippetAuthor {
    pub id: u64,
    pub username: String,
    pub avatar_url: Option<String>,
}

/// A created proof snippet: its numeric id (as a string, the wire `proofId`)
/// plus the token owner's identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofSnippet {
    pub snippet_id: String,
    pub author: SnippetAuthor,
}

/// What the host reads back to verify a proof: the author, the snippet's
/// `created_at` (RFC 3339, as GitLab reports it) and the trimmed first line
/// of the raw content — `None` when the snippet carries no readable
/// [`PROOF_FILE_NAME`] file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofSnippetView {
    pub author: SnippetAuthor,
    pub created_at: String,
    pub proof_first_line: Option<String>,
}

/// The snippet title a proof is created with.
#[must_use]
pub fn proof_title(host_label: &str) -> String {
    format!("Intent identity proof for {host_label} (safe to delete)")
}

/// The proof snippet's file content: the nonce on the first line (what the
/// host verifies — the same layout as [`super::proof_content`]), a human
/// explanation on the second.
#[must_use]
pub fn proof_content(nonce: &str, host_label: &str) -> String {
    format!("{nonce}\nProof of GitLab identity for Intent host {host_label}; safe to delete.\n")
}

/// A GitLab snippet id as the wire may name it: non-empty ASCII digits, so
/// it is safe in a request path.
#[must_use]
pub fn valid_snippet_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 20 && id.bytes().all(|b| b.is_ascii_digit())
}

/// Whether a snippet body (as `GET /api/v4/snippets/:id` returns it) is an
/// Intent proof snippet: exactly one file, named [`PROOF_FILE_NAME`]. The
/// `files` array is authoritative; the legacy single `file_name` field is
/// consulted only when the instance reports no `files`.
#[must_use]
pub fn is_proof_snippet(snippet: &Value) -> bool {
    match snippet.get("files").and_then(Value::as_array) {
        Some(files) => {
            files.len() == 1
                && files[0].get("path").and_then(Value::as_str) == Some(PROOF_FILE_NAME)
        }
        None => snippet.get("file_name").and_then(Value::as_str) == Some(PROOF_FILE_NAME),
    }
}

fn author_of(snippet: &Value, what: &str) -> Result<SnippetAuthor> {
    let author = snippet
        .get("author")
        .ok_or_else(|| Error::Decode(format!("{what} response missing `author`")))?;
    let id = author
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::Decode(format!("{what} response missing `author.id`")))?;
    let username = author
        .get("username")
        .and_then(Value::as_str)
        .filter(|u| !u.is_empty())
        .ok_or_else(|| Error::Decode(format!("{what} response missing `author.username`")))?
        .to_string();
    let avatar_url = author
        .get("avatar_url")
        .and_then(Value::as_str)
        .filter(|u| !u.is_empty())
        .map(str::to_string);
    Ok(SnippetAuthor {
        id,
        username,
        avatar_url,
    })
}

/// reqwest transport failures → [`IdentityProofError::Unreachable`]; the
/// message carries the URL only (never headers), so no token can leak.
fn map_transport(e: &reqwest::Error) -> IdentityProofError {
    IdentityProofError::Unreachable(format!("gitlab request failed: {e}"))
}

fn api_error(host: &GitlabHost, what: &str, status: reqwest::StatusCode) -> IdentityProofError {
    IdentityProofError::Other(Error::Api(format!(
        "gitlab {what} on {} failed ({status})",
        host.host()
    )))
}

async fn send(
    req: reqwest::RequestBuilder,
    token: Option<&str>,
) -> std::result::Result<reqwest::Response, IdentityProofError> {
    let req = match token {
        Some(token) => req.bearer_auth(token),
        None => req,
    };
    req.send().await.map_err(|e| map_transport(&e))
}

/// Create the public proof snippet for `nonce` on `host` with the guest's
/// own `token`.
///
/// # Errors
///
/// [`IdentityProofError::Unauthorized`] when the instance rejects the token
/// (`401`); [`IdentityProofError::ScopeMissing`] when it refuses the write
/// (`403` — a token without the `api` scope); [`IdentityProofError::Unreachable`]
/// on transport failure.
pub async fn create_proof_snippet(
    host: &GitlabHost,
    token: &str,
    nonce: &str,
    host_label: &str,
) -> Result<ProofSnippet> {
    let client = http_client()?;
    let body = json!({
        "title": proof_title(host_label),
        "visibility": "public",
        "files": [{ "file_path": PROOF_FILE_NAME, "content": proof_content(nonce, host_label) }],
    });
    let response = send(
        client
            .post(format!("{}/snippets", host.api_base()))
            .json(&body),
        Some(token),
    )
    .await?;
    let status = response.status();
    match status.as_u16() {
        401 => {
            return Err(IdentityProofError::Unauthorized(format!(
                "gitlab rejected the token for {} ({status})",
                host.host()
            )))
        }
        403 => {
            let detail: Value = response.json().await.unwrap_or(Value::Null);
            return Err(IdentityProofError::ScopeMissing {
                granted: detail
                    .get("error")
                    .or_else(|| detail.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("forbidden")
                    .to_string(),
            });
        }
        429 => {
            return Err(IdentityProofError::Other(Error::RateLimited(format!(
                "gitlab rate limited the snippet create on {}",
                host.host()
            ))))
        }
        _ if !status.is_success() => return Err(api_error(host, "snippet create", status)),
        _ => {}
    }
    let snippet: Value = response
        .json()
        .await
        .map_err(|e| Error::Decode(format!("unrecognized gitlab snippet response: {e}")))?;
    let snippet_id = snippet
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::Decode("POST /snippets response missing `id`".to_string()))?
        .to_string();
    let author = author_of(&snippet, "POST /snippets")?;
    Ok(ProofSnippet { snippet_id, author })
}

/// How a metadata / raw read of a snippet ended for one caller.
enum Read<T> {
    Ok(T),
    /// `404`.
    Missing,
    /// `401` / `403`: the caller (anonymous, or this token) may not see it.
    Refused,
}

async fn read_snippet(
    host: &GitlabHost,
    client: &reqwest::Client,
    path: &str,
    token: Option<&str>,
) -> Result<Read<reqwest::Response>> {
    let response = send(client.get(format!("{}{path}", host.api_base())), token).await?;
    let status = response.status();
    match status.as_u16() {
        401 | 403 => Ok(Read::Refused),
        404 => Ok(Read::Missing),
        429 => Err(IdentityProofError::Other(Error::RateLimited(format!(
            "gitlab rate limited the snippet read on {}",
            host.host()
        )))),
        _ if !status.is_success() => Err(api_error(host, "snippet read", status)),
        _ => Ok(Read::Ok(response)),
    }
}

async fn snippet_json(response: reqwest::Response) -> Result<Value> {
    response
        .json::<Value>()
        .await
        .map_err(|e| Error::Decode(format!("unrecognized gitlab snippet response: {e}")).into())
}

/// Delete a proof snippet with the guest's own `token`: reads it back first
/// — an already-deleted snippet (`404`) or an id that is not a snippet id
/// ([`valid_snippet_id`]) is `Ok` (idempotent, nothing to delete) — and
/// refuses anything that is not a proof snippet ([`is_proof_snippet`])
/// before the `DELETE` is sent.
///
/// # Errors
///
/// [`IdentityProofError::NotProofSnippet`] when `snippet_id` names a snippet
/// that is not an Intent proof snippet (nothing deleted);
/// [`IdentityProofError::Unauthorized`] when the instance rejects the token;
/// [`IdentityProofError::Unreachable`] on transport failure.
pub async fn delete_proof_snippet(host: &GitlabHost, token: &str, snippet_id: &str) -> Result<()> {
    if !valid_snippet_id(snippet_id) {
        return Ok(());
    }
    let client = http_client()?;
    let path = format!("/snippets/{snippet_id}");
    let snippet = match read_snippet(host, &client, &path, Some(token)).await? {
        Read::Ok(response) => snippet_json(response).await?,
        Read::Missing => return Ok(()),
        Read::Refused => {
            return Err(IdentityProofError::Unauthorized(format!(
                "gitlab rejected the token for {}",
                host.host()
            )))
        }
    };
    if !is_proof_snippet(&snippet) {
        return Err(IdentityProofError::NotProofSnippet {
            snippet_id: snippet_id.to_string(),
        });
    }
    let response = send(
        client.delete(format!("{}{path}", host.api_base())),
        Some(token),
    )
    .await?;
    let status = response.status();
    match status.as_u16() {
        404 => Ok(()),
        401 | 403 => Err(IdentityProofError::Unauthorized(format!(
            "gitlab rejected the token for {} ({status})",
            host.host()
        ))),
        _ if !status.is_success() => Err(api_error(host, "snippet delete", status)),
        _ => Ok(()),
    }
}

/// Read a proof snippet's metadata and raw content as `token` (anonymously
/// when `None`). A `404` / `401` / `403` on either read is the whole view's
/// [`Read::Missing`] / [`Read::Refused`]: a snippet whose metadata is public
/// but whose raw content is hidden from this caller is not readable by it.
async fn read_view(
    host: &GitlabHost,
    client: &reqwest::Client,
    snippet_id: &str,
    token: Option<&str>,
) -> Result<Read<ProofSnippetView>> {
    let path = format!("/snippets/{snippet_id}");
    let snippet = match read_snippet(host, client, &path, token).await? {
        Read::Ok(response) => snippet_json(response).await?,
        Read::Missing => return Ok(Read::Missing),
        Read::Refused => return Ok(Read::Refused),
    };
    let author = author_of(&snippet, "GET /snippets/:id")?;
    let created_at = snippet
        .get("created_at")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
        .ok_or_else(|| {
            Error::Decode("GET /snippets/:id response missing `created_at`".to_string())
        })?
        .to_string();
    let proof_first_line = if is_proof_snippet(&snippet) {
        match read_snippet(host, client, &format!("{path}/raw"), token).await? {
            Read::Ok(response) => {
                let raw = response.text().await.map_err(|e| map_transport(&e))?;
                Some(raw.lines().next().unwrap_or_default().trim().to_string())
            }
            Read::Missing => return Ok(Read::Missing),
            Read::Refused => return Ok(Read::Refused),
        }
    } else {
        None
    };
    Ok(Read::Ok(ProofSnippetView {
        author,
        created_at,
        proof_first_line,
    }))
}

/// The host-side read of a guest's proof snippet on `host`: anonymously
/// first, then — when the instance refused or hid it — with `own_token`,
/// the host's own credential **for the same instance**.
///
/// # Errors
///
/// [`IdentityProofError::NotFound`] when the snippet does not exist (an
/// authenticated read answered `404`, or the id is malformed);
/// [`IdentityProofError::Unverifiable`] when the anonymous read (metadata
/// or raw content) was refused or found nothing and the host holds no
/// credential for `host` (or its credential was refused too);
/// [`IdentityProofError::Unreachable`] on transport failure.
pub async fn verify_proof_snippet(
    host: &GitlabHost,
    snippet_id: &str,
    own_token: Option<&str>,
) -> Result<ProofSnippetView> {
    if !valid_snippet_id(snippet_id) {
        return Err(IdentityProofError::NotFound {
            proof_id: snippet_id.to_string(),
        });
    }
    let unverifiable = || IdentityProofError::Unverifiable {
        host: host.host().to_string(),
    };
    let client = http_client()?;
    if let Read::Ok(view) = read_view(host, &client, snippet_id, None).await? {
        return Ok(view);
    }
    let Some(token) = own_token else {
        return Err(unverifiable());
    };
    match read_view(host, &client, snippet_id, Some(token)).await? {
        Read::Ok(view) => Ok(view),
        Read::Missing => Err(IdentityProofError::NotFound {
            proof_id: snippet_id.to_string(),
        }),
        Read::Refused => Err(unverifiable()),
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// One canned answer: status + body (JSON or raw text).
    struct Answer {
        status: u16,
        body: String,
    }

    fn json_answer(status: u16, body: &Value) -> Answer {
        Answer {
            status,
            body: body.to_string(),
        }
    }

    fn raw_answer(body: &str) -> Answer {
        Answer {
            status: 200,
            body: body.to_string(),
        }
    }

    /// `(request line, bearer token if any, body)` for every request seen.
    type Seen = Arc<Mutex<Vec<(String, Option<String>, String)>>>;

    /// Loopback GitLab API stub answering by request line + bearer.
    async fn spawn_mock(
        respond: impl Fn(&str, Option<&str>) -> Answer + Send + Sync + 'static,
    ) -> (GitlabHost, Seen) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind mock gitlab");
        let host = GitlabHost::parse(&format!(
            "http://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        ))
        .expect("loopback host");
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
                    let header = |name: &str| -> Option<String> {
                        head.lines().find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case(name)
                                .then(|| v.trim().to_string())
                        })
                    };
                    let content_length = header("content-length")
                        .and_then(|v| v.parse::<usize>().ok())
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
                    let bearer = header("authorization")
                        .and_then(|v| v.strip_prefix("Bearer ").map(str::to_string));
                    let body = String::from_utf8_lossy(&buf[body_start..]).to_string();
                    let answer = respond(&request_line, bearer.as_deref());
                    recorded.lock().unwrap().push((request_line, bearer, body));
                    let resp = format!(
                        "HTTP/1.1 {} Status\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        answer.status,
                        answer.body.len(),
                        answer.body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.flush().await;
                });
            }
        });
        (host, seen)
    }

    fn snippet_body(id: u64, files: &[&str]) -> Value {
        json!({
            "id": id,
            "title": proof_title("host-1"),
            "visibility": "public",
            "created_at": "2026-09-21T02:00:00.000Z",
            "author": {
                "id": 4242,
                "username": "glab-octocat",
                "name": "GitLab Octocat",
                "avatar_url": "https://gitlab.example/avatar.png",
            },
            "file_name": files.first().copied().unwrap_or_default(),
            "files": files.iter().map(|f| json!({ "path": f, "raw_url": format!("https://gitlab.example/-/snippets/{id}/raw/main/{f}") })).collect::<Vec<_>>(),
        })
    }

    fn author() -> SnippetAuthor {
        SnippetAuthor {
            id: 4242,
            username: "glab-octocat".to_string(),
            avatar_url: Some("https://gitlab.example/avatar.png".to_string()),
        }
    }

    #[test]
    fn proof_content_is_nonce_then_explanation() {
        assert_eq!(
            proof_content("n0nce", "Clement's Mac Studio"),
            "n0nce\nProof of GitLab identity for Intent host Clement's Mac Studio; safe to delete.\n"
        );
    }

    #[test]
    fn snippet_id_shape() {
        assert!(valid_snippet_id("1"));
        assert!(valid_snippet_id("4815162342"));
        assert!(!valid_snippet_id(""));
        assert!(!valid_snippet_id("12a"));
        assert!(!valid_snippet_id("../1"));
        assert!(!valid_snippet_id("-1"));
        assert!(!valid_snippet_id("123456789012345678901"));
    }

    #[test]
    fn proof_snippet_detection() {
        assert!(is_proof_snippet(&snippet_body(1, &[PROOF_FILE_NAME])));
        assert!(!is_proof_snippet(&snippet_body(
            1,
            &[PROOF_FILE_NAME, "other.txt"]
        )));
        assert!(!is_proof_snippet(&snippet_body(1, &["notes.md"])));
        assert!(!is_proof_snippet(&snippet_body(1, &[])));
        // Legacy single-file shape without a `files` array.
        assert!(is_proof_snippet(&json!({ "file_name": PROOF_FILE_NAME })));
        assert!(!is_proof_snippet(&json!({ "file_name": "x.txt" })));
        assert!(!is_proof_snippet(&json!({})));
    }

    #[tokio::test]
    async fn create_posts_a_public_single_file_snippet_with_the_bearer() {
        let (host, seen) = spawn_mock(|line, bearer| match (line, bearer) {
            ("POST /api/v4/snippets", Some("glpat-guest")) => {
                json_answer(201, &snippet_body(77, &[PROOF_FILE_NAME]))
            }
            _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
        })
        .await;
        let created = create_proof_snippet(&host, "glpat-guest", "the-nonce", "host-1")
            .await
            .expect("create proof snippet");
        assert_eq!(
            created,
            ProofSnippet {
                snippet_id: "77".to_string(),
                author: author(),
            }
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "one request: no user probe");
        let posted: Value = serde_json::from_str(&seen[0].2).expect("json body");
        assert_eq!(posted["visibility"], json!("public"));
        assert_eq!(posted["title"], json!(proof_title("host-1")));
        assert_eq!(
            posted["files"],
            json!([{ "file_path": PROOF_FILE_NAME, "content": proof_content("the-nonce", "host-1") }])
        );
    }

    #[tokio::test]
    async fn create_maps_401_and_403_onto_unauthorized_and_scope_missing() {
        let (host, _) = spawn_mock(|_, bearer| match bearer {
            Some("revoked") => json_answer(401, &json!({ "message": "401 Unauthorized" })),
            Some("read-only") => json_answer(
                403,
                &json!({ "error": "insufficient_scope", "error_description": "api" }),
            ),
            _ => json_answer(500, &json!({ "message": "unexpected" })),
        })
        .await;
        let err = create_proof_snippet(&host, "revoked", "n", "h")
            .await
            .expect_err("rejected token");
        assert!(
            matches!(err, IdentityProofError::Unauthorized(_)),
            "{err:?}"
        );
        let err = create_proof_snippet(&host, "read-only", "n", "h")
            .await
            .expect_err("scope missing");
        assert!(
            matches!(&err, IdentityProofError::ScopeMissing { granted } if granted == "insufficient_scope"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn create_against_a_dark_host_is_unreachable() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let host = GitlabHost::parse(&format!(
            "http://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        ))
        .unwrap();
        drop(listener);
        let err = create_proof_snippet(&host, "tok", "n", "h")
            .await
            .expect_err("connection refused");
        assert!(matches!(err, IdentityProofError::Unreachable(_)), "{err:?}");
    }

    #[tokio::test]
    async fn delete_reads_back_then_deletes_a_proof_snippet_and_is_idempotent() {
        let (host, seen) = spawn_mock(|line, bearer| match (line, bearer) {
            ("GET /api/v4/snippets/77", Some("glpat-guest")) => {
                json_answer(200, &snippet_body(77, &[PROOF_FILE_NAME]))
            }
            ("DELETE /api/v4/snippets/77", Some("glpat-guest")) => Answer {
                status: 204,
                body: String::new(),
            },
            ("GET /api/v4/snippets/78", Some("glpat-guest")) => {
                json_answer(404, &json!({ "message": "404 Snippet Not Found" }))
            }
            _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
        })
        .await;
        delete_proof_snippet(&host, "glpat-guest", "77")
            .await
            .expect("delete proof snippet");
        delete_proof_snippet(&host, "glpat-guest", "78")
            .await
            .expect("already deleted is ok");
        delete_proof_snippet(&host, "glpat-guest", "not-an-id")
            .await
            .expect("malformed id names nothing");
        let seen = seen.lock().unwrap();
        let lines: Vec<&str> = seen.iter().map(|s| s.0.as_str()).collect();
        assert_eq!(
            lines,
            [
                "GET /api/v4/snippets/77",
                "DELETE /api/v4/snippets/77",
                "GET /api/v4/snippets/78",
            ]
        );
    }

    #[tokio::test]
    async fn delete_refuses_a_non_proof_snippet_without_deleting() {
        let (host, seen) = spawn_mock(|line, _| match line {
            "GET /api/v4/snippets/5" => json_answer(200, &snippet_body(5, &["notes.md"])),
            _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
        })
        .await;
        let err = delete_proof_snippet(&host, "glpat-guest", "5")
            .await
            .expect_err("not a proof snippet");
        assert!(
            matches!(&err, IdentityProofError::NotProofSnippet { snippet_id } if snippet_id == "5"),
            "{err:?}"
        );
        assert_eq!(seen.lock().unwrap().len(), 1, "no DELETE was sent");
    }

    #[tokio::test]
    async fn delete_with_a_rejected_token_is_unauthorized() {
        let (host, _) = spawn_mock(|_, _| json_answer(401, &json!({ "message": "401" }))).await;
        let err = delete_proof_snippet(&host, "revoked", "5")
            .await
            .expect_err("rejected");
        assert!(
            matches!(err, IdentityProofError::Unauthorized(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn verify_reads_metadata_and_raw_anonymously() {
        let (host, seen) = spawn_mock(|line, bearer| match (line, bearer) {
            ("GET /api/v4/snippets/77", None) => {
                json_answer(200, &snippet_body(77, &[PROOF_FILE_NAME]))
            }
            ("GET /api/v4/snippets/77/raw", None) => {
                raw_answer(&proof_content("the-nonce", "host-1"))
            }
            _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
        })
        .await;
        let view = verify_proof_snippet(&host, "77", Some("glpat-host"))
            .await
            .expect("verify anonymously");
        assert_eq!(
            view,
            ProofSnippetView {
                author: author(),
                created_at: "2026-09-21T02:00:00.000Z".to_string(),
                proof_first_line: Some("the-nonce".to_string()),
            }
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(
            seen.iter().all(|s| s.1.is_none()),
            "the host's own token is never sent when the anonymous read succeeds"
        );
    }

    #[tokio::test]
    async fn verify_falls_back_to_the_hosts_own_token_when_anonymous_reads_are_refused() {
        let (host, seen) = spawn_mock(|line, bearer| match (line, bearer) {
            (_, None | Some("glpat-other")) => {
                json_answer(401, &json!({ "message": "401 Unauthorized" }))
            }
            ("GET /api/v4/snippets/77", Some("glpat-host")) => {
                json_answer(200, &snippet_body(77, &[PROOF_FILE_NAME]))
            }
            ("GET /api/v4/snippets/77/raw", Some("glpat-host")) => {
                raw_answer(&proof_content("the-nonce", "host-1"))
            }
            ("GET /api/v4/snippets/78", Some("glpat-host")) => {
                json_answer(404, &json!({ "message": "404 Snippet Not Found" }))
            }
            _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
        })
        .await;
        let view = verify_proof_snippet(&host, "77", Some("glpat-host"))
            .await
            .expect("verify with the host's own token");
        assert_eq!(view.proof_first_line.as_deref(), Some("the-nonce"));
        assert_eq!(view.author, author());
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen[0].1, None, "anonymous first");
            assert_eq!(seen[1].1.as_deref(), Some("glpat-host"));
        }
        // With the credential, a hidden-vs-missing snippet is decidable.
        let err = verify_proof_snippet(&host, "78", Some("glpat-host"))
            .await
            .expect_err("missing");
        assert!(
            matches!(&err, IdentityProofError::NotFound { proof_id } if proof_id == "78"),
            "{err:?}"
        );
        // Without one, the host cannot tell and says so.
        let err = verify_proof_snippet(&host, "77", None)
            .await
            .expect_err("unverifiable");
        assert!(
            matches!(&err, IdentityProofError::Unverifiable { host: h } if h == host.host()),
            "{err:?}"
        );
        // A refused credential is unverifiable too, never "not found".
        let err = verify_proof_snippet(&host, "77", Some("glpat-other"))
            .await
            .expect_err("refused credential");
        assert!(
            matches!(err, IdentityProofError::Unverifiable { .. }),
            "{err:?}"
        );
    }

    /// Metadata readable but raw content hidden: the raw status decides the
    /// read the same way a metadata status would. Matrix over the raw status
    /// seen anonymously × the raw status seen with the host's own token.
    #[tokio::test]
    async fn verify_preserves_a_restricted_raw_read_and_falls_back_or_refuses() {
        struct Case {
            anon_raw: u16,
            own_raw: u16,
            expect: &'static str,
        }
        let cases = [
            // Anonymous raw hidden → own token reads it.
            Case {
                anon_raw: 401,
                own_raw: 200,
                expect: "ok",
            },
            Case {
                anon_raw: 403,
                own_raw: 200,
                expect: "ok",
            },
            Case {
                anon_raw: 404,
                own_raw: 200,
                expect: "ok",
            },
            // Own token refused on raw too → unverifiable, never a nonce-less view.
            Case {
                anon_raw: 401,
                own_raw: 401,
                expect: "unverifiable",
            },
            Case {
                anon_raw: 403,
                own_raw: 403,
                expect: "unverifiable",
            },
            Case {
                anon_raw: 404,
                own_raw: 403,
                expect: "unverifiable",
            },
            // Own token sees metadata but the raw content is gone → not found.
            Case {
                anon_raw: 404,
                own_raw: 404,
                expect: "not-found",
            },
            Case {
                anon_raw: 401,
                own_raw: 404,
                expect: "not-found",
            },
        ];
        for Case {
            anon_raw,
            own_raw,
            expect,
        } in cases
        {
            let (host, seen) = spawn_mock(move |line, bearer| match (line, bearer) {
                ("GET /api/v4/snippets/77", _) => {
                    json_answer(200, &snippet_body(77, &[PROOF_FILE_NAME]))
                }
                ("GET /api/v4/snippets/77/raw", None) => {
                    json_answer(anon_raw, &json!({ "message": "raw hidden" }))
                }
                ("GET /api/v4/snippets/77/raw", Some("glpat-host")) if own_raw == 200 => {
                    raw_answer(&proof_content("the-nonce", "host-1"))
                }
                ("GET /api/v4/snippets/77/raw", Some("glpat-host")) => {
                    json_answer(own_raw, &json!({ "message": "raw hidden" }))
                }
                _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
            })
            .await;
            let label = format!("anon raw {anon_raw} / own raw {own_raw}");

            // With the host's own token: fall back, then decide.
            let result = verify_proof_snippet(&host, "77", Some("glpat-host")).await;
            match expect {
                "ok" => {
                    let view = result.unwrap_or_else(|e| panic!("{label}: {e:?}"));
                    assert_eq!(
                        view.proof_first_line.as_deref(),
                        Some("the-nonce"),
                        "{label}"
                    );
                }
                "unverifiable" => assert!(
                    matches!(&result, Err(IdentityProofError::Unverifiable { host: h }) if h == host.host()),
                    "{label}: {result:?}"
                ),
                "not-found" => assert!(
                    matches!(&result, Err(IdentityProofError::NotFound { proof_id }) if proof_id == "77"),
                    "{label}: {result:?}"
                ),
                _ => unreachable!(),
            }
            {
                let mut seen = seen.lock().unwrap();
                let bearers: Vec<Option<&str>> = seen.iter().map(|s| s.1.as_deref()).collect();
                assert_eq!(
                    bearers,
                    [None, None, Some("glpat-host"), Some("glpat-host")],
                    "{label}: anonymous metadata + raw, then both again with the own token"
                );
                seen.clear();
            }

            // Without one: every restricted raw read is unverifiable.
            let err = verify_proof_snippet(&host, "77", None)
                .await
                .expect_err(&label);
            assert!(
                matches!(&err, IdentityProofError::Unverifiable { host: h } if h == host.host()),
                "{label}: {err:?}"
            );
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2, "{label}: no token to fall back to");
            assert!(seen.iter().all(|s| s.1.is_none()), "{label}");
        }
    }

    #[tokio::test]
    async fn verify_of_a_hidden_snippet_without_a_credential_is_unverifiable_on_404_too() {
        let (host, _) =
            spawn_mock(|_, _| json_answer(404, &json!({ "message": "404 Not Found" }))).await;
        let err = verify_proof_snippet(&host, "77", None)
            .await
            .expect_err("cannot tell hidden from missing");
        assert!(
            matches!(err, IdentityProofError::Unverifiable { .. }),
            "{err:?}"
        );
        let err = verify_proof_snippet(&host, "x", None)
            .await
            .expect_err("malformed id");
        assert!(
            matches!(err, IdentityProofError::NotFound { .. }),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn verify_of_a_non_proof_snippet_carries_no_first_line() {
        let (host, seen) = spawn_mock(|line, _| match line {
            "GET /api/v4/snippets/5" => json_answer(200, &snippet_body(5, &["notes.md"])),
            _ => json_answer(500, &json!({ "message": format!("unexpected {line}") })),
        })
        .await;
        let view = verify_proof_snippet(&host, "5", None).await.expect("view");
        assert_eq!(view.proof_first_line, None);
        assert_eq!(seen.lock().unwrap().len(), 1, "raw content is not fetched");
    }

    #[tokio::test]
    async fn verify_of_a_snippet_without_an_author_is_a_decode_error() {
        let (host, _) = spawn_mock(|_, _| {
            json_answer(
                200,
                &json!({ "id": 5, "created_at": "2026-09-21T02:00:00.000Z", "files": [] }),
            )
        })
        .await;
        let err = verify_proof_snippet(&host, "5", None)
            .await
            .expect_err("no author");
        assert!(
            matches!(err, IdentityProofError::Other(Error::Decode(_))),
            "{err:?}"
        );
    }
}
