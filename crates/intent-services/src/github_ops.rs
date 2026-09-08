//! Wire-policy glue for the explicit-addressing `github.*` methods (§5.27).
//!
//! Pure mapping/validation that renders the host-agnostic [`SourceControl`]
//! models into the GitHub-shaped wire DTOs the FE expects (`GithubPullRequest`
//! / `GithubIssue` / `ReviewComment` / `ReviewThread`, camelCase per §5.27), and
//! parses the `filter` / `state` / pagination params. The forge calls go through
//! the trait; this module owns only the parity-critical glue so it stays
//! unit-testable without a network.
//!
//! PARITY NOTE: the engine models are intentionally thinner than the GitHub-
//! native FE shapes — fields the host-agnostic model does not carry (issue
//! timestamps/labels/author, PR review/commit/diff tallies, base SHA, avatars)
//! are emitted with shape-preserving defaults so the wire keys match
//! `shared/types.ts` field-for-field even when the value is not available.

use base64::Engine as _;
use intent_core::{Error, Result};
use intent_sourcecontrol::{
    Issue, PrInvolvement, PrState, PullRequest, ReviewComment, ReviewThread,
};
use serde_json::{json, Value};

/// §5.5 pagination: default page size and the inclusive cap.
const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;

/// Clamp an optional `limit` / `perPage` into the §5.5 `[1, 200]` window
/// (default 50) and cast to the engine's `u8` query width.
pub(crate) fn clamp_limit(limit: Option<i64>) -> u8 {
    u8::try_from(limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)).expect("clamped to [1, 200]")
}

/// Wrap an engine-native cursor (a REST page number or a GraphQL end-cursor)
/// into the opaque base64 `nextToken` exposed on the wire (§5.5). The internal
/// `{"c": <cursor>}` JSON is a private detail clients MUST treat as opaque.
pub(crate) fn encode_next_token(cursor: &str) -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD
        .encode(serde_json::to_vec(&json!({ "c": cursor })).expect("cursor json is serializable"))
}

/// Decode an incoming opaque `nextToken` back into the engine-native cursor. An
/// absent or malformed token yields `None` (start from the first page).
pub(crate) fn decode_next_token(token: Option<&str>) -> Option<String> {
    let token = token?;
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(token)
        .ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    v.get("c").and_then(Value::as_str).map(String::from)
}

/// Render an engine `next_cursor` as the wire `nextToken` value: an opaque
/// base64 string when more pages remain, or JSON `null` on the last page.
pub(crate) fn next_token_value(next_cursor: Option<&str>) -> Value {
    match next_cursor {
        Some(c) => Value::String(encode_next_token(c)),
        None => Value::Null,
    }
}

/// Parse the `github.pulls.list` / `github.pulls.search` `state` onto a
/// [`PrState`] (`None` == "all"); an invalid value throws → `-32603` (parity
/// with the `pr_ops` validators).
pub(crate) fn parse_pr_state(state: Option<&str>) -> Result<Option<PrState>> {
    match state {
        None | Some("all") => Ok(None),
        Some("open") => Ok(Some(PrState::Open)),
        Some("closed") => Ok(Some(PrState::Closed)),
        Some("merged") => Ok(Some(PrState::Merged)),
        Some(_) => Err(Error::Internal(
            "state must be one of: open, closed, all".to_string(),
        )),
    }
}

/// Parse the search `filter` onto `@me` involvement (`all`/absent → `None`,
/// matching the FE's unconstrained "all").
pub(crate) fn parse_pr_involvement(filter: Option<&str>) -> Result<Option<PrInvolvement>> {
    match filter {
        None | Some("all") => Ok(None),
        Some("assigned") => Ok(Some(PrInvolvement::Assigned)),
        Some("created") => Ok(Some(PrInvolvement::Created)),
        Some("review-requested") => Ok(Some(PrInvolvement::ReviewRequested)),
        Some("involves") => Ok(Some(PrInvolvement::Involves)),
        Some(_) => Err(Error::Internal(
            "filter must be one of: all, assigned, created, review-requested, involves".to_string(),
        )),
    }
}

/// Validate the `github.issues.search` `filter` against the issues value set
/// from PROTOCOL §5 (`all`/`assigned`/`created`/`involves`, absent == `all`).
/// The PR-only `review-requested` (and anything else) throws → `-32603`
/// (parity with [`parse_pr_involvement`]). The parsed value is not returned:
/// the host-agnostic engine cannot express `@me` involvement for issues yet
/// (v1 limitation), so validation is all this gate does.
pub(crate) fn parse_issue_filter(filter: Option<&str>) -> Result<()> {
    match filter {
        None | Some("all" | "assigned" | "created" | "involves") => Ok(()),
        Some(_) => Err(Error::Internal(
            "filter must be one of: all, assigned, created, involves".to_string(),
        )),
    }
}

/// Normalize the optional free-text search `query`: trim and drop blanks so an
/// absent or whitespace-only query preserves the listing behavior exactly.
pub(crate) fn normalize_search_query(query: Option<String>) -> Option<String> {
    query
        .map(|q| q.trim().to_string())
        .filter(|q| !q.is_empty())
}

/// Validate/normalize an issue `state` (`open`/`closed`/`all`, default `open`);
/// an invalid value throws → `-32603`.
pub(crate) fn parse_issue_state(state: Option<&str>) -> Result<String> {
    match state {
        None => Ok("open".to_string()),
        Some(s @ ("open" | "closed" | "all")) => Ok(s.to_string()),
        Some(_) => Err(Error::Internal(
            "state must be one of: open, closed, all".to_string(),
        )),
    }
}

/// The GitHub-native `(state, merged)` pair for a normalized [`PrState`]: the
/// raw forge `state` is only `open`/`closed`, with `merged` carried separately.
fn pr_state_words(state: PrState) -> (&'static str, bool) {
    match state {
        PrState::Open => ("open", false),
        PrState::Closed => ("closed", false),
        PrState::Merged => ("closed", true),
    }
}

/// Build the bare `GithubUser` shape from a login (avatar/profile URLs are not
/// carried by the host-agnostic model, so they default to empty strings).
fn user_json(login: &str) -> Value {
    json!({ "login": login, "avatarUrl": "", "htmlUrl": "" })
}

/// Render a forge [`PullRequest`] to the `GithubPullRequest` wire DTO (§5.27).
pub(crate) fn pull_to_json(pr: &PullRequest) -> Value {
    let (state, merged) = pr_state_words(pr.state);
    json!({
        "number": pr.number,
        "title": pr.title,
        "body": pr.body.clone().unwrap_or_default(),
        "state": state,
        "htmlUrl": pr.url,
        "createdAt": pr.created_at,
        "updatedAt": pr.updated_at,
        "user": user_json(&pr.author),
        "headRef": pr.source_branch,
        "baseRef": pr.target_branch,
        "headSha": pr.head_sha.clone().unwrap_or_default(),
        "baseSha": "",
        "merged": merged,
        "draft": pr.draft,
        "mergeable": pr.mergeable,
        "mergeableState": pr.mergeable_state,
        "labels": Vec::<String>::new(),
        "comments": 0,
        "reviewComments": 0,
        "commits": 0,
        "additions": 0,
        "deletions": 0,
        "changedFiles": 0,
    })
}

/// Render a forge [`Issue`] to the `GithubIssue` wire DTO (§5.27); `owner`/`repo`
/// are echoed for FE convenience.
pub(crate) fn issue_to_json(issue: &Issue, owner: &str, repo: &str) -> Value {
    json!({
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "state": issue.state,
        "htmlUrl": issue.url,
        "createdAt": issue.created_at,
        "updatedAt": issue.updated_at,
        "user": user_json(&issue.author),
        "labels": Vec::<String>::new(),
        "comments": 0,
        "owner": owner,
        "repo": repo,
    })
}

/// Render a REST inline [`ReviewComment`] to the `ReviewComment` wire DTO.
pub(crate) fn review_comment_to_json(rc: &ReviewComment) -> Value {
    json!({
        "id": rc.id,
        "body": rc.body,
        "path": rc.path,
        "line": rc.line,
        "user": { "login": rc.author },
        "createdAt": rc.created_at,
        "updatedAt": rc.updated_at,
        "inReplyToId": rc.in_reply_to_id,
        "htmlUrl": rc.url,
    })
}

/// Render a GraphQL [`ReviewThread`] to the `ReviewThread` wire DTO (`author`
/// nested as `{ login }`).
pub(crate) fn review_thread_to_json(t: &ReviewThread) -> Value {
    json!({
        "id": t.id,
        "isResolved": t.is_resolved,
        "comments": t.comments.iter().map(|c| json!({
            "id": c.id,
            "body": c.body,
            "author": { "login": c.author },
            "path": c.path,
            "line": c.line,
            "createdAt": c.created_at,
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_sourcecontrol::ReviewThreadComment;

    fn pr(state: PrState, draft: bool) -> PullRequest {
        PullRequest {
            number: 42,
            url: "https://github.com/o/r/pull/42".into(),
            title: "Add feature".into(),
            body: Some("body".into()),
            state,
            draft,
            source_branch: "feature/x".into(),
            target_branch: "main".into(),
            author: "octocat".into(),
            mergeable: Some(true),
            mergeable_state: Some("clean".into()),
            head_sha: Some("abc123".into()),
            created_at: "2026-01-01".into(),
            updated_at: "2026-01-02".into(),
        }
    }

    #[test]
    fn pull_dto_uses_camelcase_github_keys() {
        let v = pull_to_json(&pr(PrState::Open, false));
        assert_eq!(v["number"], json!(42));
        assert_eq!(v["state"], json!("open"));
        assert_eq!(v["merged"], json!(false));
        assert_eq!(v["htmlUrl"], json!("https://github.com/o/r/pull/42"));
        assert_eq!(v["headRef"], json!("feature/x"));
        assert_eq!(v["baseRef"], json!("main"));
        assert_eq!(v["headSha"], json!("abc123"));
        assert_eq!(v["user"]["login"], json!("octocat"));
        assert_eq!(v["mergeableState"], json!("clean"));
        assert_eq!(v["labels"], json!([]));
        assert_eq!(v["changedFiles"], json!(0));
    }

    #[test]
    fn merged_pull_maps_to_closed_state_with_merged_flag() {
        let v = pull_to_json(&pr(PrState::Merged, false));
        assert_eq!(v["state"], json!("closed"));
        assert_eq!(v["merged"], json!(true));
    }

    #[test]
    fn issue_dto_echoes_owner_repo_and_shapes_keys() {
        let issue = Issue {
            number: 7,
            title: "Bug".into(),
            body: Some("desc".into()),
            state: "open".into(),
            url: "https://github.com/o/r/issues/7".into(),
            author: "octocat".into(),
            created_at: "2026-01-01".into(),
            updated_at: "2026-01-02".into(),
        };
        let v = issue_to_json(&issue, "o", "r");
        assert_eq!(v["number"], json!(7));
        assert_eq!(v["htmlUrl"], json!("https://github.com/o/r/issues/7"));
        assert_eq!(v["owner"], json!("o"));
        assert_eq!(v["repo"], json!("r"));
        assert_eq!(v["labels"], json!([]));
        assert_eq!(v["user"]["login"], json!("octocat"));
        assert_eq!(v["createdAt"], json!("2026-01-01"));
        assert_eq!(v["updatedAt"], json!("2026-01-02"));
        assert_eq!(v["comments"], json!(0));
    }

    #[test]
    fn review_comment_dto_nests_user_and_renames_keys() {
        let rc = ReviewComment {
            id: 5,
            body: "nit".into(),
            path: "a.rs".into(),
            line: Some(1),
            author: "rev".into(),
            created_at: "2026".into(),
            updated_at: "2026".into(),
            in_reply_to_id: Some(4),
            url: "https://github.com/o/r/pull/42#discussion_r5".into(),
        };
        let v = review_comment_to_json(&rc);
        assert_eq!(v["id"], json!(5));
        assert_eq!(v["user"]["login"], json!("rev"));
        assert_eq!(v["inReplyToId"], json!(4));
        assert_eq!(
            v["htmlUrl"],
            json!("https://github.com/o/r/pull/42#discussion_r5")
        );
    }

    #[test]
    fn review_thread_dto_nests_author_login() {
        let t = ReviewThread {
            id: "RT1".into(),
            is_resolved: true,
            comments: vec![ReviewThreadComment {
                id: "c1".into(),
                body: "x".into(),
                author: "rev".into(),
                path: "a.rs".into(),
                line: Some(2),
                created_at: "2026".into(),
            }],
        };
        let v = review_thread_to_json(&t);
        assert_eq!(v["id"], json!("RT1"));
        assert_eq!(v["isResolved"], json!(true));
        assert_eq!(v["comments"][0]["author"]["login"], json!("rev"));
        assert_eq!(v["comments"][0]["line"], json!(2));
    }

    #[test]
    fn parses_states_filters_and_clamps_limit() {
        assert_eq!(parse_pr_state(None).unwrap(), None);
        assert_eq!(parse_pr_state(Some("all")).unwrap(), None);
        assert_eq!(parse_pr_state(Some("open")).unwrap(), Some(PrState::Open));
        assert!(parse_pr_state(Some("bad")).is_err());

        assert_eq!(parse_pr_involvement(None).unwrap(), None);
        assert_eq!(
            parse_pr_involvement(Some("review-requested")).unwrap(),
            Some(PrInvolvement::ReviewRequested)
        );
        assert!(parse_pr_involvement(Some("bad")).is_err());

        assert_eq!(parse_issue_state(None).unwrap(), "open");
        assert_eq!(parse_issue_state(Some("closed")).unwrap(), "closed");
        assert!(parse_issue_state(Some("bad")).is_err());

        assert_eq!(clamp_limit(None), 50);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(9000)), 200);
        assert_eq!(clamp_limit(Some(30)), 30);
    }

    #[test]
    fn issue_filter_rejects_pr_only_review_requested() {
        // The issues filter set (PROTOCOL §5) has no `review-requested`.
        assert!(parse_issue_filter(None).is_ok());
        assert!(parse_issue_filter(Some("all")).is_ok());
        assert!(parse_issue_filter(Some("assigned")).is_ok());
        assert!(parse_issue_filter(Some("created")).is_ok());
        assert!(parse_issue_filter(Some("involves")).is_ok());

        let err = parse_issue_filter(Some("review-requested")).unwrap_err();
        assert!(
            err.to_string().contains("all, assigned, created, involves"),
            "error must list the issues filter set: {err}"
        );
        assert!(parse_issue_filter(Some("bad")).is_err());
    }

    #[test]
    fn normalizes_free_text_search_query() {
        assert_eq!(normalize_search_query(None), None);
        assert_eq!(normalize_search_query(Some(String::new())), None);
        assert_eq!(normalize_search_query(Some("   ".into())), None);
        assert_eq!(
            normalize_search_query(Some("  login bug  ".into())),
            Some("login bug".to_string())
        );
    }

    #[test]
    fn next_token_round_trips_opaque_cursor() {
        // A REST page cursor survives an encode → decode round-trip.
        let tok = encode_next_token("2");
        assert_ne!(tok, "2", "wire token must be opaque, not the raw cursor");
        assert_eq!(decode_next_token(Some(&tok)).as_deref(), Some("2"));

        // A GraphQL end-cursor round-trips identically.
        let gql = encode_next_token("Y3Vyc29yOnYyOpK5");
        assert_eq!(
            decode_next_token(Some(&gql)).as_deref(),
            Some("Y3Vyc29yOnYyOpK5")
        );

        // Absent / malformed tokens decode to "start from the first page".
        assert_eq!(decode_next_token(None), None);
        assert_eq!(decode_next_token(Some("!!not-base64!!")), None);

        // `next_token_value` is the opaque string when paging continues, JSON
        // null on the last page.
        assert_eq!(next_token_value(None), Value::Null);
        let wire = next_token_value(Some("3"));
        assert!(wire.is_string());
        assert_eq!(decode_next_token(wire.as_str()).as_deref(), Some("3"));
    }
}
