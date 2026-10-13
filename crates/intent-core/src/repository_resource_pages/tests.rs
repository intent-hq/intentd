use super::*;
use serde_json::{json, Value};

fn list() -> Value {
    json!({
        "context":{"kind":"host"}, "readLifetimeId":"original",
        "repository":{"provider":"gitlab", "instanceBaseUrl":"https://forge.test:8443/install", "projectPath":"Team/Sub/Project"},
        "kind":"merge-request", "query":{"state":"closed", "filter":"all", "search":"needle", "limit":50}
    })
}

#[test]
fn context_is_explicit_and_legacy_workspace_capture_stays_strict() {
    use crate::repository_request::RepositoryResourceQuery;
    assert!(serde_json::from_value::<RepositoryResourceQuery>(json!({"workspaceId":"w"})).is_ok());
    assert!(
        serde_json::from_value::<RepositoryResourceQuery>(json!({"context":{"kind":"host"}}))
            .is_err()
    );
    for context in [
        json!({"kind":"host","workspaceId":"borrowed"}),
        json!({"kind":"workspace"}),
        json!({"kind":"workspace","workspaceId":"w","owner":true}),
        json!({"kind":"host","accountId":"substitute"}),
    ] {
        assert!(serde_json::from_value::<RepositoryReadContext>(context).is_err());
    }
    let request: RepositoryResourceListQuery = serde_json::from_value(list()).unwrap();
    assert_eq!(request.context, RepositoryReadContext::Host {});
    assert_eq!(
        request.repository.instance_base_url,
        "https://forge.test:8443/install"
    );
    assert_eq!(request.repository.project_path, "Team/Sub/Project");
    assert_eq!(request.query.state, RepositoryListState::Closed);
}

#[test]
fn collection_request_rejects_scope_expansion_and_unbounded_parameters() {
    let invalid = [
        ("/repository/provider", json!("github")),
        ("/repository/projectPath", json!("")),
        ("/query/limit", json!(0)),
        ("/query/limit", json!(101)),
        ("/query/state", json!("everything")),
        ("/query/filter", json!("author-or-assignee")),
        ("/query/search", json!("x".repeat(513))),
        ("/readLifetimeId", json!("")),
    ];
    for (pointer, value) in invalid {
        let mut request = list();
        *request.pointer_mut(pointer).unwrap() = value;
        assert!(
            serde_json::from_value::<RepositoryResourceListQuery>(request).is_err(),
            "{pointer}"
        );
    }
    for (field, value) in [
        ("cursor", json!(null)),
        ("cursor", json!("")),
        ("cursor", json!("x".repeat(129))),
        ("extraRepos", json!(["unadmitted/project"])),
        ("accountId", json!("other")),
    ] {
        let mut request = list();
        request["query"][field] = value;
        assert!(
            serde_json::from_value::<RepositoryResourceListQuery>(request).is_err(),
            "{field}"
        );
    }
}

#[test]
fn discussions_require_original_merge_request_identity_and_bounded_page() {
    let mut query = json!({
        "context":{"kind":"workspace","workspaceId":"actual"},"readLifetimeId":"original",
        "target":{"repository":list()["repository"],"kind":"merge-request","number":7},
        "page":{"limit":100,"cursor":"original-continuation"}
    });
    assert!(serde_json::from_value::<RepositoryDiscussionsQuery>(query.clone()).is_ok());
    query["target"]["kind"] = json!("issue");
    assert!(serde_json::from_value::<RepositoryDiscussionsQuery>(query.clone()).is_err());
    query["target"]["kind"] = json!("merge-request");
    query["target"]["number"] = json!(0);
    assert!(serde_json::from_value::<RepositoryDiscussionsQuery>(query).is_err());
}

#[test]
fn empty_page_with_continuation_preserves_provider_end_of_page_truth() {
    let outcome = RepositoryPageOutcome::Page {
        items: Vec::new(),
        next_cursor: Some("continue".into()),
        omitted: 0,
    };
    assert_eq!(
        serde_json::to_value(outcome).unwrap(),
        json!({
            "kind":"page", "items":[], "nextCursor":"continue", "omitted":0
        })
    );
}

#[test]
fn unsupported_pages_and_invalidation_coverage_are_explicit() {
    let value = serde_json::to_value(RepositoryPageOutcome::Failure {
        code: RepositoryPageFailure::Unsupported,
        status: None,
    })
    .unwrap();
    assert_eq!(
        value,
        json!({"kind":"failure","code":"unsupported","status":null})
    );
    assert!(serde_json::from_value::<RepositoryResourceFailure>(json!("unsupported")).is_err());
    assert_eq!(
        serde_json::to_value(RepositoryInvalidationCoverage::List {
            resource_kind: RepositoryListKind::MergeRequest,
        })
        .unwrap(),
        json!({"kind":"list","resourceKind":"merge-request"})
    );
    assert!(
        serde_json::from_value::<RepositoryInvalidationCoverage>(json!({"kind":"list"})).is_err()
    );
    assert!(serde_json::from_value::<RepositoryInvalidationCoverage>(
        json!({"kind":"project","target":null})
    )
    .is_err());
}

#[test]
fn known_repository_identity_requires_an_original_bounded_path_query() {
    let mut input =
        json!({"context":{"kind":"host"},"readLifetimeId":"original","path":"/actual/repo"});
    assert!(serde_json::from_value::<RepositoryKnownQuery>(input.clone()).is_ok());
    input["workspaceId"] = json!("borrowed");
    assert!(serde_json::from_value::<RepositoryKnownQuery>(input.clone()).is_err());
    input.as_object_mut().unwrap().remove("workspaceId");
    for path in [String::new(), "x".repeat(4097), "bad\npath".to_string()] {
        input["path"] = json!(path);
        assert!(serde_json::from_value::<RepositoryKnownQuery>(input.clone()).is_err());
    }
}
