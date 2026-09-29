use super::*;
use crate::WorkspaceApi;

struct OrdinaryApi;
impl WorkspaceApi for OrdinaryApi {}

#[test]
fn existing_api_has_no_repository_read_connection_for_either_entry() {
    let api: &dyn WorkspaceApi = &OrdinaryApi;
    for entry in [
        RepositoryWireEntry::Bearer,
        RepositoryWireEntry::AdmittedLocal,
    ] {
        assert!(api.repository_read_connection(entry).is_none());
    }
}

#[test]
fn native_review_strict_frame_keeps_root_plan_and_rejects_injected_authority() {
    use super::{NativeReviewBoundQuery, NativeReviewExecuteQuery, NativeReviewPrepareQuery};
    let base = serde_json::json!({"workspaceId":"w","action":"create-pr","review":{"root":{"workspaceId":"w","kind":"primary"},"choice":{"kind":"saved"},"targetBranch":"trunk"}});
    let decoded: NativeReviewPrepareQuery = serde_json::from_value(base.clone()).unwrap();
    assert_eq!(decoded.review.root.workspace_id.as_str(), "w");
    for key in ["caller", "provider", "account", "repositoryLifetimeId"] {
        let mut extra = base.clone();
        extra[key] = serde_json::json!("injected");
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(extra).is_err());
    }
    let mut malformed = base;
    malformed["review"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<NativeReviewPrepareQuery>(malformed).is_err());
    let execute = serde_json::json!({"workspaceId":"w","action":"commit","review":{"operationId":"original","root":{"workspaceId":"w","kind":"registered","gitRootId":"r"}},"commitMessage":"m"});
    assert!(serde_json::from_value::<NativeReviewExecuteQuery>(execute.clone()).is_ok());
    let mut changed = execute;
    changed["files"] = serde_json::json!(["new"]);
    assert!(serde_json::from_value::<NativeReviewExecuteQuery>(changed).is_err());
    assert!(serde_json::from_value::<NativeReviewBoundQuery>(serde_json::json!({"workspaceId":"w","operationId":"original","root":{"workspaceId":"w","kind":"primary"},"retry":true})).is_err());
}

#[test]
fn native_review_nested_root_and_target_never_accept_authority_or_extra_ids() {
    use super::{NativeReviewBoundQuery, NativeReviewPrepareQuery};
    use serde_json::json;
    let base = json!({"workspaceId":"w","action":"create-pr","review":{"root":{"workspaceId":"w","kind":"primary"},"choice":{"kind":"explicitTarget","target":{"provider":"gitlab","instanceBaseUrl":"https://gitlab.test","projectPath":"group/repo"}},"targetBranch":"main"}});
    assert!(serde_json::from_value::<NativeReviewPrepareQuery>(base.clone()).is_ok());
    for key in ["gitRootId", "principalId", "accountId", "generation"] {
        let mut value = base.clone();
        value["review"]["root"][key] = json!("forged");
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(value).is_err());
    }
    for key in ["accountId", "connection", "providerProjectId"] {
        let mut value = base.clone();
        value["review"]["choice"]["target"][key] = json!("forged");
        assert!(serde_json::from_value::<NativeReviewPrepareQuery>(value).is_err());
    }
    assert!(serde_json::from_value::<NativeReviewBoundQuery>(json!({"workspaceId":"w","operationId":"o","root":{"workspaceId":"w","kind":"registered","gitRootId":"r","principalId":"forged"}})).is_err());
}
