use super::*;
use serde_json::json;

#[test]
fn checkout_queries_cannot_supply_workspace_or_credential_authority() {
    for key in [
        "workspaceId",
        "principalId",
        "token",
        "cachePath",
        "connectionId",
    ] {
        let mut value = json!({"checkoutId":"original", "revision":"original"});
        value[key] = json!("untrusted");
        assert!(
            serde_json::from_value::<CheckoutProjectsQuery>(value).is_err(),
            "{key}"
        );
    }
    let selection = json!({"checkoutId":"original","revision":"original","projectPath":"team/sub/app","branch":"release/next","commitSha":"0123456789012345678901234567890123456789","mode":"cached"});
    assert_eq!(
        serde_json::from_value::<CheckoutSelection>(selection.clone())
            .unwrap()
            .branch,
        "release/next"
    );
    for key in ["branch", "commitSha", "revision"] {
        let mut value = selection.clone();
        value.as_object_mut().unwrap().remove(key);
        assert!(
            serde_json::from_value::<CheckoutSelection>(value).is_err(),
            "{key}"
        );
    }
}

#[test]
fn checkout_missing_default_stays_absent_and_failure_contains_no_payload() {
    let project = CheckoutProject {
        project_path: "team/app".into(),
        name: "app".into(),
        namespace: "team".into(),
        web_url: "https://forge.test/root/team/app".into(),
        clone_url: "https://forge.test/root/team/app.git".into(),
        default_branch: None,
        owner_avatar_url: None,
    };
    let value = serde_json::to_value(project).unwrap();
    assert!(value.get("defaultBranch").is_none());
    assert!(value.get("ownerAvatarUrl").is_none());
    let result: CheckoutResult<CheckoutProjects> =
        CheckoutResult::unavailable(CheckoutUnavailable::AccessDenied);
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        json!({"status":"unavailable","reason":"access-denied"})
    );
}

#[test]
fn owner_avatar_capture_opt_in_is_optional_boolean_and_cannot_be_changed_on_a_page() {
    let legacy = json!({"provider":"gitlab","instanceBaseUrl":"https://forge.test/root"});
    let query: CheckoutCaptureQuery = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(query.include_owner_avatar, None);
    assert_eq!(serde_json::to_value(query).unwrap(), legacy);
    for enabled in [false, true] {
        let mut value = legacy.clone();
        value["includeOwnerAvatar"] = json!(enabled);
        let query: CheckoutCaptureQuery = serde_json::from_value(value).unwrap();
        assert_eq!(query.include_owner_avatar, Some(enabled));
    }
    for invalid in [json!(1), json!("true"), json!([]), json!({})] {
        let mut value = legacy.clone();
        value["includeOwnerAvatar"] = invalid;
        assert!(serde_json::from_value::<CheckoutCaptureQuery>(value).is_err());
    }
    assert!(serde_json::from_value::<CheckoutProjectsQuery>(
        json!({"checkoutId":"original","revision":"original","includeOwnerAvatar":true})
    )
    .is_err());
}
