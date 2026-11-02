use super::*;

#[test]
fn checkout_resource_url_uses_target_project_and_full_instance_boundary() {
    let root = "https://forge.test:8443/gitlab";
    for suffix in [
        "",
        ".git",
        "/",
        "/-/merge_requests/32?view=parallel#note_19",
        "/-/issues/19?x=y#discussion",
    ] {
        assert_eq!(
            project_from_url(root, &format!("{root}/team/sub/app{suffix}")).unwrap(),
            "team/sub/app"
        );
    }
    for url in [
        "https://forge.test/gitlab/team/sub/app",
        "https://forge.test:8443/other/team/sub/app",
        "https://forge.test:8443/gitlab-copy/team/sub/app",
        "https://forge.test:8443/gitlab/team/sub/app/-/work_items/7",
        "https://forge.test:8443/gitlab/team/sub/app/-/merge_requests/7/diffs",
        "https://forge.test:8443/gitlab/team%2Fsub/app",
        "https://token@forge.test:8443/gitlab/team/sub/app",
        "https://forge.test:8443/gitlab/team/sub/app/-/issues/0",
    ] {
        assert!(project_from_url(root, url).is_err(), "{url}");
    }
}

/// The payload sweep cannot reach the checkout reads (they need a live
/// checkout lease), so their page-size clamp is pinned here: one page is
/// 1–100 items, default 50.
#[test]
fn checkout_page_size_is_capped_at_100() {
    assert_eq!(page_scope(None, None, None, false).unwrap().limit, 50);
    assert_eq!(page_scope(None, None, Some(100), false).unwrap().limit, 100);
    for limit in [0, 101, 1_000, u32::MAX] {
        assert!(
            page_scope(None, None, Some(limit), false).is_err(),
            "limit {limit} must be refused"
        );
    }
}
