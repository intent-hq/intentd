use super::*;
fn fixture() -> (crate::testutil::TempDir, Repository, String, Vec<String>) {
    let dir = crate::testutil::init_repo("native-push");
    let repo = Repository::init_opts(
        dir.path(),
        git2::RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    repo.set_head("refs/heads/main").unwrap();
    let tree = repo.index().unwrap().write_tree().unwrap();
    let sig = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
    let sha = repo
        .commit(
            Some("HEAD"),
            &sig,
            &sig,
            "fixture",
            &repo.find_tree(tree).unwrap(),
            &[],
        )
        .unwrap()
        .to_string();
    let urls: Vec<String> = vec!["https://127.0.0.1:9/group/project.git".into()];
    repo.remote("forge", &urls[0]).unwrap();
    (dir, repo, sha, urls)
}
#[test]
fn native_push_complete_destinations_ref_and_transport_are_prepared_before_credential() {
    let (dir, repo, sha, urls) = fixture();
    for url in [
        "git@example.invalid:group/project.git",
        "http://127.0.0.1/group/project.git",
        "https://user:secret@example.invalid/group/project.git",
        "https://example.invalid/group/project.git?secret=x",
    ] {
        repo.remote_set_url("forge", url).unwrap();
        let actual = vec![url.into()];
        assert!(PreparedNativePush::prepare(
            dir.path(),
            "forge",
            "refs/heads/main",
            &sha,
            &actual,
            &actual
        )
        .is_err());
    }
    repo.remote_set_url("forge", &urls[0]).unwrap();
    let prepared =
        PreparedNativePush::prepare(dir.path(), "forge", "refs/heads/main", &sha, &urls, &urls)
            .unwrap();
    repo.config()
        .unwrap()
        .set_str(
            "remote.forge.pushurl",
            "https://127.0.0.1:9/foreign/project.git",
        )
        .unwrap();
    assert!(prepared
        .execute(
            || panic!("changed destination released credential"),
            |_| panic!("no effect")
        )
        .is_err());
}
#[test]
fn native_push_ref_movement_and_url_rewrite_refuse_original_action() {
    let (dir, repo, sha, urls) = fixture();
    let original =
        PreparedNativePush::prepare(dir.path(), "forge", "refs/heads/main", &sha, &urls, &urls)
            .unwrap();
    repo.set_head_detached(git2::Oid::from_str(&sha).unwrap())
        .unwrap();
    assert!(original
        .execute(|| panic!("changed reference"), |_| panic!("no effect"))
        .is_err());
    repo.set_head("refs/heads/main").unwrap();
    repo.config()
        .unwrap()
        .set_str(
            "url.https://foreign.invalid/.insteadOf",
            "https://127.0.0.1:9/",
        )
        .unwrap();
    assert!(PreparedNativePush::prepare(
        dir.path(),
        "forge",
        "refs/heads/main",
        &sha,
        &urls,
        &urls
    )
    .is_err());
}
