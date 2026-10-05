use super::*;

pub(crate) fn repository() -> (
    tempfile::TempDir,
    NativeCheckoutSource,
    NativeCheckoutSelection,
    NativeCheckoutSelection,
) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("remote");
    let repo = Repository::init_opts(
        &path,
        git2::RepositoryInitOptions::new().initial_head("main"),
    )
    .unwrap();
    let signature = git2::Signature::now("Fixture", "fixture@example.invalid").unwrap();
    let tree = repo.index().unwrap().write_tree().unwrap();
    let first = repo
        .commit(
            Some("HEAD"),
            &signature,
            &signature,
            "main",
            &repo.find_tree(tree).unwrap(),
            &[],
        )
        .unwrap();
    let parent = repo.find_commit(first).unwrap();
    let feature = repo
        .commit(
            Some("refs/heads/feature/beyond-page-one"),
            &signature,
            &signature,
            "feature",
            &repo.find_tree(tree).unwrap(),
            &[&parent],
        )
        .unwrap();
    let source = NativeCheckoutSource {
        url: format!("file://{}", path.display()),
    };
    (
        temp,
        source,
        NativeCheckoutSelection::new("main", &first.to_string()).unwrap(),
        NativeCheckoutSelection::new("feature/beyond-page-one", &feature.to_string()).unwrap(),
    )
}

pub(crate) struct NoCredential;
impl NativeCheckoutCredentials for NoCredential {
    fn credential(&mut self, _: &str) -> std::result::Result<Cred, git2::Error> {
        panic!("local fixture never reads a secret")
    }
    fn rejected(&self) {
        panic!("local fixture has no authentication")
    }
}

#[test]
fn native_checkout_direct_and_cached_land_on_the_exact_nondefault_head() {
    let (temp, source, _, selected) = repository();
    let cache = temp.path().join("cache");
    clone_exact(&source, &cache, &selected, &mut NoCredential).unwrap();
    let direct = Repository::open(&cache).unwrap();
    assert_eq!(
        direct.head().unwrap().target().unwrap().to_string(),
        selected.commit_sha
    );
    assert_eq!(direct.head().unwrap().shorthand().unwrap(), selected.branch);
    let checkout = temp.path().join("checkout");
    from_cache(&source, &cache, &checkout, &selected).unwrap();
    let result = Repository::open(&checkout).unwrap();
    assert_eq!(
        result.head().unwrap().target().unwrap().to_string(),
        selected.commit_sha
    );
    assert_eq!(result.head().unwrap().shorthand().unwrap(), selected.branch);
    assert_eq!(
        result.find_remote("origin").unwrap().url().unwrap(),
        source.url()
    );
    std::fs::remove_dir_all(&cache).unwrap();
    assert!(result
        .find_commit(git2::Oid::from_str(&selected.commit_sha).unwrap())
        .is_ok());
}

#[test]
fn native_checkout_refuses_moved_branch_and_preserves_preexisting_destination() {
    let (temp, source, main, feature) = repository();
    let wrong = NativeCheckoutSelection::new(&feature.branch, &main.commit_sha).unwrap();
    let target = temp.path().join("wrong");
    assert!(clone_exact(&source, &target, &wrong, &mut NoCredential).is_err());
    assert!(!target.exists());
    std::fs::create_dir(&target).unwrap();
    std::fs::write(target.join("retained"), "original").unwrap();
    assert!(clone_exact(&source, &target, &feature, &mut NoCredential).is_err());
    assert_eq!(
        std::fs::read_to_string(target.join("retained")).unwrap(),
        "original"
    );
}

#[test]
fn native_checkout_fetch_checks_origin_and_never_resets_the_worktree() {
    let (temp, source, main, feature) = repository();
    let target = temp.path().join("checkout");
    clone_exact(&source, &target, &main, &mut NoCredential).unwrap();
    std::fs::write(target.join("user-work"), "keep").unwrap();
    fetch_exact(&source, &target, &feature, &mut NoCredential).unwrap();
    let repo = Repository::open(&target).unwrap();
    assert_eq!(
        repo.head().unwrap().target().unwrap().to_string(),
        main.commit_sha
    );
    assert_eq!(
        std::fs::read_to_string(target.join("user-work")).unwrap(),
        "keep"
    );
    repo.remote_set_url("origin", "https://foreign.invalid/group/project.git")
        .unwrap();
    assert!(fetch_exact(&source, &target, &feature, &mut NoCredential).is_err());
}

#[test]
fn native_checkout_requires_clean_https_and_observed_branch() {
    for url in [
        "http://git.example/a/b",
        "git@git.example:a/b",
        "https://user:secret@git.example/a/b",
        "https://git.example/a/../b",
        "https://git.example/a/%2e%2e/b",
        "https://git.example/a/b?x=secret",
        "https://git.example/a//b",
    ] {
        assert!(NativeCheckoutSource::https(url).is_err(), "{url}");
    }
    assert!(
        NativeCheckoutSource::https("https://git.example:8443/forge/group/sub/project.git").is_ok()
    );
    assert!(NativeCheckoutSelection::new("", "123").is_err());
    assert!(NativeCheckoutSelection::new("feature/x", "123").is_err());
}
