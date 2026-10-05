//! Real Git repositories; the narrow original-authority test implementation is
//! injected. Services/actual caller composition is tested by their owners.
use super::*;
use crate::native_checkout::tests::{repository, NoCredential};
use std::sync::Mutex;

struct Owner(Mutex<bool>);
impl Owner {
    fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(true)))
    }
    fn retire(&self) {
        *self.0.lock().unwrap() = false;
    }
}
impl NativeCacheAuthority for Owner {
    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        let current = self.0.lock().unwrap();
        if !*current {
            return Err(Error::GitAuthorization("original owner retired".into()));
        }
        transfer()
    }
}

#[tokio::test]
async fn qualified_cache_fresh_a_then_denied_b_never_discloses_branches_or_checkout() {
    let (temp, source, _, selected) = repository();
    let root = crate::repo_cache::cache_root_for(temp.path());
    let a = Owner::new();
    let cache_a =
        QualifiedRepositoryCache::new(&root, source.clone(), "same-url-slot", a.clone()).unwrap();
    cache_a
        .ensure(selected.clone(), Box::new(NoCredential), None)
        .await
        .unwrap();
    assert!(cache_a
        .branches()
        .await
        .unwrap()
        .unwrap()
        .branches
        .contains(&selected));
    let b = Owner::new();
    let cache_b =
        QualifiedRepositoryCache::new(&root, source.clone(), "same-url-slot", b.clone()).unwrap();
    a.retire();
    b.retire();
    assert!(cache_a.branches().await.is_err());
    assert!(cache_b.branches().await.is_err());
    assert!(cache_b
        .ensure(selected.clone(), Box::new(NoCredential), None)
        .await
        .is_err());
    let denied = temp.path().join("denied-checkout");
    assert!(cache_b
        .checkout(denied.clone(), selected.clone())
        .await
        .is_err());
    assert!(!denied.exists());
    let recovered =
        QualifiedRepositoryCache::new(&root, source, "same-url-slot", Owner::new()).unwrap();
    recovered
        .ensure(selected.clone(), Box::new(NoCredential), None)
        .await
        .unwrap();
    let checkout = temp.path().join("recovered-checkout");
    recovered
        .checkout(checkout.clone(), selected.clone())
        .await
        .unwrap();
    assert_eq!(
        git2::Repository::open(checkout)
            .unwrap()
            .head()
            .unwrap()
            .target()
            .unwrap()
            .to_string(),
        selected.commit_sha
    );
}

#[tokio::test]
async fn qualified_cache_late_a_warm_cannot_publish_after_retirement() {
    let (temp, source, _, selected) = repository();
    let root = crate::repo_cache::cache_root_for(temp.path());
    let owner = Owner::new();
    let cache = QualifiedRepositoryCache::new(&root, source, "original", owner.clone()).unwrap();
    let retiring = owner.clone();
    let progress = Arc::new(move |event| {
        if matches!(event, CacheEnsureEvent::Step("clone")) {
            retiring.retire();
        }
    });
    assert!(cache
        .ensure(selected, Box::new(NoCredential), Some(progress))
        .await
        .is_err());
    assert!(!super::super::is_fresh(
        &cache.path,
        std::time::Duration::from_secs(60)
    ));
    assert!(cache.branches().await.is_err());
}

#[tokio::test]
async fn qualified_cache_isolates_full_source_connection_and_selected_head() {
    let (temp, source, main, feature) = repository();
    let root = crate::repo_cache::cache_root_for(temp.path());
    let first =
        QualifiedRepositoryCache::new(&root, source.clone(), "account-a", Owner::new()).unwrap();
    let second =
        QualifiedRepositoryCache::new(&root, source.clone(), "account-b", Owner::new()).unwrap();
    assert_ne!(first.path, second.path);
    let other =
        NativeCheckoutSource::https("https://git.example:8443/other/group/project.git").unwrap();
    let third = QualifiedRepositoryCache::new(&root, other, "account-a", Owner::new()).unwrap();
    assert_ne!(first.path, third.path);
    first
        .ensure(main.clone(), Box::new(NoCredential), None)
        .await
        .unwrap();
    first
        .ensure(feature.clone(), Box::new(NoCredential), None)
        .await
        .unwrap();
    let output = temp.path().join("feature");
    first
        .checkout(output.clone(), feature.clone())
        .await
        .unwrap();
    let repo = git2::Repository::open(output).unwrap();
    assert_eq!(
        repo.head().unwrap().target().unwrap().to_string(),
        feature.commit_sha
    );
    assert_eq!(repo.head().unwrap().shorthand().unwrap(), feature.branch);
    assert!(second.branches().await.unwrap().is_none());
}

struct CountedOwner {
    calls: Mutex<usize>,
    fail_at: Mutex<Option<usize>>,
}
impl NativeCacheAuthority for CountedOwner {
    fn with_current(&self, transfer: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        let mut calls = self.calls.lock().unwrap();
        *calls += 1;
        if *self.fail_at.lock().unwrap() == Some(*calls) {
            return Err(Error::GitAuthorization(
                "original owner retired at transfer".into(),
            ));
        }
        transfer()
    }
}

#[tokio::test]
async fn qualified_cache_removes_owned_destination_on_worker_or_caller_final_refusal() {
    for failure in [3, 4] {
        let (temp, source, _, selected) = repository();
        let owner = Arc::new(CountedOwner {
            calls: Mutex::new(0),
            fail_at: Mutex::new(None),
        });
        let cache = QualifiedRepositoryCache::new(
            &crate::repo_cache::cache_root_for(temp.path()),
            source,
            "original",
            owner.clone(),
        )
        .unwrap();
        cache
            .ensure(selected.clone(), Box::new(NoCredential), None)
            .await
            .unwrap();
        *owner.calls.lock().unwrap() = 0;
        *owner.fail_at.lock().unwrap() = Some(failure);
        let destination = temp.path().join("refused-destination");
        assert!(cache.checkout(destination.clone(), selected).await.is_err());
        assert_eq!(*owner.calls.lock().unwrap(), failure);
        assert!(
            !destination.exists(),
            "only the newly created destination is removed"
        );
    }
}
