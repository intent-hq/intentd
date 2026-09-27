use super::*;
use std::sync::{Condvar, Mutex};

#[derive(Default)]
struct Observer {
    before: Mutex<Option<(FileSecretStore, String)>>,
    deny: bool,
    users: Mutex<Vec<Option<GitlabUser>>>,
    outcomes: Mutex<Vec<GitlabWriteOutcome>>,
    entered: tokio::sync::Notify,
    done: tokio::sync::Notify,
    paused: Mutex<bool>,
    release: Condvar,
}
impl GitlabWriteObserver for Observer {
    fn before_write(&self) -> Result<()> {
        if self.deny {
            return Err(Error::AdmissionRetired);
        }
        if let Some((store, expected)) = self.before.lock().unwrap().take() {
            assert_eq!(store.load(SECRET_ACCOUNT).unwrap(), Some(expected));
        }
        self.entered.notify_one();
        let mut paused = self.paused.lock().unwrap();
        while *paused {
            paused = self.release.wait(paused).unwrap();
        }
        Ok(())
    }
    fn verified_user(&self, user: Option<GitlabUser>) {
        self.users.lock().unwrap().push(user);
    }
    fn settled(&self, outcome: GitlabWriteOutcome) {
        self.outcomes.lock().unwrap().push(outcome);
        self.done.notify_one();
    }
}

#[tokio::test]
async fn observed_pat_fences_before_effect_and_reports_actual_sibling_completion() {
    let (_dir, store) = temp_store();
    store.store(SECRET_ACCOUNT, "old").unwrap();
    store.store(REFRESH_SECRET_ACCOUNT, "old-refresh").unwrap();
    store.store(EXPIRES_AT_SECRET_ACCOUNT, "1").unwrap();
    let observer = Arc::new(Observer {
        before: Mutex::new(Some((store.clone(), "old".into()))),
        ..Observer::default()
    });
    persist_gitlab_token_observed(
        store.clone(),
        SecretString::from("new"),
        Arc::new(()),
        observer.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        *observer.outcomes.lock().unwrap(),
        [GitlabWriteOutcome::Persisted]
    );
    assert_eq!(store.load(SECRET_ACCOUNT).unwrap().as_deref(), Some("new"));
    assert_eq!(store.load(REFRESH_SECRET_ACCOUNT).unwrap(), None);
    assert_eq!(store.load(EXPIRES_AT_SECRET_ACCOUNT).unwrap(), None);
    revoke_gitlab_token_observed(store.clone(), Arc::new(()), observer.clone())
        .await
        .unwrap();
    assert_eq!(store.load(SECRET_ACCOUNT).unwrap(), None);
    assert_eq!(observer.outcomes.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn observed_rejected_owner_never_writes_or_claims_settlement() {
    let (_dir, store) = temp_store();
    store.store(SECRET_ACCOUNT, "unchanged").unwrap();
    let observer = Arc::new(Observer {
        deny: true,
        ..Observer::default()
    });
    assert!(matches!(
        persist_gitlab_token_observed(
            store.clone(),
            SecretString::from("rejected"),
            Arc::new(()),
            observer.clone()
        )
        .await,
        Err(Error::AdmissionRetired)
    ));
    assert!(observer.outcomes.lock().unwrap().is_empty());
    assert_eq!(
        store.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("unchanged")
    );
}

#[tokio::test]
async fn observed_timeout_keeps_original_writer_and_lease_until_actual_completion() {
    let (_dir, store) = temp_store();
    let observer = Arc::new(Observer {
        paused: Mutex::new(true),
        ..Observer::default()
    });
    let lease = Arc::new(());
    let weak = Arc::downgrade(&lease);
    let output = store.clone();
    let completion = observer.clone();
    let work = tokio::spawn(async move {
        run_blocking_observed(
            move || output.store(SECRET_ACCOUNT, "after-timeout"),
            "persist",
            Duration::from_millis(20),
            Some(lease),
            Some(completion),
        )
        .await
    });
    observer.entered.notified().await;
    assert!(work.await.unwrap().is_err());
    assert!(observer.outcomes.lock().unwrap().is_empty());
    assert!(weak.upgrade().is_some());
    assert_eq!(store.load(SECRET_ACCOUNT).unwrap(), None);
    *observer.paused.lock().unwrap() = false;
    observer.release.notify_all();
    observer.done.notified().await;
    assert_eq!(
        store.load(SECRET_ACCOUNT).unwrap().as_deref(),
        Some("after-timeout")
    );
    assert_eq!(
        *observer.outcomes.lock().unwrap(),
        [GitlabWriteOutcome::Persisted]
    );
}

#[tokio::test]
async fn observed_partial_failure_and_panic_are_uncertain_without_rollback() {
    for panic in [false, true] {
        let (_dir, store) = temp_store();
        let observer = Arc::new(Observer::default());
        let output = store.clone();
        let result = run_blocking_observed(
            move || {
                output.store(SECRET_ACCOUNT, "partial")?;
                assert!(!panic, "controlled writer panic");
                Err(intent_core::Error::Internal(
                    "controlled sibling failure".into(),
                ))
            },
            "persist",
            Duration::from_secs(1),
            Some(Arc::new(())),
            Some(observer.clone()),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            store.load(SECRET_ACCOUNT).unwrap().as_deref(),
            Some("partial")
        );
        assert_eq!(
            *observer.outcomes.lock().unwrap(),
            [GitlabWriteOutcome::Uncertain]
        );
    }
}

#[tokio::test]
async fn observed_refresh_reports_verified_account_without_exporting_the_pair() {
    for accepted in [true, false] {
        let mock = spawn_mock(Arc::new(move |method, path, _body| match (method, path) {
            ("POST", "/oauth/token") => (200, json!({"access_token":"rotated","refresh_token":"rotated-refresh","expires_in":7200})),
            ("GET", "/api/v4/user") if accepted => (200, user_body()),
            _ => (401, json!({"error":"rejected"})),
        })).await;
        let (_dir, store) = temp_store();
        store.store(REFRESH_SECRET_ACCOUNT, "old-refresh").unwrap();
        let observer = Arc::new(Observer::default());
        refresh_access_token_observed(
            &mock.host,
            "client-1",
            store.clone(),
            Arc::new(()),
            observer.clone(),
        )
        .await
        .unwrap();
        assert_eq!(observer.users.lock().unwrap()[0].is_some(), accepted);
        assert_eq!(
            store.load(SECRET_ACCOUNT).unwrap().as_deref(),
            Some("rotated")
        );
        assert_eq!(
            *observer.outcomes.lock().unwrap(),
            [GitlabWriteOutcome::Persisted]
        );
    }
}
