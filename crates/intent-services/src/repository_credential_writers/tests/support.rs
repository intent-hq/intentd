use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{ExecutionScope, RepositoryProvider, RepositoryTarget};
use intent_sourcecontrol::{GitlabDescriptor, GitlabInstance, SecretString};
use tokio::sync::{Notify, Semaphore};

use crate::repository_credential_writers::*;
use crate::repository_credentials::authority::{
    CredentialFuture, RepositoryAuthorityFence, RepositoryCredentialTransport,
};
use crate::repository_credentials::*;

pub const INSTANCE: &str = "https://git.example:8443/forge";
pub const PROJECT: &str = "team/nested/project";

pub struct Pause {
    pub entered: Notify,
    pub release: Semaphore,
}
impl Pause {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    pub async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
}

#[derive(Default)]
pub struct Secrets {
    pub value: Mutex<String>,
    pub writes: AtomicUsize,
    pub reads: Mutex<Vec<RepositorySecretRequest>>,
    pub pause: Mutex<Option<Arc<Pause>>>,
}
impl Secrets {
    pub fn persist(&self, value: &str) {
        *self.value.lock().unwrap() = value.into();
        self.writes.fetch_add(1, Ordering::SeqCst);
    }
}
impl RepositorySecretReader for Secrets {
    fn load<'a>(
        &'a self,
        expected: &'a RepositorySecretRequest,
    ) -> CredentialFuture<'a, RepositorySecretSnapshot> {
        Box::pin(async move {
            self.reads.lock().unwrap().push(expected.clone());
            let token = SecretString::from(self.value.lock().unwrap().clone());
            let pause = self.pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.wait().await;
            }
            Ok(RepositorySecretSnapshot {
                request: expected.clone(),
                token,
            })
        })
    }
}
struct Authority;
struct Fence;
impl RepositoryAuthority for Authority {
    fn revalidate<'a>(
        &'a self,
        _: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async { Ok(Box::new(Fence) as Box<dyn RepositoryAuthorityFence>) })
    }
}
impl RepositoryAuthorityFence for Fence {
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        action()
    }
}

pub struct Test {
    pub directory: Arc<RepositoryConnectionDirectory>,
    pub writers: RepositoryCredentialWriters,
    pub verified: VerifiedRepositoryAccount,
    pub descriptor: GitlabDescriptor,
    pub secrets: Arc<Secrets>,
}
impl Test {
    pub fn new() -> Self {
        let descriptor = GitlabDescriptor::new(GitlabInstance::parse(INSTANCE).unwrap());
        let directory = Arc::new(RepositoryConnectionDirectory::new("daemon-A".into()));
        let test = Self {
            writers: RepositoryCredentialWriters::new(directory.clone()),
            directory,
            verified: verified(
                descriptor.clone(),
                71,
                RepositoryCredentialSource::GitlabSecretSlot,
            ),
            descriptor,
            secrets: Arc::new(Secrets::default()),
        };
        test.secrets.persist("old-local-token");
        test.begin(RepositoryMutationKind::Replace)
            .complete(SettledCredentialState::Verified(test.verified.clone()))
            .unwrap();
        test
    }
    pub fn begin(&self, kind: RepositoryMutationKind) -> RepositoryWriterMutation {
        self.writers
            .reserve(kind)
            .unwrap()
            .begin(|| Ok(RepositoryWriterPreflight::Change))
            .unwrap()
            .unwrap()
    }
    pub fn request(&self, use_kind: RepositoryCredentialUse) -> RepositoryAuthorityRequest {
        let binding = self.directory.binding().unwrap();
        RepositoryAuthorityRequest {
            execution: ExecutionScope {
                daemon_id: "daemon-A".into(),
                authority_scope_id: "original-root".into(),
                authority_generation: 1,
            },
            target: RepositoryTarget {
                provider: RepositoryProvider::Gitlab,
                instance_base_url: binding.account.instance_base_url.clone(),
                project_path: PROJECT.into(),
            },
            connection: binding.scope,
            use_kind,
            allowed_transport: if use_kind == RepositoryCredentialUse::ChildGit {
                RepositoryCredentialTransport::GitHttps(vec![format!("{INSTANCE}/{PROJECT}.git")])
            } else {
                RepositoryCredentialTransport::GitlabApi(self.descriptor.clone())
            },
        }
    }
    pub fn admit(
        &self,
        use_kind: RepositoryCredentialUse,
    ) -> Result<RepositoryCredentialAdmission> {
        self.directory.admit(
            &self.directory.binding()?,
            self.request(use_kind),
            Arc::new(Authority),
        )
    }
    pub fn native(&self) -> RepositoryCredentialAdmission {
        self.admit(RepositoryCredentialUse::NativeRead).unwrap()
    }
    pub async fn acquire(
        &self,
        admission: &RepositoryCredentialAdmission,
    ) -> Result<RepositoryCredentialTicket> {
        self.directory
            .acquire_exact(admission, self.secrets.as_ref(), Duration::from_secs(2))
            .await
    }
    pub fn policy(&self) -> RepositoryChildPolicyMutation {
        self.writers
            .reserve_child_policy(self.directory.binding().unwrap())
            .unwrap()
            .begin(|| Ok(RepositoryWriterPreflight::Change))
            .unwrap()
            .unwrap()
    }
}
pub fn verified(
    descriptor: GitlabDescriptor,
    id: u64,
    source: RepositoryCredentialSource,
) -> VerifiedRepositoryAccount {
    VerifiedRepositoryAccount::from_verified_user(descriptor, id, source).unwrap()
}
