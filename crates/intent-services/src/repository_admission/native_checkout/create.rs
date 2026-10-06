//! Exact checkout selection carried into workspace provisioning. The filesystem
//! worker and admitted insert remain owned if their waiting RPC is cancelled.
use std::path::{Path, PathBuf};

use intent_core::{Workspace, WorkspaceCreate, WorkspaceId};
use intent_git::native_checkout::{NativeCheckoutSelection, NativeCheckoutSource};
use sha2::{Digest, Sha256};

use super::{
    denied, load_project, unavailable, with_caller, with_wire_credential, Arc, BoxFuture,
    CheckoutFrame, CheckoutMode, CheckoutSelection, Engine, Error, GitlabCheckoutConnection, Lease,
    Mutex, Project, Request, Result, Services, WireCredential, CAPTURE_LIMIT,
};

pub(crate) struct Plan {
    request: Arc<Request>,
    lease: Arc<Lease>,
    provider: Arc<GitlabCheckoutConnection>,
    project: Project,
    selection: CheckoutSelection,
    destination: Mutex<Option<Destination>>,
}

struct Destination {
    path: PathBuf,
    published: bool,
}
impl Drop for Destination {
    fn drop(&mut self) {
        if !self.published {
            // Only the exclusive, previously absent directory created by this
            // worker belongs to this guard. Never delete a supplied repository.
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn shape(input: &WorkspaceCreate) -> Result<()> {
    if input.github_url.is_some()
        || input.repository_path.is_some()
        || input.repository_owner.is_some()
        || input.repository_name.is_some()
        || input.worktree_path.is_some()
        || input.clone_path.is_some()
        || input.base_ref.is_some()
        || input.base_commit_sha.is_some()
        || input.remote.is_some()
        || input.path.is_some()
        || input.is_remote == Some(true)
        || input.is_new_repo == Some(true)
        || input.skip_isolation == Some(true)
    {
        return Err(Error::InvalidParams(
            "repositoryCheckout cannot be combined with another repository source or checkout path"
                .into(),
        ));
    }
    if input
        .branch
        .as_deref()
        .is_some_and(|b| !git2::Reference::is_valid_name(&format!("refs/heads/{b}")))
    {
        return Err(Error::InvalidParams("invalid workspace branch".into()));
    }
    Ok(())
}

/// Capture synchronously on the real transport frame, before create's first await.
pub(crate) fn prepare(
    services: &Services,
    input: &WorkspaceCreate,
) -> BoxFuture<'static, Result<Option<Arc<Plan>>>> {
    let captured = input
        .repository_checkout
        .clone()
        .map(|selection| {
            let request = Request::current(services, &CheckoutFrame::Create(selection.clone()))?;
            shape(input)?;
            Ok::<_, Error>((request, selection))
        })
        .transpose();
    Box::pin(async move {
        let Some((request, selection)) = captured? else {
            return Ok(None);
        };
        let _legacy = tokio::time::timeout(CAPTURE_LIMIT, request.connection.caller.legacy_lease())
            .await
            .map_err(denied)?
            .map_err(denied)?;
        let (lease, provider) = request
            .bind(&selection.checkout_id, &selection.revision)
            .await?;
        let project = load_project(&lease, &provider, &selection.project_path)
            .await?
            .map_err(|_| unavailable())?;
        checked_selection(&lease, &selection)?;
        request.private_projects(vec![selection.project_path.clone()])?;
        Ok(Some(Arc::new(Plan {
            request,
            lease,
            provider,
            project,
            selection,
            destination: Mutex::default(),
        })))
    })
}

pub(super) fn checked_selection(
    lease: &Lease,
    selection: &CheckoutSelection,
) -> Result<NativeCheckoutSelection> {
    let actual = NativeCheckoutSelection::new(&selection.branch, &selection.commit_sha)?;
    let branches = lease.branches.lock().map_err(denied)?;
    if branches
        .get(&(selection.project_path.clone(), selection.branch.clone()))
        .is_none_or(|branch| branch.commit_sha != selection.commit_sha)
    {
        return Err(Error::InvalidParams(
            "checkout selection must match a branch observed on this connection".into(),
        ));
    }
    Ok(actual)
}

impl Plan {
    pub(crate) async fn current(&self) -> Result<()> {
        let _legacy =
            tokio::time::timeout(CAPTURE_LIMIT, self.request.connection.caller.legacy_lease())
                .await
                .map_err(denied)?
                .map_err(denied)?;
        self.current_under_credential().await
    }

    async fn current_under_credential(&self) -> Result<()> {
        self.request.validate(&self.lease).await?;
        self.provider
            .with_project_current(&self.selection.project_path, &mut || Ok(()))
    }

    pub(crate) fn idempotency_key(&self, key: Option<String>) -> Option<String> {
        key.map(|key| {
            let mut digest = Sha256::new();
            for part in [
                &self.lease.id,
                &self.lease.revision,
                &self.selection.project_path,
                &self.selection.branch,
                &self.selection.commit_sha,
                &key,
            ] {
                digest.update((part.len() as u64).to_be_bytes());
                digest.update(part.as_bytes());
            }
            digest.update([u8::from(self.selection.mode == CheckoutMode::Cached)]);
            format!(
                "native-checkout:{}",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
            )
        })
    }

    pub(crate) fn configure(
        &self,
        input: &mut WorkspaceCreate,
        root: &Path,
        id: &WorkspaceId,
    ) -> Result<()> {
        let path = root
            .join(id.as_str())
            .join(crate::worktree_folder_slug(&self.project.wire.name));
        if path.exists() {
            return Err(unavailable());
        }
        input.repository_path = Some(path.to_string_lossy().into_owned());
        input.repository_name = Some(self.project.wire.name.clone());
        input.base_ref = Some(self.selection.branch.clone());
        input.base_commit_sha = Some(self.selection.commit_sha.clone());
        Ok(())
    }

    pub(crate) async fn provision(
        self: &Arc<Self>,
        path: PathBuf,
        branch: String,
        root: &Path,
        progress: Option<Arc<crate::create_progress::CreateProgress>>,
    ) -> Result<()> {
        self.current().await?;
        let plan = self.clone();
        let cache_root = intent_git::repo_cache::cache_root_for(root);
        let caller = self.request.connection.caller.caller().clone();
        let wire = self.request.connection.caller.wire_credential().cloned();
        let worker = self
            .request
            .connection
            .services
            .store_tasks
            .spawn_draining(with_caller(
                caller,
                with_wire_credential(wire, async move {
                    let _legacy = tokio::time::timeout(
                        CAPTURE_LIMIT,
                        plan.request.connection.caller.legacy_lease(),
                    )
                    .await
                    .map_err(denied)?
                    .map_err(denied)?;
                    plan.current_under_credential().await?;
                    let selection = checked_selection(&plan.lease, &plan.selection)?;
                    let source = NativeCheckoutSource::https(&plan.project.wire.clone_url)?;
                    if path.exists() {
                        return Err(unavailable());
                    }
                    let parent = path.parent().ok_or_else(unavailable)?;
                    tokio::fs::create_dir_all(parent).await.map_err(denied)?;
                    if let Some(progress) = &progress {
                        let message = if plan.selection.mode == CheckoutMode::Cached {
                            "Preparing repository cache..."
                        } else {
                            "Cloning repository..."
                        };
                        progress.milestone("receiving", 5, message).await;
                    }
                    match plan.selection.mode {
                        CheckoutMode::Cached => {
                            let cache = plan.provider.cache(&cache_root, source.url())?;
                            let credential = plan.provider.native_credential(source.url()).await?;
                            cache
                                .ensure(selection.clone(), Box::new(credential), None)
                                .await?;
                            cache.checkout(path.clone(), selection.clone()).await?;
                        }
                        CheckoutMode::Direct => {
                            let mut credential =
                                plan.provider.native_credential(source.url()).await?;
                            let target = path.clone();
                            tokio::task::spawn_blocking(move || {
                                intent_git::native_checkout::clone_exact(
                                    &source,
                                    &target,
                                    &selection,
                                    &mut credential,
                                )
                            })
                            .await
                            .map_err(denied)??;
                        }
                    }
                    *plan.destination.lock().map_err(denied)? = Some(Destination {
                        path: path.clone(),
                        published: false,
                    });
                    plan.current_under_credential().await?;
                    if let Some(progress) = &progress {
                        progress
                            .milestone("checkout", 88, "Checking out selected branch...")
                            .await;
                    }
                    let expected = plan.selection.commit_sha.clone();
                    let original_source = plan.project.wire.clone_url.clone();
                    tokio::task::spawn_blocking(move || {
                        let repo = git2::Repository::open(&path).map_err(denied)?;
                        let oid = git2::Oid::from_str(&expected).map_err(denied)?;
                        if repo.head().map_err(denied)?.target() != Some(oid) {
                            return Err(unavailable());
                        }
                        let commit = repo.find_commit(oid).map_err(denied)?;
                        if let Ok(existing) = repo.find_branch(&branch, git2::BranchType::Local) {
                            if existing.get().target() != Some(oid) {
                                return Err(unavailable());
                            }
                        } else {
                            repo.branch(&branch, &commit, false).map_err(denied)?;
                        }
                        repo.set_head(&format!("refs/heads/{branch}"))
                            .map_err(denied)?;
                        // Restriction marker only, never authority: an instance
                        // change must not route this checkout to a user helper.
                        repo.config()
                            .map_err(denied)?
                            .set_str("intent.nativeCheckoutSource", &original_source)
                            .map_err(denied)?;
                        if repo.head().map_err(denied)?.target() != Some(oid) {
                            return Err(unavailable());
                        }
                        Ok(())
                    })
                    .await
                    .map_err(denied)??;
                    plan.current_under_credential().await
                }),
            ))
            .ok_or_else(unavailable)?;
        worker.await.map_err(denied)?
    }

    pub(crate) async fn insert(
        self: &Arc<Self>,
        workspace: Workspace,
        auto_commit: bool,
    ) -> Result<()> {
        self.current().await?;
        let plan = self.clone();
        let caller = self.request.connection.caller.caller().clone();
        let wire = self.request.connection.caller.wire_credential().cloned();
        let worker = self
            .request
            .connection
            .services
            .store_tasks
            .spawn_draining(with_caller(
                caller,
                with_wire_credential(wire, async move {
                    let _legacy = tokio::time::timeout(
                        CAPTURE_LIMIT,
                        plan.request.connection.caller.legacy_lease(),
                    )
                    .await
                    .map_err(denied)?
                    .map_err(denied)?;
                    let hash = match plan.request.connection.caller.wire_credential() {
                        Some(WireCredential::Principal { token_hash, .. }) => {
                            Some(token_hash.as_str())
                        }
                        _ => None,
                    };
                    plan.request
                        .connection
                        .services
                        .store
                        .insert_workspace_with_host_admission(
                            &workspace,
                            Some(auto_commit),
                            &plan.lease.authority,
                            hash,
                            || {
                                plan.provider
                                    .with_project_current(&plan.selection.project_path, &mut || {
                                        Ok(())
                                    })
                            },
                        )
                        .await?;
                    if let Some(destination) = plan.destination.lock().map_err(denied)?.as_mut() {
                        destination.published = true;
                    }
                    Ok(())
                }),
            ))
            .ok_or_else(unavailable)?;
        worker.await.map_err(denied)?
    }
}
