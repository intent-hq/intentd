//! The original qualified startup producer outlives a cancelled response waiter.
use super::{
    AcpSessionOpened, AgentId, CallbackClient, Error, InitializeResponse, McpServer, PathBuf,
    RepositoryCreationOwner, RepositorySessionOutcome, Result, Services,
};
use crate::repository_admission::lifecycle::physical_owner::{
    RepositoryCreationRetirement, RepositoryReviewCreationOwner,
};

use crate::repository_admission::lifecycle::native_allocation::{NativeAllocation, NativeProducer};

struct StartupWaiter(Option<RepositoryCreationRetirement>);
impl Drop for StartupWaiter {
    fn drop(&mut self) {
        if let Some(retirement) = self.0.take() {
            retirement.retire();
        }
    }
}

impl Services {
    pub(super) async fn create_owned_review_session(
        &self,
        callbacks: CallbackClient,
        agent: AgentId,
        cwd: PathBuf,
        mcp_servers: Vec<McpServer>,
        creation: RepositoryCreationOwner,
        replacing: bool,
    ) -> Result<RepositorySessionOutcome<AcpSessionOpened>> {
        let mut waiter = StartupWaiter(Some(creation.retirement()));
        let creation = creation
            .prepare_review()
            .await
            .map_err(|_| Error::Forbidden("Private native startup unavailable".into()))?;
        let context = creation.context().clone();
        let native = NativeAllocation::new(callbacks.clone(), context.clone());
        creation
            .attach_native(native.clone())
            .map_err(|_| Error::Forbidden("Private native startup unavailable".into()))?;
        self.store_tasks
            .spawn_draining_with_current_caller(native.clone().drive())
            .ok_or_else(|| Error::Internal("Private native startup unavailable".into()))?;
        let producer = NativeProducer(native.clone());
        let services = self.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let (accept, accepted) = tokio::sync::oneshot::channel();
        // Registration precedes dispatch. Dropping the response future only
        // closes its original eligibility; it cannot cancel the producer.
        self.store_tasks
            .spawn_draining_with_current_caller(async move {
                let _producer = producer;
                let outcome = services
                    .finish_owned_review_startup(
                        &callbacks,
                        &agent,
                        cwd,
                        mcp_servers,
                        creation,
                        replacing,
                        &native,
                    )
                    .await;
                let session = outcome
                    .as_ref()
                    .ok()
                    .map(|outcome| outcome.response.session_id.clone());
                let delivered = send.send(outcome).is_ok();
                // Sending is not acceptance: the receiver can disappear after the
                // send but before polling it. Keep cleanup until its synchronous ACK.
                if !delivered || accepted.await.is_err() {
                    if let Some(session) = session {
                        // The lost result drops its physical owner. Retire the same
                        // remote allocation; a committed Store write stays committed.
                        let _ = callbacks.retire_review_context(&session, &context).await;
                    }
                }
            })
            .ok_or_else(|| Error::Internal("Private native startup unavailable".into()))?;
        let result = receive
            .await
            .map_err(|_| Error::Internal("Private native startup unavailable".into()))?;
        let _ = accept.send(());
        waiter.0 = None;
        result
    }

    pub(super) async fn load_owned_review_session(
        &self,
        callbacks: CallbackClient,
        init: InitializeResponse,
        agent: AgentId,
        cwd: PathBuf,
        mcp_servers: Vec<McpServer>,
        creation: RepositoryCreationOwner,
    ) -> Result<RepositorySessionOutcome<AcpSessionOpened>> {
        let mut waiter = StartupWaiter(Some(creation.retirement()));
        let creation = creation.prepare_review().await.map_err(|_| {
            Error::Forbidden("Private native history requires original admission".into())
        })?;
        let bootstrap = creation.bootstrap().map_err(|_| {
            Error::Forbidden("Private native history requires original admission".into())
        })?;
        let native = NativeAllocation::new_loaded(
            callbacks.clone(),
            creation.context().clone(),
            bootstrap
                .loaded_session()
                .map_err(|_| Error::Forbidden("Private native history unavailable".into()))?,
        );
        creation
            .attach_native(native.clone())
            .map_err(|_| Error::Forbidden("Private native history unavailable".into()))?;
        self.store_tasks
            .spawn_draining_with_current_caller(native.clone().drive())
            .ok_or_else(|| Error::Internal("Private native startup unavailable".into()))?;
        let producer = NativeProducer(native.clone());
        let services = self.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        let (accept, accepted) = tokio::sync::oneshot::channel();
        self.store_tasks
            .spawn_draining_with_current_caller(async move {
                let _producer = producer;
                let result: Result<RepositorySessionOutcome<AcpSessionOpened>> = async {
                    let outcome = creation
                        .initialize_with_outcome(&services.store, || async {
                            let prepared = services
                                .prepare_acp_load_with_review(
                                    callbacks.original_connection(),
                                    Some(&callbacks),
                                    &init,
                                    &agent,
                                    cwd,
                                    mcp_servers,
                                    Some((&bootstrap, &native)),
                                )
                                .await?
                                .ok_or_else(|| {
                                    Error::Unsupported("Private native history unavailable".into())
                                })?;
                            Ok::<_, Error>((prepared.session_id.clone(), prepared))
                        })
                        .await
                        .map_err(|_| {
                            Error::Forbidden("Private native history unavailable".into())
                        })?;
                    let mut prepared = outcome.producer?;
                    let owner = outcome.owner.map_err(|_| {
                        Error::Forbidden("Private native history unavailable".into())
                    })?;
                    let query = prepared.query.take();
                    let response = services.finish_acp_load(prepared).await;
                    Ok(RepositorySessionOutcome {
                        response,
                        owner: Ok(owner),
                        query,
                    })
                }
                .await;
                if result.is_err() {
                    // Preserve the original failure, but keep driving exact cleanup.
                    let _ = native.join().await;
                }
                let delivered = send.send(result).is_ok();
                if !delivered || accepted.await.is_err() {
                    let _ = native.join().await;
                }
            })
            .ok_or_else(|| Error::Internal("Private native startup unavailable".into()))?;
        let result = receive
            .await
            .map_err(|_| Error::Internal("Private native history unavailable".into()))?;
        let _ = accept.send(());
        waiter.0 = None;
        result
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "original startup owners and admission inputs remain explicit"
    )]
    async fn finish_owned_review_startup(
        &self,
        callbacks: &CallbackClient,
        agent: &AgentId,
        cwd: PathBuf,
        mcp_servers: Vec<McpServer>,
        creation: RepositoryReviewCreationOwner,
        replacing: bool,
        native: &NativeAllocation,
    ) -> Result<RepositorySessionOutcome<AcpSessionOpened>> {
        let context = creation.context().clone();
        let outcome = creation
            .initialize_with_outcome(&self.store, || async {
                let prepared = self
                    .prepare_acp_session_with_review_context(
                        callbacks.original_connection(),
                        Some(callbacks),
                        agent,
                        cwd,
                        mcp_servers,
                        Some(native),
                    )
                    .await?;
                Ok::<_, Error>((prepared.response.session_id.0.to_string(), prepared))
            })
            .await
            .map_err(|_| Error::Forbidden("Private native startup unavailable".into()))?;
        let mut prepared = outcome.producer?;
        let Ok(owner) = outcome.owner else {
            callbacks
                .retire_review_context(&prepared.response.session_id.0, &context)
                .await
                .map_err(|_| Error::Internal("Private native retirement unconfirmed".into()))?;
            return Err(Error::Forbidden(
                "Private native startup unavailable".into(),
            ));
        };
        let canonical = prepared.response.session_id.0.to_string();
        let query = prepared.query.take();
        let response = self
            .finish_created_acp_session(prepared, canonical, replacing)
            .await;
        Ok(RepositorySessionOutcome {
            response,
            owner: Ok(owner),
            query,
        })
    }
}
