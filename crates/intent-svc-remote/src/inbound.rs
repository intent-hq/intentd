//! The head's domain as the node-connection service calls it: every call a
//! machine link makes back into [`Services`].

use std::sync::Arc;

use intent_core::node_link::{HomeEvents, HomeRef, LinkFrame};
use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_node_connections::{LinkView, NodeInbound};
use intent_services::feature_api::Services;
use intent_transport::node_link::IncomingRequest;
use tokio::time::Instant;

use crate::rpc::HeadCalls;

/// [`NodeInbound`] answered with the backend's [`Services`].
pub(crate) struct ServicesInbound {
    services: Services,
    calls: HeadCalls,
}

impl ServicesInbound {
    pub(crate) fn shared(services: Services) -> Arc<dyn NodeInbound> {
        Arc::new(Self {
            calls: HeadCalls::new(services.clone()),
            services,
        })
    }

    pub(crate) fn services(&self) -> &Services {
        &self.services
    }
}

impl NodeInbound for ServicesInbound {
    fn link_request(&self, link: Arc<dyn LinkView>, request: IncomingRequest) {
        self.calls.answer(link, request);
    }

    fn home_events<'a>(&'a self, machine_id: &'a str, events: HomeEvents) -> BoxFuture<'a, ()> {
        Box::pin(self.services.publish_home_events(machine_id, events))
    }

    fn machine_script_frame<'a>(
        &'a self,
        machine_id: &'a str,
        frame: LinkFrame,
    ) -> BoxFuture<'a, ()> {
        let svc = &self.services;
        svc.feature_runtime()
            .machine_script_frame(svc, machine_id, frame)
    }

    fn machine_runs_live(&self, machine_id: &str) -> bool {
        let svc = &self.services;
        svc.feature_runtime().machine_runs_live(svc, machine_id)
    }

    fn end_agents_machine_runs<'a>(
        &'a self,
        agents: &'a [AgentId],
        deadline: Instant,
        reason: &'a str,
    ) -> BoxFuture<'a, Vec<AgentId>> {
        let svc = &self.services;
        svc.feature_runtime()
            .end_agents_machine_runs(svc, agents, deadline, reason)
    }

    fn machine_runs_lost<'a>(&'a self, machine_id: &'a str, reason: &'a str) -> BoxFuture<'a, ()> {
        let svc = &self.services;
        svc.feature_runtime()
            .machine_link_lost(svc, machine_id, reason)
    }

    fn final_stop_begun(&self, agent_id: &AgentId) {
        let svc = &self.services;
        svc.feature_runtime()
            .machine_runs_final_stop_begun(svc, agent_id);
    }

    fn final_stop_settled<'a>(
        &'a self,
        agent_id: &'a AgentId,
        reason: &'a str,
    ) -> BoxFuture<'a, ()> {
        let svc = &self.services;
        svc.feature_runtime()
            .machine_runs_final_stop_acked(svc, agent_id, reason)
    }

    fn homes_link_lost<'a>(
        &'a self,
        link: &'a dyn LinkView,
        lost_at: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(crate::home_state::detach_homes(
            &self.services,
            link,
            lost_at,
        ))
    }

    fn machine_changed<'a>(&'a self, link: &'a dyn LinkView) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            match self
                .services
                .store()
                .get_exposed_machine(&link.machine_id())
                .await
            {
                Ok(Some(row)) => {
                    crate::methods::publish_machine(&self.services, &row, Some(link)).await;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, "remote machine: could not read it to announce a change");
                }
            }
        })
    }

    fn home_setup_output<'a>(
        &'a self,
        home: &'a HomeRef,
        seq: u64,
        data: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let services = &self.services;
            match services.home_setups().output(
                &home.workspace_id,
                &home.home_id,
                seq,
                data,
                std::time::Instant::now(),
            ) {
                intent_services::home_setup::OutputAnnounce::Now => {
                    services
                        .publish_workspace_home_changed(&home.workspace_id)
                        .await;
                }
                intent_services::home_setup::OutputAnnounce::After(delay) => {
                    // One trailing announcement, so the last output is not
                    // left unannounced until the next state change.
                    let services = services.clone();
                    let home = home.clone();
                    intent_core::spawn_daemon(async move {
                        tokio::time::sleep(delay).await;
                        if services.home_setups().flush_output(
                            &home.workspace_id,
                            &home.home_id,
                            std::time::Instant::now(),
                        ) {
                            services
                                .publish_workspace_home_changed(&home.workspace_id)
                                .await;
                        }
                    });
                }
                intent_services::home_setup::OutputAnnounce::No => {}
            }
        })
    }

    fn agent_placement_changed<'a>(
        &'a self,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(
            self.services
                .publish_agent_placement_changed(workspace_id, agent_id),
        )
    }

    fn agent_skills_changed<'a>(
        &'a self,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(
            self.services
                .publish_agent_skills_changed(workspace_id, agent_id),
        )
    }

    fn machine_became_orchestrator(&self, machine_id: String, refused_token: String) {
        crate::methods::forget_orchestrator(self.services.clone(), machine_id, refused_token);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
