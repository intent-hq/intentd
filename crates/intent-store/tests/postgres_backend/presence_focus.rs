//! Presence focus resolution on both backends: source and candidate access
//! for the viewer and the person (primary, workspace member, host member),
//! deleted and chief workspaces, agent and note existence, and the target
//! order.
//!
//! Decoy rule: the decoy owns every row the subject owns, plus the ids the
//! subject probes as absent: workspace `ws-x`, principal `ghost` (a member of
//! `ws-a`), agent `ag-decoy` and note `n-decoy` in `ws-a`. A statement
//! without its tenant filter reaches them and trips the leak trap.
//! Workspaces `ws-C`/`ws-b` and notes `nB`/`na` sort differently bytewise
//! and under a locale, so `COLLATE "C"` decides the target order.

use intent_core::{
    AgentId, AgentSession, AgentStatus, ContentType, Error, HostInvite, Note, NoteId, NoteMetadata,
    NoteVisibility, Principal, PrincipalId, PrincipalIdentity, Workspace, WorkspaceId,
    WorkspaceRole, WorkspaceStatus, CHIEF_WORKSPACE_ID,
};
use intent_store::{HostInviteJoinOutcome, HostJoinCredential, Store};
use serde_json::{json, Value};

use crate::harness::parity_test;

const T1: &str = "2026-02-01T00:00:01.000Z";

fn pid(id: &str) -> PrincipalId {
    PrincipalId(id.into())
}

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn person(id: &str, github: i64, tenant: &str) -> Principal {
    let identity = PrincipalIdentity::github(github);
    Principal {
        id: pid(id),
        github_user_id: identity.github_user_id(),
        identity: Some(identity),
        login: Some(format!("{id}-{tenant}")),
        display_name: Some(format!("{id} of {tenant}")),
        avatar_url: None,
        is_primary: false,
        created_at: T1.into(),
        updated_at: T1.into(),
    }
}

async fn workspace(store: &Store, id: &str, status: WorkspaceStatus, tenant: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("{id} of {tenant}"),
            branch: "main".into(),
            status,
            created_at: T1.into(),
            updated_at: T1.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn agent(store: &Store, id: &str, ws: &str, tenant: &str) {
    store
        .insert_agent_session(&AgentSession {
            id: AgentId(id.into()),
            workspace_id: wid(ws),
            name: format!("{id} of {tenant}"),
            status: AgentStatus::Idle,
            created_at: T1.into(),
            updated_at: T1.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
}

async fn note(store: &Store, id: &str, ws: &str, tenant: &str) {
    store
        .insert_note(&Note {
            id: NoteId(id.into()),
            workspace_id: wid(ws),
            title: format!("{id} of {tenant}"),
            content: String::new(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: T1.into(),
            rev: 0,
            updated_at: T1.into(),
        })
        .await
        .unwrap();
}

async fn host_member(store: &Store, owner: &PrincipalId, p: &Principal) {
    let invite = HostInvite::new(
        format!("hi-{}", p.id),
        owner.clone(),
        p.identity.clone().unwrap(),
        p.login.clone().unwrap(),
        format!("secret-{}", p.id),
        None,
    )
    .unwrap();
    store.insert_host_invite(&invite).await.unwrap();
    let generation = store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    let joined = store
        .join_host_by_invite(
            &invite.id,
            p,
            HostJoinCredential::Proof {
                token_hash: &format!("h-{}", p.id),
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        joined,
        HostInviteJoinOutcome::Joined {
            membership_added: true,
            ..
        }
    ));
}

fn at(ws: &str) -> Value {
    json!({"workspaceId": ws})
}

fn agent_at(ws: &str, agent: &str) -> Value {
    json!({"workspaceId": ws, "agentId": agent})
}

fn note_at(ws: &str, note: &str) -> Value {
    json!({"workspaceId": ws, "noteId": note})
}

parity_test!(
    presence_focus_resolves_authorized_targets,
    |store, tenant| {
        let me = store.get_primary_principal().await.unwrap().id;
        for id in ["ws-a", "ws-b", "ws-C"] {
            workspace(&store, id, WorkspaceStatus::Active, tenant).await;
        }
        workspace(&store, "ws-del", WorkspaceStatus::Deleted, tenant).await;
        let alice = person("alice", 101, tenant);
        store.upsert_principal(&alice).await.unwrap();
        assert!(store
            .add_workspace_member(&wid("ws-a"), &alice.id, WorkspaceRole::Collaborator)
            .await
            .unwrap());
        let carol = person("carol", 102, tenant);
        host_member(&store, &me, &carol).await;
        agent(&store, "ag1", "ws-a", tenant).await;
        agent(&store, "ag2", "ws-b", tenant).await;
        for id in ["n1", "nB", "na"] {
            note(&store, id, "ws-a", tenant).await;
        }
        let focus = |source: &'static str, viewer: PrincipalId, who: PrincipalId, c: Vec<Value>| {
            let store = &store;
            async move {
                store
                    .authorized_presence_focus(&wid(source), &viewer, &who, &c)
                    .await
            }
        };
        let not_found = |r: intent_core::Result<Value>| matches!(r, Err(Error::NotFound(m)) if m == "presence focus");

        // Source in the person's scope; agents before notes before bare
        // workspaces; candidates outside the person's scope are dropped.
        let all = vec![
            agent_at("ws-b", "ag2"),
            at("ws-a"),
            note_at("ws-a", "n1"),
            agent_at("ws-a", "ag1"),
        ];
        let winner = focus("ws-a", me.clone(), alice.id.clone(), all.clone())
            .await
            .unwrap();
        assert_eq!(winner, agent_at("ws-a", "ag1"));
        // The caller's object comes back as given, keys in its own order:
        // the wire re-emits it, so a reordered copy would change the bytes.
        assert_eq!(
            serde_json::to_string(&winner).unwrap(),
            serde_json::to_string(&all[3]).unwrap()
        );
        assert_eq!(
            focus("ws-a", me.clone(), alice.id.clone(), all[..3].to_vec())
                .await
                .unwrap(),
            note_at("ws-a", "n1")
        );
        assert_eq!(
            focus("ws-a", me.clone(), alice.id.clone(), all[..2].to_vec())
                .await
                .unwrap(),
            at("ws-a")
        );
        // The source wins over other workspaces, then workspace and id order.
        assert_eq!(
            focus("ws-b", me.clone(), me.clone(), all.clone())
                .await
                .unwrap(),
            agent_at("ws-b", "ag2")
        );
        assert_eq!(
            focus("ws-a", me.clone(), me.clone(), vec![at("ws-b"), at("ws-C")])
                .await
                .unwrap(),
            at("ws-C")
        );
        assert_eq!(
            focus(
                "ws-b",
                me.clone(),
                me.clone(),
                vec![note_at("ws-a", "na"), note_at("ws-a", "nB")]
            )
            .await
            .unwrap(),
            note_at("ws-a", "nB")
        );

        // Dropped: both agent and note, a resource in another workspace, unknown
        // resources and workspaces, deleted workspaces, malformed entries.
        let rejected = vec![
            json!({"workspaceId": "ws-a", "agentId": "ag1", "noteId": "n1"}),
            agent_at("ws-a", "ag2"),
            note_at("ws-b", "n1"),
            agent_at("ws-a", "ag-decoy"),
            note_at("ws-a", "n-decoy"),
            at("ws-x"),
            at("ws-del"),
            json!({"agentId": "ag1"}),
            json!(7),
            Value::Null,
        ];
        assert_eq!(
            focus("ws-a", me.clone(), me.clone(), rejected.clone())
                .await
                .unwrap(),
            Value::Null
        );
        assert_eq!(
            focus("ws-a", me.clone(), me.clone(), vec![]).await.unwrap(),
            Value::Null
        );

        // Host members see every live workspace except chief.
        assert_eq!(
            focus(
                "ws-b",
                me.clone(),
                carol.id.clone(),
                vec![at(CHIEF_WORKSPACE_ID), note_at("ws-a", "na")]
            )
            .await
            .unwrap(),
            note_at("ws-a", "na")
        );
        assert!(not_found(
            focus("__chief__", me.clone(), carol.id.clone(), vec![]).await
        ));
        assert_eq!(
            focus(
                "__chief__",
                me.clone(),
                me.clone(),
                vec![at(CHIEF_WORKSPACE_ID)]
            )
            .await
            .unwrap(),
            at(CHIEF_WORKSPACE_ID)
        );

        // The source must be live and in both principals' scope.
        assert!(not_found(
            focus("ws-b", me.clone(), alice.id.clone(), all.clone()).await
        ));
        assert!(not_found(
            focus("ws-b", alice.id.clone(), me.clone(), all.clone()).await
        ));
        assert!(not_found(
            focus("ws-del", me.clone(), me.clone(), all.clone()).await
        ));
        assert!(not_found(
            focus("ws-x", me.clone(), me.clone(), all.clone()).await
        ));
        assert!(not_found(
            focus("ws-a", me.clone(), pid("ghost"), all.clone()).await
        ));
        assert!(not_found(
            focus("ws-a", pid("ghost"), me.clone(), all.clone()).await
        ));
        // Unknown principals in no tenant at all.
        assert!(not_found(
            focus("ws-a", me.clone(), pid("nobody"), all.clone()).await
        ));
        // Decoy-only rows, made after the decoy's own probes.
        if tenant == "decoy" {
            workspace(&store, "ws-x", WorkspaceStatus::Active, tenant).await;
            let ghost = person("ghost", 103, tenant);
            store.upsert_principal(&ghost).await.unwrap();
            store
                .add_workspace_member(&wid("ws-a"), &ghost.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
            agent(&store, "ag-decoy", "ws-a", tenant).await;
            note(&store, "n-decoy", "ws-a", tenant).await;
        }
    }
);
