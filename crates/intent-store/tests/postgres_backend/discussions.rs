//! Labs discussions on both backends: history, conversations, private
//! participants, management, agent deliveries and activity, and reactions.
//!
//! Principals, members, host members, agents and interrupted-agent rows are
//! seeded with SQL: their `Store` methods are not ported to Postgres yet.
//! Bodies use fixed ids, leave rows behind and assert exact values from an
//! empty start, so a statement without its tenant filter fails the subject
//! run; the decoy snapshot check catches writes that reach another tenant.

use intent_core::discussion::{
    DiscussionActor, DiscussionAuthor, DiscussionTarget, DEFAULT_CONVERSATION,
};
use intent_core::{AgentId, Error, PrincipalId, Workspace, WorkspaceId};
use intent_store::{AgentQueueRow, Store};

use crate::harness::{open, sqlite_store, tenant_rows, test_db};

const WS: &str = "ws-d";
const T0: &str = "2026-01-01T00:00:00.000Z";

fn ws() -> WorkspaceId {
    WorkspaceId(WS.into())
}

fn pid(id: &str) -> PrincipalId {
    PrincipalId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn human(id: &PrincipalId) -> DiscussionAuthor {
    DiscussionAuthor {
        principal_id: Some(id.clone()),
        agent_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
    }
}

fn agent(id: &str) -> DiscussionAuthor {
    DiscussionAuthor {
        principal_id: None,
        agent_id: Some(aid(id)),
        login: None,
        display_name: Some(id.into()),
        avatar_url: None,
    }
}

/// Raw SQL access for seeding and checks: `{c}` expands to the tenant
/// column (`tenant_id, ` on Postgres), `{v}` to its value and `{w}` to a
/// tenant filter (`tenant_id = '…' AND `); all empty on `SQLite`.
enum Seed {
    Sqlite(intent_store::StorePool),
    Pg(sqlx::PgPool, String),
}

impl Seed {
    fn sql(&self, template: &str) -> String {
        let (c, v, w) = match self {
            Self::Sqlite(_) => (String::new(), String::new(), String::new()),
            Self::Pg(_, t) => (
                "tenant_id, ".to_owned(),
                format!("'{t}', "),
                format!("tenant_id = '{t}' AND "),
            ),
        };
        template
            .replace("{c}", &c)
            .replace("{v}", &v)
            .replace("{w}", &w)
    }

    async fn exec(&self, template: &str) {
        let sql = self.sql(template);
        match self {
            Self::Sqlite(pool) => sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(pool)
                .await
                .map(|_| ()),
            Self::Pg(pool, _) => sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                .execute(pool)
                .await
                .map(|_| ()),
        }
        .expect("seed statement");
    }

    async fn texts(&self, template: &str) -> Vec<String> {
        let sql = self.sql(template);
        match self {
            Self::Sqlite(pool) => {
                sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                    .fetch_all(pool)
                    .await
            }
            Self::Pg(pool, _) => {
                sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                    .fetch_all(pool)
                    .await
            }
        }
        .expect("check query")
    }

    async fn primary(&self) -> PrincipalId {
        let ids = self
            .texts("SELECT id FROM principal WHERE {w}is_primary = 1")
            .await;
        assert_eq!(ids.len(), 1);
        pid(&ids[0])
    }

    /// Workspace `ws-d` (owned by the primary principal), members alice
    /// (collaborator) and bob (manager), host member carol, outsider dave,
    /// and agents agent-a and agent-b.
    async fn workspace(&self, store: &Store) {
        store
            .insert_workspace(&Workspace {
                id: ws(),
                title: "Discussions".into(),
                branch: "main".into(),
                created_at: T0.into(),
                updated_at: T0.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        for (id, login) in [
            ("alice", "alice-l"),
            ("bob", "bob-l"),
            ("carol", "carol-l"),
            ("dave", "dave-l"),
        ] {
            self.exec(&format!(
                "INSERT INTO principal ({{c}}id, login, display_name, created_at, updated_at) \
                 VALUES ({{v}}'{id}', '{login}', 'Name {id}', '{T0}', '{T0}')"
            ))
            .await;
        }
        for (id, role) in [("alice", "collaborator"), ("bob", "manager")] {
            self.exec(&format!(
                "INSERT INTO workspace_member ({{c}}workspace_id, principal_id, role, added_at) \
                 VALUES ({{v}}'{WS}', '{id}', '{role}', '{T0}')"
            ))
            .await;
        }
        self.exec(&format!(
            "INSERT INTO host_member ({{c}}principal_id, added_at) VALUES ({{v}}'carol', '{T0}')"
        ))
        .await;
        for id in ["agent-a", "agent-b"] {
            self.exec(&format!(
                "INSERT INTO agent_session ({{c}}id, workspace_id, name, status, created_at, updated_at) \
                 VALUES ({{v}}'{id}', '{WS}', '{id}', 'idle', '{T0}', '{T0}')"
            ))
            .await;
        }
    }
}

/// Like `parity_test!`, with a [`Seed`] for the backend under test.
macro_rules! seeded_test {
    ($name:ident, |$store:ident, $seed:ident, $tenant:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            {
                eprintln!("[{}] backend: sqlite", stringify!($name));
                let (store, _dir) = sqlite_store().await;
                let $seed = Seed::Sqlite(store.write_pool().clone());
                let $store = store;
                let $tenant: &str = "sqlite";
                $body
            }
            if let Some(db) = test_db(false).await {
                let mut before = Vec::new();
                for t in ["decoy", "subject"] {
                    eprintln!("[{}] backend: postgres, tenant {t}", stringify!($name));
                    if t == "subject" {
                        before = tenant_rows(&db.admin, "decoy").await;
                    }
                    let $store = open(&db, t).await;
                    let $seed = Seed::Pg(db.admin.clone(), t.to_owned());
                    let $tenant: &str = t;
                    $body
                }
                let after = tenant_rows(&db.admin, "decoy").await;
                assert!(
                    before == after,
                    "the subject tenant's run changed the decoy tenant's rows"
                );
            }
        }
    };
}

seeded_test!(messages_page_retry_and_reply, |store, seed, tenant| {
    seed.workspace(&store).await;
    let primary = seed.primary().await;
    let me = human(&primary);
    assert!(store
        .list_discussion_messages(&ws(), None, None, 10)
        .await
        .unwrap()
        .messages
        .is_empty());
    let mut sent = Vec::new();
    for n in 1..=3 {
        let (m, inserted) = store
            .insert_discussion_message(&ws(), &format!("k{n}"), &format!("{tenant} {n}"), &me)
            .await
            .unwrap();
        assert!(inserted);
        assert_eq!(m.sequence, n);
        sent.push(m);
    }
    let (retry, inserted) = store
        .insert_discussion_message(&ws(), "k1", &format!("{tenant} 1"), &me)
        .await
        .unwrap();
    assert!(!inserted);
    assert_eq!(retry, sent[0]);
    assert!(matches!(
        store
            .insert_discussion_message(&ws(), "k1", "changed", &me)
            .await,
        Err(Error::InvalidParams(_))
    ));

    let page = store
        .list_discussion_messages(&ws(), None, None, 2)
        .await
        .unwrap();
    assert!(page.has_more);
    assert_eq!(page.messages, sent[1..].to_vec());
    let page = store
        .list_discussion_messages_in(&ws(), DEFAULT_CONVERSATION, Some(2), None, 10)
        .await
        .unwrap();
    assert!(!page.has_more);
    assert_eq!(page.messages, sent[..1].to_vec());
    let page = store
        .list_discussion_messages_for(&ws(), DEFAULT_CONVERSATION, None, Some(1), 1, None)
        .await
        .unwrap();
    assert!(page.has_more);
    assert_eq!(page.messages, sent[1..2].to_vec());
    for (before, after, limit) in [(Some(0), None, 1), (Some(2), Some(1), 1), (None, None, 0)] {
        assert!(matches!(
            store
                .list_discussion_messages(&ws(), before, after, limit)
                .await,
            Err(Error::InvalidParams(_))
        ));
    }

    let reply_target = DiscussionTarget {
        reply_to_message_id: Some(sent[0].id.clone()),
        ..DiscussionTarget::default()
    };
    let (reply, inserted) = store
        .send_discussion_message(&ws(), "k4", "re", &me, &reply_target)
        .await
        .unwrap();
    assert!(inserted);
    assert_eq!(reply.message.sequence, 4);
    let quoted = reply.message.reply_to.expect("reply snapshot");
    assert_eq!(
        (quoted.id.as_str(), quoted.body.as_str()),
        (sent[0].id.as_str(), format!("{tenant} 1").as_str())
    );
    // An outsider cannot post; another workspace's history stays empty.
    assert!(matches!(
        store
            .insert_discussion_message(&ws(), "k5", "x", &human(&pid("dave")))
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(store
        .list_discussion_messages(&WorkspaceId("__chief__".into()), None, None, 10)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert!(!store.has_private_discussions(&ws()).await.unwrap());
});

seeded_test!(
    conversations_participants_and_management,
    |store, seed, _tenant| {
        seed.workspace(&store).await;
        let primary = seed.primary().await;
        let (alice, bob, dave) = (pid("alice"), pid("bob"), pid("dave"));
        let (created, inserted) = store
            .send_discussion_message(
                &ws(),
                "c1",
                "topic",
                &human(&alice),
                &DiscussionTarget {
                    create_conversation: true,
                    ..DiscussionTarget::default()
                },
            )
            .await
            .unwrap();
        assert!(inserted);
        let topic = created.conversation.id.clone();
        assert_eq!(created.conversation.kind, "workspace");
        let (retry, inserted) = store
            .send_discussion_message(
                &ws(),
                "c1",
                "topic",
                &human(&alice),
                &DiscussionTarget {
                    create_conversation: true,
                    ..DiscussionTarget::default()
                },
            )
            .await
            .unwrap();
        assert!(!inserted);
        assert_eq!(retry.conversation.id, topic);

        let (direct, _) = store
            .send_discussion_message(
                &ws(),
                "d1",
                "private",
                &human(&alice),
                &DiscussionTarget {
                    recipient_principal_ids: Some(vec![bob.clone()]),
                    ..DiscussionTarget::default()
                },
            )
            .await
            .unwrap();
        let private = direct.conversation.id.clone();
        assert_eq!(direct.conversation.kind, "direct");
        let logins: Vec<_> = direct
            .conversation
            .participants
            .iter()
            .map(|p| p.login.clone().unwrap())
            .collect();
        assert_eq!(logins, ["alice-l", "bob-l"]);

        assert!(store.has_private_discussions(&ws()).await.unwrap());
        assert!(store
            .discussion_is_participant(&ws(), &private, &bob)
            .await
            .unwrap());
        assert!(!store
            .discussion_is_participant(&ws(), &private, &dave)
            .await
            .unwrap());
        let mut visible = store
            .discussion_visible_ids(
                &ws(),
                &alice,
                &[private.clone(), topic.clone(), "nope".into()],
            )
            .await
            .unwrap();
        visible.sort();
        let mut expected = vec![private.clone(), topic.clone()];
        expected.sort();
        assert_eq!(visible, expected);
        assert_eq!(
            store
                .discussion_visible_ids(&ws(), &dave, std::slice::from_ref(&private))
                .await
                .unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            store
                .discussion_conversation(&ws(), &private)
                .await
                .unwrap(),
            direct.conversation
        );

        let listed = store
            .discussion_conversations(&ws(), &alice, None, 10)
            .await
            .unwrap();
        let ids: Vec<_> = listed["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["id"].as_str().unwrap().to_owned())
            .collect();
        let mut newest_first = vec![private.clone(), topic.clone()];
        newest_first.sort_by(|x, y| y.cmp(x));
        assert_eq!(ids[0], DEFAULT_CONVERSATION);
        assert_eq!(ids[1..], newest_first);
        assert_eq!(listed["hasMore"], false);
        let page = store
            .discussion_conversations(&ws(), &dave, Some(DEFAULT_CONVERSATION), 1)
            .await
            .unwrap();
        assert_eq!(page["conversations"][0]["id"], topic.as_str());
        assert_eq!(page["hasMore"], false);

        let members = store
            .discussion_recipients(&ws(), &alice, "-L", None, 1)
            .await
            .unwrap();
        assert_eq!(members["members"][0]["login"], "bob-l");
        assert_eq!(members["hasMore"], true);
        let members = store
            .discussion_recipients(&ws(), &alice, "-l", Some("bob"), 10)
            .await
            .unwrap();
        assert_eq!(members["members"].as_array().unwrap().len(), 1);
        assert_eq!(members["members"][0]["login"], "carol-l");
        assert_eq!(members["hasMore"], false);

        assert!(matches!(
            store
                .manage_discussion(&ws(), &alice, &topic, Some("x"))
                .await,
            Err(Error::Forbidden(_))
        ));
        let renamed = store
            .manage_discussion(&ws(), &bob, &topic, Some("  Renamed "))
            .await
            .unwrap();
        assert_eq!(renamed.name.as_deref(), Some("Renamed"));
        assert_eq!(
            store
                .discussion_conversation(&ws(), &topic)
                .await
                .unwrap()
                .name
                .as_deref(),
            Some("Renamed")
        );
        assert!(matches!(
            store.manage_discussion(&ws(), &bob, &topic, None).await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            store
                .manage_discussion(&ws(), &primary, &private, Some("p"))
                .await,
            Err(Error::Forbidden(_))
        ));
        store
            .manage_discussion(&ws(), &primary, &topic, None)
            .await
            .unwrap();
        assert!(matches!(
            store.discussion_conversation(&ws(), &topic).await,
            Err(Error::NotFound(_))
        ));
        assert!(store
            .list_discussion_messages_in(&ws(), &topic, None, None, 10)
            .await
            .unwrap()
            .messages
            .is_empty());
    }
);

seeded_test!(agent_deliveries_and_activity, |store, seed, tenant| {
    seed.workspace(&store).await;
    let primary = seed.primary().await;
    let (a, b) = (aid("agent-a"), aid("agent-b"));
    assert!(!store
        .discussion_agent_access(&ws(), DEFAULT_CONVERSATION, &a)
        .await
        .unwrap());
    let (sent, _) = store
        .send_discussion_message(
            &ws(),
            "m1",
            "@agent-a",
            &human(&primary),
            &DiscussionTarget {
                recipient_agent_ids: vec![a.clone()],
                ..DiscussionTarget::default()
            },
        )
        .await
        .unwrap();
    let message = sent.message.id.clone();
    assert_eq!(sent.message.recipient_agent_ids, std::slice::from_ref(&a));
    assert_eq!(sent.message.agent_activity.len(), 1);
    assert_eq!(sent.message.agent_activity[0].state, "queued");
    assert!(store
        .discussion_agent_access(&ws(), DEFAULT_CONVERSATION, &a)
        .await
        .unwrap());
    assert!(!store
        .discussion_agent_access(&ws(), DEFAULT_CONVERSATION, &b)
        .await
        .unwrap());
    let granted = store
        .discussion_agent_conversations(&ws(), &a, None, 10)
        .await
        .unwrap();
    assert_eq!(granted["conversations"][0]["id"], DEFAULT_CONVERSATION);
    assert_eq!(granted["conversations"].as_array().unwrap().len(), 1);
    assert_eq!(granted["hasMore"], false);

    // The granted agent can answer in the workspace discussion.
    let (answer, _) = store
        .send_discussion_message(
            &ws(),
            "a1",
            "done",
            &agent("agent-a"),
            &DiscussionTarget::default(),
        )
        .await
        .unwrap();
    assert_eq!(answer.message.sequence, 2);
    assert!(matches!(
        store
            .send_discussion_message(
                &ws(),
                "b1",
                "x",
                &agent("agent-b"),
                &DiscussionTarget::default()
            )
            .await,
        Err(Error::NotFound(_))
    ));

    let pending = store.discussion_pending_deliveries(None).await.unwrap();
    assert_eq!(pending.len(), 1);
    let turn = pending[0].turn_id.clone();
    assert_eq!(turn, format!("discussion:{message}:agent-a"));
    assert_eq!(
        (pending[0].agent_id.clone(), pending[0].attempt),
        (a.clone(), 1)
    );
    assert_eq!(
        store
            .discussion_pending_deliveries(Some(&message))
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(store
        .discussion_invocation_allowed(&turn, &a)
        .await
        .unwrap());
    assert!(!store
        .discussion_invocation_allowed(&turn, &b)
        .await
        .unwrap());

    let queued = |turn: &str, id: &str| AgentQueueRow {
        id: id.into(),
        agent_id: a.clone(),
        position: 0,
        payload: serde_json::json!({ "text": tenant }),
        created_at: T0.into(),
        turn_id: turn.into(),
    };
    assert!(store
        .admit_discussion_delivery(&message, &queued(&turn, &turn))
        .await
        .unwrap());
    assert!(!store
        .admit_discussion_delivery(&message, &queued(&turn, &turn))
        .await
        .unwrap());
    assert!(store
        .discussion_pending_deliveries(None)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        seed.texts("SELECT turn_id FROM agent_queue WHERE {w}agent_id = 'agent-a'")
            .await,
        std::slice::from_ref(&turn)
    );

    let working = store
        .transition_discussion_activity(&turn, "working", None)
        .await
        .unwrap()
        .expect("live invocation");
    assert_eq!(working.activity.state, "working");
    assert_eq!(working.workspace_id, ws());
    assert!(store
        .transition_discussion_activity(&turn, "working", None)
        .await
        .unwrap()
        .is_none());
    let read = store
        .discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &a)
        .await
        .unwrap();
    assert_eq!(
        (read.activity.state.as_str(), read.activity.version),
        ("working", 2)
    );
    assert!(matches!(
        store
            .discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &b)
            .await,
        Err(Error::NotFound(_))
    ));

    let (stopped, changed) = store
        .control_discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &a, 1, false)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(stopped.activity.state, "stopped");
    assert!(seed
        .texts("SELECT turn_id FROM agent_queue WHERE {w}agent_id = 'agent-a'")
        .await
        .is_empty());
    let (_, changed) = store
        .control_discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &a, 1, false)
        .await
        .unwrap();
    assert!(!changed);
    assert!(matches!(
        store
            .control_discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &a, 2, true)
            .await,
        Err(Error::InvalidParams(_))
    ));
    let (retried, changed) = store
        .control_discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &a, 1, true)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(
        (retried.activity.attempt, retried.activity.state.as_str()),
        (2, "queued")
    );
    let retry_turn = format!("discussion:{message}:agent-a:2");
    assert_eq!(retried.turn_id, retry_turn);
    let pending = store.discussion_pending_deliveries(None).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].message.agent_activity[0].attempt, 2);

    // A daemon restart stops the in-flight attempt and abandons its interruption.
    assert!(store
        .admit_discussion_delivery(&message, &queued(&retry_turn, &retry_turn))
        .await
        .unwrap());
    store
        .transition_discussion_activity(&retry_turn, "working", None)
        .await
        .unwrap()
        .expect("live retry");
    seed.exec(&format!(
        "INSERT INTO interrupted_agent ({{c}}agent_id, workspace_id, prev_status, interrupted_at) \
         VALUES ({{v}}'agent-a', '{WS}', 'working', '{T0}')"
    ))
    .await;
    store.recover_discussion_activity().await.unwrap();
    let after = store
        .discussion_activity(&ws(), DEFAULT_CONVERSATION, &message, &a)
        .await
        .unwrap();
    assert_eq!(after.activity.state, "stopped");
    assert_eq!(
        after.activity.error.as_deref(),
        Some("Interrupted by daemon restart.")
    );
    assert!(seed
        .texts("SELECT turn_id FROM agent_queue WHERE {w}agent_id = 'agent-a'")
        .await
        .is_empty());
    assert_eq!(
        seed.texts("SELECT resolution FROM interrupted_agent WHERE {w}agent_id = 'agent-a'")
            .await,
        ["abandoned"]
    );
    let history = store
        .list_discussion_messages(&ws(), None, None, 10)
        .await
        .unwrap();
    assert_eq!(history.messages[0].agent_activity[0].state, "stopped");
    if tenant == "decoy" {
        // Left pending: the subject's recovery must not abandon it.
        seed.exec(
            "UPDATE interrupted_agent SET resolution = 'pending' WHERE {w}agent_id = 'agent-a'",
        )
        .await;
    }
});

seeded_test!(reactions_by_humans_and_agents, |store, seed, _tenant| {
    seed.workspace(&store).await;
    let primary = seed.primary().await;
    let alice = pid("alice");
    let (sent, _) = store
        .send_discussion_message(
            &ws(),
            "r1",
            "react",
            &human(&primary),
            &DiscussionTarget {
                recipient_agent_ids: vec![aid("agent-a")],
                ..DiscussionTarget::default()
            },
        )
        .await
        .unwrap();
    let message = sent.message.id.clone();
    let state = store
        .discussion_reaction_state(&ws(), DEFAULT_CONVERSATION, &message, &alice)
        .await
        .unwrap();
    assert!(state.reactions.is_empty());
    assert_eq!(state.reaction_version, 0);

    let (state, changed) = store
        .set_discussion_reaction(&ws(), DEFAULT_CONVERSATION, &message, &alice, "👍", true)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(state.reaction_version, 1);
    assert_eq!(
        (state.reactions[0].count, state.reactions[0].reacted),
        (1, true)
    );
    assert_eq!(
        state.reactions[0].reactors[0].login.as_deref(),
        Some("alice-l")
    );
    let (_, changed) = store
        .set_discussion_reaction(&ws(), DEFAULT_CONVERSATION, &message, &alice, "👍", true)
        .await
        .unwrap();
    assert!(!changed);

    let bot = DiscussionActor::Agent(aid("agent-a"));
    let (state, changed) = store
        .set_discussion_actor_reaction(&ws(), DEFAULT_CONVERSATION, &message, &bot, "👍", true)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(state.reaction_version, 2);
    assert_eq!(state.reactions[0].count, 2);
    assert_eq!(state.reactions[0].reactors.len(), 2);
    let (state, _) = store
        .set_discussion_actor_reaction(&ws(), DEFAULT_CONVERSATION, &message, &bot, "👀", true)
        .await
        .unwrap();
    let summary: Vec<_> = state
        .reactions
        .iter()
        .map(|r| (r.reaction.as_str(), r.count, r.reacted))
        .collect();
    assert_eq!(summary, [("👍", 2, true), ("👀", 1, true)]);

    let viewed = store
        .discussion_actor_reaction_state(
            &ws(),
            DEFAULT_CONVERSATION,
            &message,
            &DiscussionActor::Human(alice.clone()),
        )
        .await
        .unwrap();
    let summary: Vec<_> = viewed
        .reactions
        .iter()
        .map(|r| (r.reaction.as_str(), r.count, r.reacted))
        .collect();
    assert_eq!(summary, [("👍", 2, true), ("👀", 1, false)]);
    assert_eq!(viewed.reaction_version, 3);

    let (state, changed) = store
        .set_discussion_reaction(&ws(), DEFAULT_CONVERSATION, &message, &alice, "👍", false)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(
        (state.reactions[0].count, state.reactions[0].reacted),
        (1, false)
    );
    let page = store
        .list_discussion_messages_for(&ws(), DEFAULT_CONVERSATION, None, None, 10, Some(&bot))
        .await
        .unwrap();
    let projected: Vec<_> = page.messages[0]
        .reactions
        .iter()
        .map(|r| (r.reaction.as_str(), r.count, r.reacted))
        .collect();
    assert_eq!(projected, [("👍", 1, true), ("👀", 1, true)]);

    for (who, reaction) in [(pid("dave"), "👍"), (alice.clone(), "nope"), (alice, "🙂")] {
        assert!(store
            .set_discussion_reaction(&ws(), DEFAULT_CONVERSATION, &message, &who, reaction, true)
            .await
            .is_err());
    }
    assert!(matches!(
        store
            .discussion_actor_reaction_state(
                &ws(),
                DEFAULT_CONVERSATION,
                &message,
                &DiscussionActor::Agent(aid("agent-b")),
            )
            .await,
        Err(Error::NotFound(_))
    ));
});
