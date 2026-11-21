//! Sender ordering, inheritance and deletion semantics on both storage backends.

use intent_core::{AgentId, AgentSession, ClientId, WorkspaceId};

use crate::harness::{exec, parity_test};

parity_test!(
    browser_sender_receipts_preserve_provenance,
    |store, tenant, db| {
        let workspace = WorkspaceId("__chief__".into());
        let parent = AgentId("sender-parent".into());
        let child = AgentId("sender-child".into());
        for (id, parent_agent_id) in [
            (parent.clone(), None),
            (child.clone(), Some(parent.clone())),
        ] {
            store
                .insert_agent_session(&AgentSession {
                    id,
                    workspace_id: workspace.clone(),
                    parent_agent_id,
                    name: "Sender test".into(),
                    created_at: "2026-01-01T00:00:00Z".into(),
                    updated_at: "2026-01-01T00:00:00Z".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        for id in ["sender-first", "sender-last"] {
            if let Some(db) = db {
                exec(&db.admin, format!(
                "INSERT INTO client (tenant_id, id, first_seen, last_seen) VALUES ('{tenant}', '{id}', 'now', 'now')"
            )).await;
            } else {
                sqlx::query(
                    "INSERT INTO client (id, first_seen, last_seen) VALUES (?, 'now', 'now')",
                )
                .bind(id)
                .execute(store.write_pool())
                .await
                .unwrap();
            }
        }
        let first = ClientId("sender-first".into());
        let last = ClientId("sender-last".into());
        let selected = if tenant == "decoy" { &first } else { &last };
        assert_eq!(store.latest_browser_sender(&parent).await.unwrap(), None);
        store
            .record_browser_sender(&parent, "new", "002", Some(selected))
            .await
            .unwrap();
        // A late older receipt and a retry cannot replace the accepted provenance.
        store
            .record_browser_sender(&parent, "old", "001", Some(&first))
            .await
            .unwrap();
        store
            .record_browser_sender(&parent, "new", "999", None)
            .await
            .unwrap();
        assert_eq!(
            store.latest_browser_sender(&parent).await.unwrap(),
            Some(selected.clone())
        );
        assert_eq!(
            store
                .preferred_browser_sender(&child, &workspace)
                .await
                .unwrap(),
            Some(selected.clone())
        );
        assert_eq!(
            store
                .preferred_browser_sender(&child, &WorkspaceId("other".into()))
                .await
                .unwrap(),
            None
        );
        store
            .record_browser_sender(&child, "unknown", "003", None)
            .await
            .unwrap();
        assert_eq!(
            store
                .preferred_browser_sender(&child, &workspace)
                .await
                .unwrap(),
            None
        );
        // Deleting a client clears its receipt without allowing ancestor inheritance.
        store
            .record_browser_sender(&child, "own", "004", Some(&last))
            .await
            .unwrap();
        if let Some(db) = db {
            exec(
                &db.admin,
                format!("DELETE FROM client WHERE tenant_id = '{tenant}' AND id = 'sender-last'"),
            )
            .await;
        } else {
            sqlx::query("DELETE FROM client WHERE id = 'sender-last'")
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        assert_eq!(store.latest_browser_sender(&child).await.unwrap(), None);
        assert_eq!(
            store
                .preferred_browser_sender(&child, &workspace)
                .await
                .unwrap(),
            None
        );
    }
);
