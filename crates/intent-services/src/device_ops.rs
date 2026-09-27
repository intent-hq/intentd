//! Device identity and audience are derived from admission and durable authority.
use std::collections::{HashMap, HashSet};

use intent_core::{Caller, HostRole, PrincipalId, ReverseLiveClient};
use serde_json::Value;

impl crate::Services {
    pub(crate) async fn authenticated_device_list(
        &self,
    ) -> intent_core::Result<Vec<ReverseLiveClient>> {
        let mut rows = self
            .reverse_dispatch
            .as_ref()
            .map(|d| d.authenticated_clients())
            .unwrap_or_default();
        let viewer = match intent_core::current_caller() {
            Some(Caller::Wire { principal_id, .. }) => Some(principal_id),
            _ => None,
        };
        let ids: Vec<_> = rows
            .iter()
            .filter_map(|row| row.principal_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let people: HashMap<_, _> = self
            .store
            .get_device_principals(&ids, viewer.as_ref())
            .await?
            .into_iter()
            .map(|(person, role)| (person.id.clone(), (person, role)))
            .collect();
        rows.retain_mut(|row| {
            let Some(id) = &row.principal_id else {
                return viewer.is_none();
            };
            let Some((person, role)) = people.get(id) else {
                return false;
            };
            row.host_role = Some(*role);
            row.login.clone_from(&person.login);
            row.display_name.clone_from(&person.display_name);
            row.avatar_url.clone_from(&person.avatar_url);
            row.identity = person.identity_key();
            if *role == HostRole::Guest {
                row.capabilities["browserExec"] = false.into();
            }
            true
        });
        Ok(rows)
    }

    pub(crate) async fn project_device_event(&self, data: &mut Value) -> intent_core::Result<()> {
        let Some(id) = data.get("principalId").and_then(Value::as_str) else {
            return Ok(());
        };
        let ids = [PrincipalId::from(id)];
        let people = self.store.get_device_principals(&ids, None).await?;
        let Some((person, role)) = people.first() else {
            return Err(intent_core::Error::NotFound("device principal".into()));
        };
        data["hostRole"] = serde_json::to_value(role).expect("role serializes");
        data["login"] = person.login.clone().into();
        data["displayName"] = person.display_name.clone().into();
        data["avatarUrl"] = person.avatar_url.clone().into();
        if let Some(identity) = person.identity_key() {
            data["identity"] = serde_json::to_value(identity).expect("identity serializes");
        }
        if *role == HostRole::Guest {
            data["capabilities"]["browserExec"] = false.into();
        }
        Ok(())
    }
}
