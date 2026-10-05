//! Owner-authorized invitation suggestions. Selection never replaces pin resolution.
use super::{pr_ops, resolve_api_base_uri, valid_login, Error, Provider, Result, Services, Target};
use intent_sourcecontrol::account_search;
use serde_json::{json, Value};

impl Services {
    async fn search_gitlab_invitation_accounts(
        &self,
        host: &intent_sourcecontrol::GitlabHost,
        query: &str,
        limit: u8,
    ) -> intent_sourcecontrol::Result<Vec<intent_sourcecontrol::UserIdentity>> {
        let mut users = account_search::gitlab(host, None, query, limit).await;
        if matches!(users, Err(intent_sourcecontrol::Error::Auth(_))) {
            if let Some(token) = self.own_gitlab_token(host).await {
                users = account_search::gitlab(host, Some(&token), query, limit).await;
            }
        }
        users
    }

    pub(crate) async fn host_invite_search_accounts_op(
        &self,
        provider: &str,
        host: Option<&str>,
        query: &str,
        limit: Option<u8>,
    ) -> Result<Value> {
        Self::require_administrator("host.invite.searchAccounts")?;
        let provider = Provider::parse(provider)?;
        let target = self.resolve_forge_target(
            provider,
            host.or(Some(match provider {
                Provider::Github => "github.com",
                Provider::Gitlab => "gitlab.com",
            })),
        )?;
        let limit = limit.unwrap_or(8);
        if !(1..=10).contains(&limit) {
            return Err(Error::InvalidParams(
                "limit must be an integer from 1 to 10".into(),
            ));
        }
        let query = query.trim().strip_prefix('@').unwrap_or(query.trim());
        if !query.is_empty() && !valid_login(query, provider) {
            return Err(Error::InvalidParams(
                "query must be a valid account username prefix".into(),
            ));
        }
        if query.len() < 2 {
            return Ok(json!({"users":[]}));
        }
        let (host, users) = match target {
            Target::Github => {
                let api_base = resolve_api_base_uri(self.github_api_base_uri.as_deref());
                (
                    "github.com".to_string(),
                    account_search::github(api_base.as_deref(), query, limit).await,
                )
            }
            Target::Gitlab { host } => {
                let users = self
                    .search_gitlab_invitation_accounts(&host, query, limit)
                    .await;
                (host.host().to_string(), users)
            }
        };
        let users = users.map_err(|e| match e {
            intent_sourcecontrol::Error::Auth(_) => {
                Error::IdentityUnverifiable { host: host.clone() }
            }
            other => pr_ops::map_sc_err(other),
        })?;
        let mut seen = std::collections::HashSet::new();
        let users: Vec<Value> = users.into_iter().filter_map(|user| {
            let id = user.id.filter(|id| *id > 0 && i64::try_from(*id).is_ok())?;
            if !valid_login(&user.login, provider) || !seen.insert(id) { return None; }
            Some(json!({
                "identity": {"provider":provider.as_wire(),"host":host,"externalUserId":id.to_string()},
                "login":user.login,
                "name":user.name,
                "avatarUrl":user.avatar_url,
            }))
        }).take(usize::from(limit)).collect();
        Ok(json!({"users":users}))
    }
}

#[cfg(test)]
mod tests;
