//! Opaque continuation metadata on its original read lifetime. No page payload,
//! provider credential, or cross-session authority is retained here.

use intent_core::repository_resource_pages::{
    RepositoryListKind, RepositoryListQuery, RepositoryPageQuery,
};
use intent_core::{RepositoryTarget, ReviewTarget};

use super::{unavailable, CacheDelivery, Error, Result, VecDeque, LIMIT};

#[derive(Clone, PartialEq, Eq)]
pub(super) enum QueryKey {
    List(RepositoryTarget, RepositoryListKind, RepositoryListQuery),
    Discussions(ReviewTarget, RepositoryPageQuery),
}

impl QueryKey {
    pub(super) fn list(
        repository: &RepositoryTarget,
        kind: RepositoryListKind,
        query: &RepositoryListQuery,
    ) -> Self {
        let mut query = query.clone();
        query.cursor = None;
        Self::List(repository.clone(), kind, query)
    }

    pub(super) fn discussions(target: &ReviewTarget, page: &RepositoryPageQuery) -> Self {
        let mut page = page.clone();
        page.cursor = None;
        Self::Discussions(target.clone(), page)
    }
}

struct Entry {
    token: String,
    query: QueryKey,
    continuation: Continuation,
}

#[derive(Clone)]
pub(super) struct Continuation {
    pub(super) provider: String,
    pub(super) delivery: CacheDelivery,
}

#[derive(Default)]
pub(super) struct Cursors(VecDeque<Entry>);

impl Cursors {
    pub(super) fn get(&self, query: &QueryKey, token: &str) -> Result<Continuation> {
        self.0
            .iter()
            .find(|entry| entry.token == token && entry.query == *query)
            .map(|entry| entry.continuation.clone())
            .ok_or_else(|| Error::InvalidParams("Repository continuation unavailable".into()))
    }

    pub(super) fn insert(
        &mut self,
        query: QueryKey,
        provider: Option<String>,
        delivery: &CacheDelivery,
    ) -> Result<Option<String>> {
        let Some(provider) = provider else {
            return Ok(None);
        };
        if provider.is_empty() || provider.len() > 16_384 || provider.chars().any(char::is_control)
        {
            return Err(unavailable());
        }
        let token = uuid::Uuid::new_v4().to_string();
        if self.0.len() == LIMIT {
            self.0.pop_front();
        }
        self.0.push_back(Entry {
            token: token.clone(),
            query,
            continuation: Continuation {
                provider,
                delivery: delivery.clone(),
            },
        });
        Ok(Some(token))
    }
}
