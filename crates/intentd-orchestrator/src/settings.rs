//! Tenant settings for the orchestrator: every tenant's settings document
//! is its Postgres row ([`TenantConfigStore`]), not a `config.toml` under
//! its directory, so the instance that takes a tenant over reads what the
//! previous one wrote. The desktop daemon keeps its file and none of this.
//!
//! Each write is a compare-and-set on the row's revision (two writers from
//! one revision: one wins, the other gets `Error::Conflict`), and the
//! tenant's `StoreSettingsFollower` re-reads the revision to adopt another
//! process's write (`intent_services::settings_store_follower`).

use intent_core::Result;
use intent_services::{SettingsDocumentStore, SettingsWrite, StoredSettings};
use intent_store::postgres::{ConfigWrite, TenantConfigStore};

/// A tenant's [`TenantConfigStore`] as the synchronous document store the
/// settings registry takes.
#[derive(Debug, Clone)]
pub struct TenantSettings(TenantConfigStore);

impl TenantSettings {
    /// The settings document of `store`'s tenant.
    ///
    /// # Errors
    /// As [`TenantConfigStore::new`]: not a Postgres store, or not on the
    /// multi-thread runtime.
    pub fn new(store: &intent_store::Store) -> Result<Self> {
        TenantConfigStore::new(store).map(Self)
    }
}

impl SettingsDocumentStore for TenantSettings {
    fn load(&self) -> Result<Option<StoredSettings>> {
        Ok(self.0.load_blocking()?.map(|stored| StoredSettings {
            text: stored.body,
            revision: stored.revision,
        }))
    }

    fn compare_and_set(&self, expected: u64, text: &str) -> Result<SettingsWrite> {
        Ok(match self.0.compare_and_set_blocking(expected, text)? {
            ConfigWrite::Written(revision) => SettingsWrite::Written(revision),
            ConfigWrite::Conflict(current) => SettingsWrite::Conflict(current),
        })
    }

    fn label(&self) -> String {
        format!("tenant_config/{}", self.0.tenant())
    }
}
