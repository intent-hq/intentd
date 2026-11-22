//! The multi-tenant orchestrator: one process serving several Postgres
//! tenants, each under its own fenced lease, `Services`, work and
//! self-fence, so stopping one tenant leaves the others running.
//!
//! Never shipped: the crate is not packaged or released, and its Postgres
//! code (the binary, [`serve`]) builds only with `--features postgres`,
//! which only the `store-postgres` CI job turns on. The desktop daemon
//! (`intentd`) keeps refusing Postgres.
//!
//! Per tenant, [`serve::serve_tenant`] runs, in order:
//! 1. `Store::open_postgres_owned`: the schema gate, then the tenant's lease
//!    claim, then provisioning, before anything else writes;
//! 2. `LeaseHeartbeat::spawn`: renews the lease and fires the claim's fence
//!    once it cannot;
//! 3. the tenant's `Services`, with every on-disk path under
//!    [`tenant_dir`], in orchestrator mode;
//! 4. `tenant_self_fence` and `TenantRuntime::start` (`intent_app::tenant`):
//!    the tenant's restores, sweeps and loops, its client connections,
//!    machine links, PTYs, agents and MCP servers, all stopped by the fence;
//! 5. the fence, spawned. A fence step that overruns its budget leaves work
//!    running past the lease, so [`is_fatal`] makes the process exit.
//!
//! Choosing the tenant of a client connection, and so listening for
//! clients, comes with tenant-sticky routing; this binary serves no client
//! yet.

use std::path::{Path, PathBuf};

use intent_core::tenant_fence::SelfFenceReport;

#[cfg(feature = "postgres")]
pub mod serve;

/// The Postgres URL the orchestrator connects to (may carry a password).
pub const POSTGRES_URL_ENV: &str = "INTENTD_POSTGRES_URL";

/// The tenants this process serves: a comma-separated list of tenant ids.
pub const TENANTS_ENV: &str = "INTENTD_POSTGRES_TENANTS";

/// The instance id this process claims its leases as; a fresh one per
/// start when unset.
pub const INSTANCE_ENV: &str = "INTENTD_ORCHESTRATOR_INSTANCE";

/// The exit code when a tenant's self-fence overran: its work may still run
/// after its lease is gone, and only stopping the process stops it.
pub const FENCE_OVERRAN_EXIT: i32 = 70;

/// The tenants named by `raw` ([`TENANTS_ENV`]): comma-separated, each
/// trimmed, none empty or repeated. The ids themselves are checked when
/// each tenant is opened.
///
/// # Errors
/// When the list is empty, names an empty id, or repeats one.
pub fn parse_tenants(raw: &str) -> Result<Vec<String>, String> {
    let mut tenants: Vec<String> = Vec::new();
    for tenant in raw.split(',').map(str::trim) {
        if tenant.is_empty() {
            return Err(format!("{TENANTS_ENV}: empty tenant id in {raw:?}"));
        }
        if tenants.iter().any(|seen| seen == tenant) {
            return Err(format!("{TENANTS_ENV}: tenant {tenant:?} is listed twice"));
        }
        tenants.push(tenant.to_owned());
    }
    Ok(tenants)
}

/// Where tenant `tenant` keeps its files: `<data_dir>/tenants/<tenant>`,
/// with every byte other than an ASCII letter, digit, `-` or `_` written as
/// `%XX`. So no id (`.`, `..`, `a:b`) names another directory or a path
/// that some platform refuses, and two ids never share one.
#[must_use]
pub fn tenant_dir(data_dir: &Path, tenant: &str) -> PathBuf {
    use std::fmt::Write as _;
    let mut name = String::with_capacity(tenant.len());
    for byte in tenant.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') {
            name.push(char::from(byte));
        } else {
            let _ = write!(name, "%{byte:02X}");
        }
    }
    data_dir.join("tenants").join(name)
}

/// This process's instance id: `configured` ([`INSTANCE_ENV`]) when set and
/// not blank, else `orchestrator-<uuid>`.
#[must_use]
pub fn instance_id(configured: Option<&str>) -> String {
    match configured.map(str::trim) {
        Some(instance) if !instance.is_empty() => instance.to_owned(),
        _ => format!("orchestrator-{}", uuid::Uuid::new_v4()),
    }
}

/// Whether a tenant's fence report must stop the process: a step overran
/// its budget, so work it was meant to stop may still run.
#[must_use]
pub fn is_fatal(report: &SelfFenceReport) -> bool {
    !report.overran.is_empty()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use intent_core::tenant_fence::FenceStep;

    use super::*;

    #[test]
    fn tenants_are_a_trimmed_comma_list_without_blanks_or_repeats() {
        assert_eq!(parse_tenants(" a , b:c ").unwrap(), ["a", "b:c"]);
        assert_eq!(parse_tenants("one").unwrap(), ["one"]);
        assert!(parse_tenants("").is_err());
        assert!(parse_tenants("a,,b").is_err());
        assert!(parse_tenants("a, a").is_err());
    }

    #[test]
    fn each_tenant_dir_stays_under_tenants_and_is_its_own() {
        let root = Path::new("/data");
        assert_eq!(tenant_dir(root, "acme-1_x"), root.join("tenants/acme-1_x"));
        assert_eq!(tenant_dir(root, ".."), root.join("tenants/%2E%2E"));
        assert_eq!(tenant_dir(root, "."), root.join("tenants/%2E"));
        assert_eq!(
            tenant_dir(root, "org:prod.eu"),
            root.join("tenants/org%3Aprod%2Eeu")
        );
        assert_ne!(tenant_dir(root, "a.b"), tenant_dir(root, "a%2Eb"));
        for tenant in ["..", ".", "a:b", "x.y"] {
            let dir = tenant_dir(root, tenant);
            assert_eq!(dir.parent(), Some(root.join("tenants").as_path()));
        }
    }

    #[test]
    fn a_blank_instance_gets_a_fresh_id() {
        assert_eq!(instance_id(Some(" host-a ")), "host-a");
        let fresh = instance_id(Some("  "));
        assert!(fresh.starts_with("orchestrator-"));
        assert_ne!(fresh, instance_id(None));
    }

    fn report(overran: Vec<FenceStep>) -> SelfFenceReport {
        SelfFenceReport {
            reason: "lease lost".into(),
            fenced_at: Instant::now(),
            steps: Vec::new(),
            overran,
        }
    }

    #[test]
    fn only_an_overrun_fence_is_fatal() {
        assert!(!is_fatal(&report(Vec::new())));
        assert!(is_fatal(&report(vec![FenceStep::Tasks])));
    }
}
