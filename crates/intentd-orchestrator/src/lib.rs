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
//!    the tenant's restores, sweeps and loops (event retention and the
//!    idempotency reaper included), its client connections, machine links,
//!    PTYs, agents and MCP servers, all stopped by the fence;
//! 5. the fence, spawned. A fence step that overruns its budget leaves work
//!    running past the lease, so [`is_fatal`] makes the process exit.
//!
//! Clients connect to the default tenant ([`DEFAULT_TENANT`]) only, over
//! `intentd`'s own local socket and WSS listener with `intentd`'s auth
//! ([`listen`]); every connection joins that tenant's client connections,
//! which its self-fence closes. Choosing the tenant of a connection comes
//! with tenant-sticky routing.

use std::path::{Path, PathBuf};

use intent_core::tenant_fence::SelfFenceReport;

#[cfg(unix)]
pub mod listen;
#[cfg(feature = "postgres")]
pub mod serve;

/// The Postgres URL the orchestrator connects to (may carry a password).
pub const POSTGRES_URL_ENV: &str = "INTENTD_POSTGRES_URL";

/// The tenants this process serves: a comma-separated list of tenant ids.
/// Unset, the process serves [`DEFAULT_TENANT`] alone.
pub const TENANTS_ENV: &str = "INTENTD_POSTGRES_TENANTS";

/// The one tenant served when [`TENANTS_ENV`] is unset. Until tenant-sticky
/// routing (and client auth) lands, a deployment serves a single tenant.
pub const DEFAULT_TENANT: &str = "default";

/// The instance id this process claims its leases as; a fresh one per
/// start when unset.
pub const INSTANCE_ENV: &str = "INTENTD_ORCHESTRATOR_INSTANCE";

/// The exit code when a tenant's self-fence overran: its work may still run
/// after its lease is gone, and only stopping the process stops it.
pub const FENCE_OVERRAN_EXIT: i32 = 70;

/// The tenants to serve, from [`TENANTS_ENV`]'s value: `None` (unset)
/// serves [`DEFAULT_TENANT`] alone; a set value is [`parse_tenants`], so an
/// empty value, an empty entry or a repeated id is still an error, never the
/// default.
///
/// # Errors
/// As [`parse_tenants`], for a set value.
pub fn tenants_from_env(raw: Option<&str>) -> Result<Vec<String>, String> {
    match raw {
        None => Ok(vec![DEFAULT_TENANT.to_owned()]),
        Some(raw) => parse_tenants(raw),
    }
}

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

/// The longest readable directory name [`tenant_dir`] uses; a longer one
/// is hashed. Well under the 255-byte name limit of common filesystems.
pub const TENANT_DIR_NAME_MAX: usize = 128;

/// Where tenant `tenant` keeps its files: `<data_dir>/tenants/<name>`. The
/// name is the id with every byte other than an ASCII letter, digit, `-` or
/// `_` written as `%XX`, so no id (`.`, `..`, `a:b`) names another directory
/// or a path some platform refuses. When that is longer than
/// [`TENANT_DIR_NAME_MAX`] bytes (an accepted id of many `.` or `:` can
/// reach 384), the name is `~` and the id's SHA-256 instead. The escaping
/// never writes `~`, so the two forms never collide, and no two ids share a
/// directory.
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
    if name.len() > TENANT_DIR_NAME_MAX {
        use sha2::Digest as _;
        name = String::from("~");
        for byte in sha2::Sha256::digest(tenant.as_bytes()) {
            let _ = write!(name, "{byte:02x}");
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

/// Wait for `report` (a tenant's self-fence) on a task of its own and hand
/// the result to `on_stop` as soon as it is ready, whatever the other
/// tenants are doing: a fatal report ([`is_fatal`]) must not wait behind
/// another tenant's slow stop. A self-fence task that panicked or was
/// cancelled counts as overrun: its steps may not have run.
pub fn watch_report(
    report: tokio::task::JoinHandle<SelfFenceReport>,
    on_stop: impl FnOnce(SelfFenceReport) + Send + 'static,
) {
    tokio::spawn(async move {
        let report = report.await.unwrap_or_else(|e| SelfFenceReport {
            reason: format!("the self-fence task ended: {e}"),
            fenced_at: std::time::Instant::now(),
            steps: Vec::new(),
            overran: vec![intent_core::tenant_fence::FenceStep::Tasks],
        });
        on_stop(report);
    });
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
    fn an_unset_tenant_list_serves_the_default_tenant_only() {
        assert_eq!(tenants_from_env(None).unwrap(), [DEFAULT_TENANT]);
        assert_eq!(DEFAULT_TENANT, "default");
    }

    #[test]
    fn a_set_tenant_list_is_parsed_as_before() {
        assert_eq!(tenants_from_env(Some("a,b")).unwrap(), ["a", "b"]);
        assert!(
            tenants_from_env(Some("")).is_err(),
            "an explicitly empty list is an error, not the default"
        );
        assert!(tenants_from_env(Some("a,,b")).is_err());
        assert!(tenants_from_env(Some("a,a")).is_err());
    }

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
    fn a_long_tenant_id_gets_a_bounded_name_of_its_own() {
        let root = Path::new("/data");
        // The longest ids the store accepts, all escaped bytes.
        let colons = ":".repeat(128);
        let dots = ".".repeat(128);
        let mut names = Vec::new();
        for tenant in [
            colons.as_str(),
            dots.as_str(),
            &"a".repeat(128),
            &format!("{}a", ".".repeat(127)),
        ] {
            let dir = tenant_dir(root, tenant);
            assert_eq!(dir.parent(), Some(root.join("tenants").as_path()));
            let name = dir.file_name().unwrap().to_str().unwrap().to_owned();
            assert!(name.len() <= TENANT_DIR_NAME_MAX, "{name}");
            assert_eq!(dir, tenant_dir(root, tenant), "stable");
            names.push(name);
        }
        assert!(names[0].starts_with('~') && names[1].starts_with('~'));
        assert_eq!(
            names[2],
            "a".repeat(128),
            "a short enough id stays readable"
        );
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 4, "no two ids share a directory");
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

    #[tokio::test]
    async fn a_fatal_report_is_seen_while_another_tenant_is_still_stopping() {
        let (seen_tx, mut seen) = tokio::sync::mpsc::unbounded_channel();
        // A: still settling its writes (up to a minute in production).
        let slow = tokio::spawn(std::future::pending::<SelfFenceReport>());
        // B: done, and fatal.
        let fatal = tokio::spawn(async { report(vec![FenceStep::Tasks]) });
        for (tenant, handle) in [("a", slow), ("b", fatal)] {
            let seen = seen_tx.clone();
            watch_report(handle, move |report| {
                let _ = seen.send((tenant, is_fatal(&report)));
            });
        }
        let first = tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
            seen.recv(),
        )
        .await
        .expect("B's report does not wait behind A's")
        .unwrap();
        assert_eq!(first, ("b", true));
    }

    #[tokio::test]
    async fn a_self_fence_that_panicked_is_fatal() {
        let (seen_tx, mut seen) = tokio::sync::mpsc::unbounded_channel();
        let panicked = tokio::spawn(async { panic!("self-fence failed") });
        watch_report(panicked, move |report| {
            let _ = seen_tx.send(is_fatal(&report));
        });
        assert_eq!(seen.recv().await, Some(true));
    }

    #[test]
    fn only_an_overrun_fence_is_fatal() {
        assert!(!is_fatal(&report(Vec::new())));
        assert!(is_fatal(&report(vec![FenceStep::Tasks])));
    }
}
