//! Fresh, prompt-free catalogs. This path never reads or updates model caches.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use tokio::process::Command;

use super::catalog_io::{self, Authentication, Rpc, PHASE_TIMEOUT};
use super::process::ProbeProcess;
use super::{CodexInspection, CodexLaunch, CodexRuntimeReport, ProviderLaunch, UnknownReason};

const PAGE_LIMIT: usize = 10;
const MODEL_LIMIT: usize = 2000;

/// Fixed report text only. In particular, never retain a JSON-RPC error body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CatalogFailure {
    AdapterUnavailable,
    RuntimeUnverified,
    AuthenticationUnavailable,
    UnsupportedCapability,
    IsolationFailed,
    SpawnFailed,
    TimedOut,
    OutputLimit,
    PaginationLimit,
    InvalidResponse,
    RequestFailed,
    ProcessExited,
    CleanupFailed,
    FileMissing,
}

impl CatalogFailure {
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::AdapterUnavailable => "selected adapter is unavailable",
            Self::RuntimeUnverified => "selected adapter's runtime could not be verified",
            Self::AuthenticationUnavailable => "authentication is unavailable for this probe",
            Self::UnsupportedCapability => "provider does not support this diagnostic conversation",
            Self::IsolationFailed | Self::FileMissing => {
                "isolated diagnostic state could not be prepared"
            }
            Self::SpawnFailed => {
                "diagnostic process could not start safely; temporary state may be retained"
            }
            Self::TimedOut => "catalog probe exceeded its deadline",
            Self::OutputLimit => "catalog probe exceeded its output limit",
            Self::PaginationLimit => "catalog pagination exceeded its bound or repeated a cursor",
            Self::InvalidResponse => "provider returned an invalid diagnostic response",
            Self::RequestFailed => "provider rejected the diagnostic request",
            Self::ProcessExited => "provider closed the diagnostic connection",
            Self::CleanupFailed => {
                "probe cleanup could not be confirmed; temporary state was retained"
            }
        }
    }
}

impl From<UnknownReason> for CatalogFailure {
    fn from(reason: UnknownReason) -> Self {
        match reason {
            UnknownReason::CleanupFailed => Self::CleanupFailed,
            UnknownReason::TimedOut => Self::TimedOut,
            UnknownReason::OutputLimit => Self::OutputLimit,
            _ => Self::SpawnFailed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CatalogSource {
    AcpAvailableModels,
    AcpConfigOptions,
    CodexModelList,
}

impl CatalogSource {
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::AcpAvailableModels => "ACP advertised model; may be synthesized by the adapter",
            Self::AcpConfigOptions => {
                "ACP configuration choice; not confirmed upstream availability"
            }
            Self::CodexModelList => {
                "selected runtime model/list observation; not an entitlement check"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogModel {
    /// Original ID, not normalized. Unsafe/sensitive IDs are withheld entirely.
    pub id: String,
    pub source: CatalogSource,
    /// The runtime's separate model identifier, when present. Also unnormalized.
    pub model: Option<String>,
    pub hidden: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    /// False means a successful ACP session had no catalog field at all.
    pub advertised: bool,
    pub models: Vec<CatalogModel>,
    /// Withheld IDs make a negative membership comparison inconclusive.
    pub withheld_model_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "camelCase")]
pub enum CatalogOutcome {
    Success(Catalog),
    Failed(CatalogFailure),
}

impl From<Result<Catalog, CatalogFailure>> for CatalogOutcome {
    fn from(value: Result<Catalog, CatalogFailure>) -> Self {
        match value {
            Ok(catalog) => Self::Success(catalog),
            Err(reason) => Self::Failed(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ModelObservation {
    PresentInBoth,
    PresentInAcpOnly,
    PresentInRawOnly,
    AbsentFromBothObservedCatalogs,
    Inconclusive,
}

impl ModelObservation {
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::PresentInBoth => "ID observed in both catalogs; this does not verify entitlement",
            Self::PresentInAcpOnly => "ID observed only in ACP; adapter choices may be synthesized",
            Self::PresentInRawOnly => "ID observed only in the selected runtime catalog",
            Self::AbsentFromBothObservedCatalogs => "ID absent from both observed catalogs; this does not establish an account restriction",
            Self::Inconclusive => "comparison unavailable because a catalog failed or withheld model IDs",
        }
    }
}

/// The safe serialization/rendering boundary. No account or raw child payloads.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexCatalogReport {
    pub runtime: CodexRuntimeReport,
    pub acp: CatalogOutcome,
    pub raw: CatalogOutcome,
}

impl CodexCatalogReport {
    /// Compare exact original identifiers (including raw `model` aliases).
    /// Effort suffixes are not guessed or silently normalized.
    #[must_use]
    pub fn observe(&self, id: &str) -> ModelObservation {
        let (CatalogOutcome::Success(acp), CatalogOutcome::Success(raw)) = (&self.acp, &self.raw)
        else {
            return ModelObservation::Inconclusive;
        };
        if acp.withheld_model_count != 0 || raw.withheld_model_count != 0 {
            return ModelObservation::Inconclusive;
        }
        let contains = |catalog: &Catalog| {
            catalog
                .models
                .iter()
                .any(|row| row.id == id || row.model.as_deref() == Some(id))
        };
        match (contains(acp), contains(raw)) {
            (true, true) => ModelObservation::PresentInBoth,
            (true, false) => ModelObservation::PresentInAcpOnly,
            (false, true) => ModelObservation::PresentInRawOnly,
            (false, false) => ModelObservation::AbsentFromBothObservedCatalogs,
        }
    }
}

#[derive(Clone, Copy)]
struct Limits {
    timeout: Duration,
    pages: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            timeout: PHASE_TIMEOUT,
            pages: PAGE_LIMIT,
        }
    }
}

impl CodexLaunch {
    /// Explicit opt-in only: may download the selected managed npm package.
    /// Each catalog has a 30s deadline and 1 MiB limit per output stream; raw
    /// pagination is at most 10 pages/2000 rows. Existing bounded inspection
    /// and process cleanup have their own deadlines. No prompt/login is sent.
    /// Discovery should already have run off the async executor.
    pub async fn fresh_catalogs(&self) -> CodexCatalogReport {
        let auth = tokio::time::timeout(PHASE_TIMEOUT, Authentication::capture(self))
            .await
            .unwrap_or(Err(CatalogFailure::TimedOut));
        self.catalogs_with_auth(auth, Limits::default()).await
    }

    async fn catalogs_with_auth(
        &self,
        auth: Result<Authentication, CatalogFailure>,
        limits: Limits,
    ) -> CodexCatalogReport {
        let mut inspection = self.inspect_local().await;
        let mut auth = match auth {
            Ok(auth) => auth,
            Err(reason) => {
                return CodexCatalogReport {
                    runtime: inspection.report,
                    acp: CatalogOutcome::Failed(reason),
                    raw: CatalogOutcome::Failed(reason),
                }
            }
        };
        if matches!(self.selection(), ProviderLaunch::Bare { .. }) {
            return finish(
                inspection,
                Err(CatalogFailure::AdapterUnavailable),
                Err(CatalogFailure::RuntimeUnverified),
                &auth,
            );
        }
        if matches!(self.selection(), ProviderLaunch::Managed { .. }) {
            // Explicit launch may install even if it never reaches the capture
            // hook. Do not reuse the offline reason claiming nothing installed.
            inspection = self.unknown(UnknownReason::PackageUnreadable);
        }

        let acp_deadline = tokio::time::Instant::now() + limits.timeout;
        let started = tokio::time::timeout_at(acp_deadline, self.start_acp(&auth))
            .await
            .unwrap_or(Err(CatalogFailure::TimedOut));
        let mut guard = None;
        let acp = match started {
            Ok((mut process, home)) => {
                let cwd = home.clone();
                let auth_ref = &auth;
                let remaining = acp_deadline.saturating_duration_since(tokio::time::Instant::now());
                let result = converse(&mut process, remaining, |mut rpc| async move {
                    let initialized = rpc
                        .request(
                            "initialize",
                            json!({"protocolVersion":1,
                        "clientInfo":{"name":"intentd-doctor","version":"1"},
                        "clientCapabilities":{}}),
                        )
                        .await?;
                    if initialized.get("protocolVersion").and_then(Value::as_u64) != Some(1) {
                        return Err(CatalogFailure::UnsupportedCapability);
                    }
                    let session = rpc
                        .request("session/new", json!({"cwd":cwd,"mcpServers":[]}))
                        .await?;
                    if session.get("sessionId").and_then(Value::as_str).is_none() {
                        return Err(CatalogFailure::InvalidResponse);
                    }
                    rpc.late_notifications().await?;
                    let mut catalog = parse_acp(&session, auth_ref)?;
                    for notification in &rpc.notifications {
                        if notification
                            .get("sessionId")
                            .is_some_and(|id| Some(id) != session.get("sessionId"))
                        {
                            continue;
                        }
                        let payload = notification
                            .get("update")
                            .or_else(|| notification.get("sessionUpdate"))
                            .unwrap_or(notification);
                        append(&mut catalog, parse_acp(payload, auth_ref)?)?;
                    }
                    Ok(catalog)
                })
                .await;
                // Keep the actual launch's private npm installation alive until
                // inspection AND the raw probe have finished. All stdio has
                // closed; the guard retains process ownership on every outcome.
                if matches!(self.selection(), ProviderLaunch::Managed { .. }) {
                    if let Ok(bytes) = catalog_io::read_file(&home.join("entry.json")).await {
                        if let Ok(path) = serde_json::from_slice::<std::path::PathBuf>(&bytes) {
                            inspection = self.inspect_materialized(&path).await;
                        }
                    }
                }
                guard = Some(process);
                result
            }
            Err(reason) => Err(reason),
        };
        let raw = match inspection.runtime.as_ref() {
            Some(runtime) => self.raw_catalog(runtime.command(), &mut auth, limits).await,
            None => Err(CatalogFailure::RuntimeUnverified),
        };
        let acp = if let Some(mut process) = guard {
            match process.cleanup().await {
                Ok(()) => acp,
                Err(reason) => Err(reason.into()),
            }
        } else {
            acp
        };
        finish(inspection, acp, raw, &auth)
    }

    async fn start_acp(
        &self,
        auth: &Authentication,
    ) -> Result<(ProbeProcess, std::path::PathBuf), CatalogFailure> {
        let home = auth.home().await?;
        let path = home.path().to_owned();
        let mut command = intent_acp::spawn::build_command(&self.spawn_options());
        auth.isolate(&mut command, self, &path);
        // An unresolved relative override cannot identify the same runtime in a
        // fresh cwd. Do not silently probe the adapter's default instead.
        if self.runtime_override_path().is_err() {
            return Err(CatalogFailure::RuntimeUnverified);
        }
        if matches!(self.selection(), ProviderLaunch::Managed { .. }) {
            let preload = path.join("capture.cjs");
            catalog_io::private_file(&preload, include_bytes!("catalog_entry.cjs")).await?;
            command
                .env(
                    "NODE_OPTIONS",
                    format!(
                        "--require={}",
                        serde_json::to_string(&preload)
                            .map_err(|_| CatalogFailure::IsolationFailed)?
                    ),
                )
                .env("INTENT_CODEX_ENTRY", path.join("entry.json"));
        }
        Ok((
            ProbeProcess::spawn(command, home)
                .await
                .map_err(CatalogFailure::from)?,
            path,
        ))
    }

    async fn raw_catalog(
        &self,
        mut command: Command,
        auth: &mut Authentication,
        limits: Limits,
    ) -> Result<Catalog, CatalogFailure> {
        let deadline = tokio::time::Instant::now() + limits.timeout;
        let home = tokio::time::timeout_at(deadline, auth.home())
            .await
            .unwrap_or(Err(CatalogFailure::TimedOut))?;
        auth.isolate(&mut command, self, home.path());
        // app-server defaults to newline-delimited stdio; no thread/turn is created.
        command.arg("app-server");
        let mut guard = tokio::time::timeout_at(deadline, ProbeProcess::spawn(command, home))
            .await
            .map_err(|_| CatalogFailure::TimedOut)??;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let result = converse(&mut guard, remaining, |mut rpc| async move {
            rpc.request(
                "initialize",
                json!({"clientInfo":{"name":"intentd-doctor","version":"1"},
                "capabilities":{"experimentalApi":true}}),
            )
            .await?;
            rpc.send(json!({"jsonrpc":"2.0","method":"initialized","params":{}}))
                .await?;
            let account = rpc
                .request("account/read", json!({"refreshToken":false}))
                .await?;
            if let Some(account) = account.get("account") {
                catalog_io::collect_secrets(account, &mut auth.secrets);
            }
            match (
                account.get("requiresOpenaiAuth").and_then(Value::as_bool),
                account.get("account"),
            ) {
                (Some(true), Some(Value::Null)) => {
                    return Err(CatalogFailure::AuthenticationUnavailable)
                }
                (Some(false), Some(Value::Null)) | (Some(_), Some(Value::Object(_))) => {}
                _ => return Err(CatalogFailure::InvalidResponse),
            }
            let mut catalog = Catalog {
                advertised: true,
                ..Catalog::default()
            };
            let mut cursor = None::<String>;
            let mut seen = BTreeSet::new();
            for _ in 0..limits.pages {
                let page = rpc
                    .request(
                        "model/list",
                        json!({"cursor":cursor,"limit":100,"includeHidden":true}),
                    )
                    .await?;
                let rows = page
                    .get("data")
                    .and_then(Value::as_array)
                    .ok_or(CatalogFailure::InvalidResponse)?;
                for row in rows {
                    push_model(&mut catalog, row, "id", CatalogSource::CodexModelList, auth)?;
                }
                match page.get("nextCursor") {
                    None | Some(Value::Null) => return Ok(catalog),
                    Some(Value::String(next))
                        if !next.is_empty() && next.len() <= 4096 && seen.insert(next.clone()) =>
                    {
                        cursor = Some(next.clone());
                    }
                    Some(Value::String(_)) => return Err(CatalogFailure::PaginationLimit),
                    _ => return Err(CatalogFailure::InvalidResponse),
                }
            }
            Err(CatalogFailure::PaginationLimit)
        })
        .await;
        guard.cleanup().await?;
        result
    }
}

async fn converse<F, Fut>(
    guard: &mut ProbeProcess,
    timeout: Duration,
    conversation: F,
) -> Result<Catalog, CatalogFailure>
where
    F: FnOnce(Rpc) -> Fut,
    Fut: std::future::Future<Output = Result<Catalog, CatalogFailure>>,
{
    let rpc = Rpc::new(guard)?;
    let stderr = guard.stderr.take().ok_or(CatalogFailure::SpawnFailed)?;
    tokio::time::timeout(timeout, async {
        tokio::select! {
            biased;
            error = catalog_io::discard_stderr(stderr) => Err(error),
            result = conversation(rpc) => result,
        }
    })
    .await
    .unwrap_or(Err(CatalogFailure::TimedOut))
}

fn parse_acp(value: &Value, auth: &Authentication) -> Result<Catalog, CatalogFailure> {
    let mut catalog = Catalog::default();
    if value
        .get("models")
        .is_some_and(|v| !v.is_null() && !v.is_object() && !v.is_array())
    {
        return Err(CatalogFailure::InvalidResponse);
    }
    for rows in [
        value.pointer("/models/availableModels"),
        value.get("availableModels"),
        value.pointer("/models/available"),
        value.get("models").filter(|v| v.is_array()),
    ]
    .into_iter()
    .flatten()
    {
        catalog.advertised = true;
        for row in rows.as_array().ok_or(CatalogFailure::InvalidResponse)? {
            let key = if row.get("modelId").is_some() {
                "modelId"
            } else {
                "id"
            };
            push_model(
                &mut catalog,
                row,
                key,
                CatalogSource::AcpAvailableModels,
                auth,
            )?;
        }
    }
    if let Some(options) = value.get("configOptions") {
        for option in options.as_array().ok_or(CatalogFailure::InvalidResponse)? {
            if option.get("id").and_then(Value::as_str) == Some("model")
                || option.get("category").and_then(Value::as_str) == Some("model")
            {
                catalog.advertised = true;
                let rows = option
                    .get("options")
                    .and_then(Value::as_array)
                    .ok_or(CatalogFailure::InvalidResponse)?;
                for row in rows {
                    if let Some(group) = row.get("options") {
                        for row in group.as_array().ok_or(CatalogFailure::InvalidResponse)? {
                            push_model(
                                &mut catalog,
                                row,
                                "value",
                                CatalogSource::AcpConfigOptions,
                                auth,
                            )?;
                        }
                    } else {
                        push_model(
                            &mut catalog,
                            row,
                            "value",
                            CatalogSource::AcpConfigOptions,
                            auth,
                        )?;
                    }
                }
            }
        }
    }
    Ok(catalog)
}

fn push_model(
    catalog: &mut Catalog,
    row: &Value,
    key: &str,
    source: CatalogSource,
    auth: &Authentication,
) -> Result<(), CatalogFailure> {
    if catalog.models.len() + catalog.withheld_model_count >= MODEL_LIMIT {
        return Err(CatalogFailure::OutputLimit);
    }
    let id = row
        .get(key)
        .and_then(Value::as_str)
        .ok_or(CatalogFailure::InvalidResponse)?;
    let Some(id) = auth.model_id(id) else {
        catalog.withheld_model_count += 1;
        return Ok(());
    };
    let model = if source == CatalogSource::CodexModelList {
        match row.get("model") {
            Some(Value::String(value)) => {
                if let Some(value) = auth.model_id(value) {
                    Some(value)
                } else {
                    catalog.withheld_model_count += 1;
                    return Ok(());
                }
            }
            None | Some(Value::Null) => None,
            _ => return Err(CatalogFailure::InvalidResponse),
        }
    } else {
        None
    };
    catalog.models.push(CatalogModel {
        id,
        source,
        model,
        hidden: row.get("hidden").and_then(Value::as_bool),
    });
    Ok(())
}

fn append(catalog: &mut Catalog, more: Catalog) -> Result<(), CatalogFailure> {
    catalog.advertised |= more.advertised;
    catalog.withheld_model_count += more.withheld_model_count;
    catalog.models.extend(more.models);
    if catalog.models.len() + catalog.withheld_model_count > MODEL_LIMIT {
        return Err(CatalogFailure::OutputLimit);
    }
    Ok(())
}

fn finish(
    mut inspection: CodexInspection,
    mut acp: Result<Catalog, CatalogFailure>,
    mut raw: Result<Catalog, CatalogFailure>,
    auth: &Authentication,
) -> CodexCatalogReport {
    // The raw account check can discover account identifiers AFTER ACP ended.
    for catalog in [&mut acp, &mut raw]
        .into_iter()
        .filter_map(|v| v.as_mut().ok())
    {
        let before = catalog.models.len();
        catalog.models.retain(|row| {
            auth.model_id(&row.id).is_some()
                && row
                    .model
                    .as_ref()
                    .is_none_or(|id| auth.model_id(id).is_some())
        });
        catalog.withheld_model_count += before - catalog.models.len();
    }
    auth.scrub(&mut inspection.report.launch_program);
    for text in [
        &mut inspection.report.adapter_path,
        &mut inspection.report.runtime_path,
    ]
    .into_iter()
    .flatten()
    {
        auth.scrub(text);
    }
    CodexCatalogReport {
        runtime: inspection.report,
        acp: acp.into(),
        raw: raw.into(),
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
