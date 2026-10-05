//! Explicit staged activation. Deferred selection preserves the caller's legacy
//! launch path; a selected plan's build errors never authorize fallback.
use std::path::Path;

use super::{
    auth, policy, AuthModelContext, ManagedProviderProfile, ProfileDirectory, ProfilePurpose,
    ProfileResult, RuntimeIdentity,
};
use intent_acp::NormalizedMcpServers;

/// Safe diagnostics contain no executable, configuration values, or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredReason {
    ProviderControls,
    RuntimeVersion,
    Platform,
    AdapterDelivery,
    PiGatewayDelivery,
    PolicyAuthority,
    PolicyAcquisition,
    ExclusiveManagedMcp,
    AuthProjection,
}

impl DeferredReason {
    #[must_use]
    pub const fn explanation(self) -> &'static str {
        match self {
            Self::ProviderControls => "native source isolation is not verified for this provider; retain existing behavior",
            Self::RuntimeVersion => "installed native runtime is outside the verified version",
            Self::Platform => "managed profiles are verified on Linux x86_64 only",
            Self::AdapterDelivery => "adapter/SDK delivery is outside the verified combination",
            Self::PiGatewayDelivery => "Pi ACP and external MCP require verified delivery through the Intent gateway extension",
            Self::PolicyAuthority => "resolve all applicable organization, remote and host policy sources before managed activation",
            Self::PolicyAcquisition => "applicable policy cannot be acquired or represented by the supported parser",
            Self::ExclusiveManagedMcp => "exclusive Claude managed-mcp.json conflicts with strict Intent MCP delivery",
            Self::AuthProjection => "authentication or model routing requires an unsupported projection",
        }
    }
}

/// The account/policy acquisition owner must positively resolve source coverage.
/// Missing local files alone do not justify `LocalFilesOnly`. For organization
/// accounts supply effective restrictions (native precedence already resolved)
/// through additional sources; otherwise use `Unresolved`. No default is provided.
#[derive(Clone, Copy)]
pub enum PolicyAuthority {
    LocalFilesOnly,
    EffectiveSourcesResolved,
    Unresolved,
}

pub struct PolicyAcquisition<'a> {
    /// Actual system root (normally /etc); injection exists for fixture hosts.
    pub etc_root: &'a Path,
    pub authority: PolicyAuthority,
    /// Host-wide Intent policy and resolved effective native sources, if any.
    pub additional: &'a [policy::PolicySource],
}

pub struct SelectionRequest<'a> {
    pub provider: &'a str,
    pub runtime: RuntimeIdentity<'a>,
    /// Mandatory for the verified Claude ACP path; native CLI needs no SDK.
    pub sdk_version: Option<&'a str>,
    pub purpose: ProfilePurpose,
    pub approved_servers: &'a NormalizedMcpServers,
    pub auth_model: &'a AuthModelContext,
    /// Already resolved Intent/workspace instructions, including owned skills.
    /// Empty means deliberately empty; native context discovery stays disabled.
    pub instructions: &'a str,
    /// Whether the resolved instructions include an Intent skill catalog.
    pub has_skill_instructions: bool,
    pub policy: PolicyAcquisition<'a>,
}

pub enum ProfileSelection<'a> {
    Managed(Box<ManagedProfilePlan<'a>>),
    Deferred(DeferredReason),
}

/// Constructible only by selection. Holds the exact inputs and policy snapshot
/// that were selected; callers cannot turn an arbitrary candidate into a plan.
pub struct ManagedProfilePlan<'a> {
    request: SelectionRequest<'a>,
    policy: policy::HostPolicySnapshot,
}

pub(super) fn runtime_decision(
    provider: &str,
    runtime: &RuntimeIdentity<'_>,
    sdk_version: Option<&str>,
    has_servers: bool,
) -> Result<(), DeferredReason> {
    if !matches!(provider, "claude-code" | "pi") {
        return Err(DeferredReason::ProviderControls);
    }
    if runtime.os != "linux" || runtime.arch != "x86_64" {
        return Err(DeferredReason::Platform);
    }
    if provider == "pi" {
        if runtime.native_version != "0.81.0" {
            return Err(DeferredReason::RuntimeVersion);
        }
        if runtime.adapter_version.is_some() || has_servers {
            return Err(DeferredReason::PiGatewayDelivery);
        }
    } else {
        if runtime.native_version != "2.1.280" {
            return Err(DeferredReason::RuntimeVersion);
        }
        if runtime.adapter_version.is_some()
            && (runtime.adapter_version != Some("0.81.1") || sdk_version != Some("0.3.280"))
        {
            return Err(DeferredReason::AdapterDelivery);
        }
    }
    Ok(())
}

/// Decide without creating/modifying profile files. Unknown native controls,
/// acquisition or auth routes defer explicitly; this is not a launch failure.
/// Policy denial of a particular catalog is checked by the selected builder and
/// cannot become a legacy fallback.
#[must_use]
pub fn select(request: SelectionRequest<'_>) -> ProfileSelection<'_> {
    if let Err(reason) = runtime_decision(
        request.provider,
        &request.runtime,
        request.sdk_version,
        request.purpose == ProfilePurpose::Interactive && !request.approved_servers.is_empty(),
    ) {
        return ProfileSelection::Deferred(reason);
    }
    if matches!(request.policy.authority, PolicyAuthority::Unresolved) {
        return ProfileSelection::Deferred(DeferredReason::PolicyAuthority);
    }
    if request.provider == "claude-code" {
        match std::fs::symlink_metadata(
            request.policy.etc_root.join("claude-code/managed-mcp.json"),
        ) {
            Ok(_) => return ProfileSelection::Deferred(DeferredReason::ExclusiveManagedMcp),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return ProfileSelection::Deferred(DeferredReason::PolicyAcquisition),
        }
        if request.auth_model.endpoint.is_some()
            || request.auth_model.codex_routing_toml.is_some()
            || request
                .auth_model
                .claude_settings
                .as_ref()
                .is_some_and(|s| auth::project_claude_settings(s).is_err())
        {
            return ProfileSelection::Deferred(DeferredReason::AuthProjection);
        }
    }
    let Ok(policy) = policy::read_linux_system_policy(
        request.provider,
        request.policy.etc_root,
        request.policy.additional,
    ) else {
        return ProfileSelection::Deferred(DeferredReason::PolicyAcquisition);
    };
    ProfileSelection::Managed(Box::new(ManagedProfilePlan { request, policy }))
}

impl ManagedProfilePlan<'_> {
    /// Build after selection. I/O, invalid auth, denied policy and invalid
    /// ephemeral inputs are errors, never a `Deferred` result. Allocate a stable
    /// session directory for interactive use and retain it through child reap.
    /// # Errors
    /// Any selected profile construction or policy enforcement failure is fatal
    /// to this managed launch; the caller must not retry through its legacy path.
    pub fn build(self, directory: ProfileDirectory) -> ProfileResult<ManagedProviderProfile> {
        if self.request.purpose == ProfilePurpose::Ephemeral && self.request.has_skill_instructions
        {
            return Err(super::ProfileError::EphemeralCatalog);
        }
        // Reacquire before publication so a newly installed exclusive managed
        // source or unreadable restriction fails this launch instead of falling back.
        let current = policy::read_linux_system_policy(
            self.request.provider,
            self.request.policy.etc_root,
            self.request.policy.additional,
        )?;
        if current.identity() != self.policy.identity() {
            return Err(super::ProfileError::UnsupportedPolicy {
                source: "managed selection".into(),
                requirement: "applicable policy changed; reselect before launching",
            });
        }
        self.policy
            .validate_skills(self.request.has_skill_instructions)?;
        let mut profile = super::prepare_candidate_with_instructions(
            self.request.provider,
            self.request.purpose,
            directory,
            self.request.approved_servers,
            self.request.auth_model,
            &self.policy,
            self.request.instructions,
        )?;
        profile.capability.source_baseline = match self.request.provider {
            "claude-code" => {
                "Claude 2.1.280 Linux x86_64; native and ACP 0.81.1 / SDK 0.3.280 fixtures"
            }
            _ => "Pi 0.81.0 Linux x86_64 native RPC fixtures; ACP/gateway delivery deferred",
        };
        profile.capability.missing = &[];
        profile.ensure_launchable()?;
        Ok(profile)
    }
}

#[cfg(test)]
mod tests;
