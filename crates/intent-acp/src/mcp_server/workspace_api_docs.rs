//! Harness-versioned rendering of the `workspace_api` text surfaces: the
//! description `tools/list` serves and the reference the system prompt
//! carries. A session's stamped harness version picks one
//! [`WorkspaceApiDocs`]; every version before 3.10 uses
//! [`WORKSPACE_API_DOCS_V1`], so their bytes never change.

use intent_core::settings_file::AgentFeaturesSettings;

use super::tools::{self, SpecialistModelOptions, WORKSPACE_API_SYSTEM_PROMPT_HEADING};

/// One bridge's captured spawn state, the input every docs version renders
/// from.
// Independent spawn-time captures, as on `WorkspaceMcpServer`.
#[expect(clippy::struct_excessive_bools)]
pub struct WorkspaceApiDocsInputs<'a> {
    pub is_chief: bool,
    /// The bridge's effective `[agentFeatures]` (sub-agent gates folded in).
    pub features: &'a AgentFeaturesSettings,
    pub model_options: &'a [SpecialistModelOptions],
    pub is_sub_agent: bool,
    pub plain_folder: bool,
    pub remote_placement_help: Option<&'a str>,
    /// The provider truncates long tool descriptions.
    pub truncating_provider: bool,
}

/// One harness version's `workspace_api` text surfaces.
pub trait WorkspaceApiDocs: Send + Sync {
    /// The `workspace_api` description `tools/list` serves.
    fn tool_description(&self, inputs: &WorkspaceApiDocsInputs<'_>) -> String;
    /// The reference the system prompt carries, or `None` when the tool
    /// description carries everything.
    fn prompt_reference(&self, inputs: &WorkspaceApiDocsInputs<'_>) -> Option<String>;
    /// The system-prompt section around a [`Self::prompt_reference`].
    fn prompt_section(&self, reference: &str) -> String;
}

/// The surfaces of harness 1.0 through 3.9.
pub struct WorkspaceApiDocsV1;

/// Harness 1.0 through 3.9.
pub static WORKSPACE_API_DOCS_V1: WorkspaceApiDocsV1 = WorkspaceApiDocsV1;

impl WorkspaceApiDocsV1 {
    /// Shape a rendering for the bridge: placement text (never in a plain
    /// folder, whose remote agents could not start from a commit) and the
    /// plain-folder cut.
    fn shape(inputs: &WorkspaceApiDocsInputs<'_>, description: String) -> String {
        let description = match inputs
            .remote_placement_help
            .filter(|_| !inputs.plain_folder)
        {
            Some(help) => tools::inject_remote_placement_help(&description, help),
            None => description,
        };
        if inputs.plain_folder {
            tools::plain_folder_description(&description)
        } else {
            description
        }
    }

    /// The full description, as a non-truncating provider reads it.
    pub(crate) fn full(inputs: &WorkspaceApiDocsInputs<'_>) -> String {
        let description = tools::workspace_api_description_with_model_options(
            inputs.is_chief,
            inputs.features,
            inputs.model_options,
            inputs.is_sub_agent,
        );
        Self::shape(inputs, description.into_owned())
    }

    /// The condensed reference: every method's signature and first sentence.
    pub(crate) fn condensed(inputs: &WorkspaceApiDocsInputs<'_>) -> String {
        Self::shape(
            inputs,
            tools::condensed_workspace_api_description(
                inputs.is_chief,
                inputs.features,
                inputs.model_options,
            ),
        )
    }
}

impl WorkspaceApiDocs for WorkspaceApiDocsV1 {
    fn tool_description(&self, inputs: &WorkspaceApiDocsInputs<'_>) -> String {
        if inputs.truncating_provider {
            let compact =
                tools::compact_workspace_api_description(inputs.is_chief, inputs.features);
            if inputs.plain_folder {
                tools::plain_folder_description(&compact)
            } else {
                compact
            }
        } else {
            Self::full(inputs)
        }
    }

    fn prompt_reference(&self, inputs: &WorkspaceApiDocsInputs<'_>) -> Option<String> {
        inputs.truncating_provider.then(|| Self::condensed(inputs))
    }

    fn prompt_section(&self, reference: &str) -> String {
        format!(
            "{WORKSPACE_API_SYSTEM_PROMPT_HEADING}\n\nThe `workspace_api` (ws.*) API reference: every method's signature with \
             a one-line summary. The MCP tool description you see is a compact index of the \
             same surface; call ws.help(\"<namespace>\") for one namespace's full docs.\n\n{reference}"
        )
    }
}

/// Harness 3.10: the compact reference lands here. Until then it renders
/// exactly what [`WORKSPACE_API_DOCS_V1`] renders.
pub struct WorkspaceApiDocsV3_10;

/// Harness 3.10.
pub static WORKSPACE_API_DOCS_V3_10: WorkspaceApiDocsV3_10 = WorkspaceApiDocsV3_10;

impl WorkspaceApiDocs for WorkspaceApiDocsV3_10 {
    fn tool_description(&self, inputs: &WorkspaceApiDocsInputs<'_>) -> String {
        WORKSPACE_API_DOCS_V1.tool_description(inputs)
    }

    fn prompt_reference(&self, inputs: &WorkspaceApiDocsInputs<'_>) -> Option<String> {
        WORKSPACE_API_DOCS_V1.prompt_reference(inputs)
    }

    fn prompt_section(&self, reference: &str) -> String {
        WORKSPACE_API_DOCS_V1.prompt_section(reference)
    }
}
