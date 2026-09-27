//! Provider seam over the identity proofs: one [`ProofProvider`] value per
//! `(provider, host)` with the GitHub gist proof ([`super`], behaviour
//! unchanged) and the GitLab snippet proof ([`super::gitlab`]) behind the
//! same three operations — guest `create` / `delete`, host `verify`. The
//! service layer picks the variant from the wire `provider` / `host` and
//! never names a gist or a snippet itself.

use super::gitlab::{self, ProofSnippetView};
use super::{IdentityProofError, ProofGistView, Result};
use crate::error::Error;
use crate::github::GitHubSourceControl;
use crate::gitlab_auth::GitlabHost;
use crate::SourceControl;

/// The `host` every GitHub proof reports.
pub const GITHUB_HOST: &str = "github.com";

/// Where an identity proof lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProofProvider {
    /// A secret gist on GitHub (`api_base_url` `None` = api.github.com).
    Github { api_base_url: Option<String> },
    /// A public personal snippet on the GitLab instance `host`.
    Gitlab { host: GitlabHost },
}

/// A created proof: the provider-neutral `proof_id` (gist id / snippet id)
/// plus the identity of the token's owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedProof {
    pub proof_id: String,
    pub owner: ProofOwner,
}

/// The account a proof belongs to, as the provider reports it. GitHub's
/// gist create reports only the login; GitLab's snippet carries the author's
/// numeric id and avatar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofOwner {
    pub login: String,
    pub external_user_id: Option<String>,
    pub avatar_url: Option<String>,
}

/// What the host reads back to verify a proof: its owner, the proof's
/// `created_at` (RFC 3339, as the forge reports it) and the trimmed first
/// line of the proof file — `None` when the proof carries no such file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofView {
    pub owner: ProofOwner,
    pub created_at: String,
    pub proof_first_line: Option<String>,
}

impl From<ProofGistView> for ProofView {
    fn from(gist: ProofGistView) -> Self {
        Self {
            owner: ProofOwner {
                login: gist.owner_login,
                external_user_id: None,
                avatar_url: None,
            },
            created_at: gist.created_at,
            proof_first_line: gist.proof_first_line,
        }
    }
}

impl From<ProofSnippetView> for ProofView {
    fn from(snippet: ProofSnippetView) -> Self {
        Self {
            owner: ProofOwner {
                login: snippet.author.username,
                external_user_id: Some(snippet.author.id.to_string()),
                avatar_url: snippet.author.avatar_url,
            },
            created_at: snippet.created_at,
            proof_first_line: snippet.proof_first_line,
        }
    }
}

impl ProofProvider {
    /// The wire `provider` name.
    #[must_use]
    pub fn wire_name(&self) -> &'static str {
        match self {
            Self::Github { .. } => "github",
            Self::Gitlab { .. } => "gitlab",
        }
    }

    /// The canonical host the proof lives on (`github.com`, or the GitLab
    /// instance's `host[:port]`).
    #[must_use]
    pub fn host(&self) -> &str {
        match self {
            Self::Github { .. } => GITHUB_HOST,
            Self::Gitlab { host } => host.host(),
        }
    }

    /// Whether `proof_id` has the shape this provider's ids have (a gist id
    /// is alphanumeric, a snippet id numeric), so it is safe in a request
    /// path.
    #[must_use]
    pub fn valid_proof_id(&self, proof_id: &str) -> bool {
        match self {
            Self::Github { .. } => {
                !proof_id.is_empty() && proof_id.chars().all(|c| c.is_ascii_alphanumeric())
            }
            Self::Gitlab { .. } => gitlab::valid_snippet_id(proof_id),
        }
    }

    /// Guest half: publish `nonce` in a new proof with the guest's own
    /// `token` ([`super::create_proof_gist`] / [`gitlab::create_proof_snippet`]).
    ///
    /// # Errors
    ///
    /// As the provider's create.
    pub async fn create(&self, token: &str, nonce: &str, host_label: &str) -> Result<CreatedProof> {
        match self {
            Self::Github { api_base_url } => {
                let gist =
                    super::create_proof_gist(token, api_base_url.as_deref(), nonce, host_label)
                        .await?;
                Ok(CreatedProof {
                    proof_id: gist.gist_id,
                    owner: ProofOwner {
                        login: gist.login,
                        external_user_id: None,
                        avatar_url: None,
                    },
                })
            }
            Self::Gitlab { host } => {
                let snippet = gitlab::create_proof_snippet(host, token, nonce, host_label).await?;
                Ok(CreatedProof {
                    proof_id: snippet.snippet_id,
                    owner: ProofOwner {
                        login: snippet.author.username,
                        external_user_id: Some(snippet.author.id.to_string()),
                        avatar_url: snippet.author.avatar_url,
                    },
                })
            }
        }
    }

    /// Guest half: remove the proof `proof_id` with the guest's own `token`
    /// ([`super::delete_proof_gist`] / [`gitlab::delete_proof_snippet`]);
    /// idempotent on an already-deleted proof, refusing anything that is not
    /// an Intent proof.
    ///
    /// # Errors
    ///
    /// As the provider's delete.
    pub async fn delete(&self, token: &str, proof_id: &str) -> Result<()> {
        match self {
            Self::Github { api_base_url } => {
                super::delete_proof_gist(token, api_base_url.as_deref(), proof_id).await
            }
            Self::Gitlab { host } => gitlab::delete_proof_snippet(host, token, proof_id).await,
        }
    }

    /// Host half: read the proof `proof_id` back for verification.
    /// `own_token` is the host's **own** credential for this provider and
    /// host, if any: GitHub reads with it when present (anonymously
    /// otherwise — a secret gist is readable by id); GitLab reads
    /// anonymously first and falls back to it only when the instance refused
    /// or hid the snippet ([`gitlab::verify_proof_snippet`]).
    ///
    /// # Errors
    ///
    /// [`IdentityProofError::NotFound`] when no such proof exists;
    /// [`IdentityProofError::Unverifiable`] when a GitLab instance will not
    /// serve the snippet to this host; [`IdentityProofError::Unreachable`] on
    /// transport failure; `Other(Decode)` for a proof that names no owner.
    pub async fn verify(&self, proof_id: &str, own_token: Option<&str>) -> Result<ProofView> {
        match self {
            Self::Github { api_base_url } => {
                let sc = match own_token {
                    Some(token) => GitHubSourceControl::new(token, api_base_url.as_deref()),
                    None => GitHubSourceControl::anonymous(api_base_url.as_deref()),
                }?;
                match sc.get_proof_gist(proof_id).await {
                    Ok(view) => Ok(view.into()),
                    Err(Error::NotFound(_)) => Err(IdentityProofError::NotFound {
                        proof_id: proof_id.to_string(),
                    }),
                    Err(Error::Auth(msg)) => Err(IdentityProofError::Unauthorized(msg)),
                    Err(e) => Err(e.into()),
                }
            }
            Self::Gitlab { host } => gitlab::verify_proof_snippet(host, proof_id, own_token)
                .await
                .map(Into::into),
        }
    }
}
