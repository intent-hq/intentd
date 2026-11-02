//! Where an exposed machine can be dialed: its direct hosts, its listener
//! port, its tailcat address, the certificate pin and the executor token.
//! `Debug` never shows the token.

use std::path::PathBuf;

use intent_core::machines::ExposedMachine;
use intent_core::{Error, Result};
use intent_transport::node_link::{CertificatePin, ExecutorToken, NodeEndpoint, TailcatRoute};

/// One way to reach the machine, with the label errors name it by.
#[derive(Debug, Clone)]
pub(crate) struct Route {
    /// `host:port` for a direct route, `tailcat:<address>` for tailcat.
    pub label: String,
    pub endpoint: NodeEndpoint,
}

/// An exposed machine's dial target.
#[derive(Clone)]
pub(crate) struct MachineTarget {
    pub id: String,
    pub name: String,
    pub hosts: Vec<String>,
    pub port: u16,
    pub tc_address: Option<String>,
    pub pin: CertificatePin,
    /// The pin as 64 lowercase hex digits.
    pub certificate_sha256: String,
    pub token: ExecutorToken,
    /// The sitter-shipped tailcat binary; without it the tailcat route is
    /// not tried.
    pub tailcat_bin: Option<PathBuf>,
}

impl std::fmt::Debug for MachineTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MachineTarget")
            .field("id", &self.id)
            .field("hosts", &self.hosts)
            .field("port", &self.port)
            .field("tc_address", &self.tc_address)
            .finish_non_exhaustive()
    }
}

fn invalid(detail: &str) -> Error {
    Error::InvalidParams(format!("remote-machine-invalid: {detail}"))
}

impl MachineTarget {
    /// The target of a stored machine with its `token`.
    ///
    /// # Errors
    ///
    /// When the stored fingerprint is not a SHA-256 or the token is
    /// malformed; the message never quotes the token.
    pub(crate) fn new(
        machine: &ExposedMachine,
        token: &str,
        tailcat_bin: Option<PathBuf>,
    ) -> Result<Self> {
        Self::from_parts(
            &machine.id,
            &machine.name,
            machine.hosts.clone(),
            machine.port,
            machine.tc_address.clone(),
            &machine.fingerprint,
            token,
            tailcat_bin,
        )
    }

    /// Validate the parts of a target.
    ///
    /// # Errors
    ///
    /// As [`MachineTarget::new`].
    #[expect(clippy::too_many_arguments, reason = "the add params, one value each")]
    pub(crate) fn from_parts(
        id: &str,
        name: &str,
        hosts: Vec<String>,
        port: u16,
        tc_address: Option<String>,
        fingerprint: &str,
        token: &str,
        tailcat_bin: Option<PathBuf>,
    ) -> Result<Self> {
        let pin = CertificatePin::parse(fingerprint)
            .map_err(|_| invalid("fingerprint must be a SHA-256 in hex"))?;
        let token = ExecutorToken::new(token.to_owned())
            .map_err(|_| invalid("token must be at least 32 printable ASCII bytes"))?;
        Ok(Self {
            id: id.to_owned(),
            name: name.to_owned(),
            hosts,
            port,
            tc_address: tc_address.filter(|address| !address.is_empty()),
            pin,
            certificate_sha256: fingerprint
                .chars()
                .filter(|c| *c != ':')
                .map(|c| c.to_ascii_lowercase())
                .collect(),
            token,
            tailcat_bin,
        })
    }

    /// The direct routes, one per host, in the stored order.
    pub(crate) fn direct_routes(&self) -> Vec<Route> {
        self.hosts
            .iter()
            .filter(|host| !host.is_empty())
            .map(|host| {
                let authority = if host.contains(':') && !host.starts_with('[') {
                    format!("[{host}]:{}", self.port)
                } else {
                    format!("{host}:{}", self.port)
                };
                Route {
                    label: authority.clone(),
                    endpoint: NodeEndpoint::direct(
                        format!("wss://{authority}"),
                        self.pin,
                        self.token.clone(),
                    ),
                }
            })
            .collect()
    }

    /// The tailcat route, when the machine has a tailcat address and this
    /// daemon has the tailcat binary.
    pub(crate) fn tailcat_route(&self) -> Option<Route> {
        let address = self.tc_address.clone()?;
        let bin = self.tailcat_bin.clone()?;
        Some(Route {
            label: format!("tailcat:{address}"),
            endpoint: NodeEndpoint::via_tailcat(
                TailcatRoute { bin, address },
                self.port,
                self.pin,
                self.token.clone(),
            ),
        })
    }
}
