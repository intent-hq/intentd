//! Certificate pinning: head trusts exactly the node certificate whose DER
//! SHA-256 it was configured with, and no CA.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use x509_parser::prelude::{FromDer, X509Certificate};

use intent_core::Result;

use crate::TlsCertificate;

/// rustls error text that marks a pin mismatch, so the dialer can report
/// `node-link-certificate-mismatch` instead of a generic TLS failure.
pub(super) const PIN_MISMATCH: &str = "node-link-certificate-mismatch";

/// The SHA-256 of a node certificate's DER body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CertificatePin([u8; 32]);

impl CertificatePin {
    /// Parse 64 hex digits, optionally colon-separated (the
    /// [`TlsCertificate::fingerprint256`] form), in either case.
    ///
    /// # Errors
    ///
    /// `node-link-config` for anything else.
    pub fn parse(text: &str) -> Result<Self> {
        let hex_digits: String = text.chars().filter(|c| *c != ':').collect();
        hex::decode(hex_digits)
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .map(Self)
            .ok_or_else(|| super::config("certificate pin must be a SHA-256 in hex"))
    }

    /// The pin of a certificate body.
    #[must_use]
    pub fn of_der(der: &[u8]) -> Self {
        Self(Sha256::digest(der).into())
    }
}

#[derive(Debug)]
struct PinVerifier {
    pin: CertificatePin,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let mismatch = || rustls::Error::General(PIN_MISMATCH.into());
        if CertificatePin::of_der(end_entity.as_ref()) != self.pin {
            return Err(mismatch());
        }
        let (_, parsed) = X509Certificate::from_der(end_entity.as_ref()).map_err(|_| mismatch())?;
        let now = i64::try_from(now.as_secs()).map_err(|_| mismatch())?;
        let validity = parsed.validity();
        if validity.not_before.timestamp() > now || validity.not_after.timestamp() < now {
            return Err(mismatch());
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A client config that trusts only `pin`.
pub(super) fn client_config(pin: CertificatePin) -> Result<rustls::ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(
        rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|_| super::config("TLS protocol versions"))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinVerifier { pin, provider }))
            .with_no_client_auth(),
    )
}

/// The node's server config for its own certificate.
pub(super) fn server_config(certificate: &TlsCertificate) -> Result<rustls::ServerConfig> {
    let invalid = || super::config("node TLS certificate or key is invalid");
    let certs = rustls_pemfile::certs(&mut certificate.cert.as_bytes())
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|_| invalid())?;
    if certs.is_empty() {
        return Err(invalid());
    }
    let key = rustls_pemfile::private_key(&mut certificate.key.as_bytes())
        .map_err(|_| invalid())?
        .ok_or_else(invalid)?;
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|_| invalid())?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|_| invalid())
}
