use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use anyhow::{ensure, Context, Result};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

fn builder() -> rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier> {
    // rust_cast also enables AWS-LC; choose explicitly rather than relying on
    // rustls's process-global provider selection when both providers are built.
    ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("the Ring provider supports TLS 1.2 and TLS 1.3")
}

pub fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("reading {}", path.display()))?);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("parsing certificates in {}", path.display()))?;
    ensure!(
        !certificates.is_empty(),
        "no PEM certificates in {}",
        path.display()
    );
    Ok(certificates)
}

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut BufReader::new(
        File::open(path).with_context(|| format!("reading {}", path.display()))?,
    ))?
    .with_context(|| format!("no PEM private key in {}", path.display()))
}

pub fn client_config(
    ca: Option<&Path>,
    insecure: bool,
    client_cert: Option<&Path>,
    client_key: Option<&Path>,
) -> Result<ClientConfig> {
    ensure!(
        !(ca.is_some() && insecure),
        "MQTT CA and insecure TLS are mutually exclusive"
    );
    ensure!(
        client_cert.is_some() == client_key.is_some(),
        "MQTT client certificate and key must be supplied together"
    );
    let config = if insecure {
        log::warn!(
            "MQTT TLS server authentication is DISABLED; vulnerable to man-in-the-middle attacks"
        );
        builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier())
    } else {
        let mut roots = RootCertStore::empty();
        if let Some(path) = ca {
            for cert in certificates(path)? {
                roots
                    .add(cert)
                    .with_context(|| format!("invalid CA certificate in {}", path.display()))?;
            }
        } else {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                roots.add(cert).context("invalid system root certificate")?;
            }
            ensure!(
                !roots.is_empty(),
                "no system CA certificates available; supply --mqtt-ca-cert"
            );
        }
        builder().with_root_certificates(roots)
    };
    match (client_cert, client_key) {
        (Some(cert), Some(key)) => config
            .with_client_auth_cert(certificates(cert)?, private_key(key)?)
            .context("invalid MQTT client certificate/key pair"),
        _ => Ok(config.with_no_client_auth()),
    }
}

/// Cast devices present self-signed certificates. This is deliberately separate
/// from MQTT, where certificate and hostname validation are enabled by default.
pub fn insecure_config() -> ClientConfig {
    builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier())
        .with_no_client_auth()
}

fn verifier() -> Arc<NoCertificateVerification> {
    Arc::new(NoCertificateVerification(Arc::new(
        rustls::crypto::ring::default_provider(),
    )))
}

#[derive(Debug)]
struct NoCertificateVerification(Arc<CryptoProvider>);

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_tls_configuration_fails_before_network_io() {
        assert!(client_config(Some(Path::new("missing.pem")), true, None, None).is_err());
        assert!(client_config(None, false, Some(Path::new("missing.pem")), None).is_err());
        assert!(client_config(Some(Path::new("missing.pem")), false, None, None).is_err());
    }

    #[test]
    fn insecure_verifier_still_advertises_signature_verification() {
        assert!(!verifier().supported_verify_schemes().is_empty());
        let _ = insecure_config();
    }
}
