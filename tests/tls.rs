use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{ensure, Context, Result};
use chromecast2mqtt::tls;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::{
    crypto::ring,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
    server::WebPkiClientVerifier,
    ClientConfig, RootCertStore, ServerConfig,
};
use tempfile::{tempdir, TempDir};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const DEADLINE: Duration = Duration::from_secs(5);

struct Pki {
    directory: TempDir,
    ca_path: PathBuf,
    client_cert_path: PathBuf,
    client_key_path: PathBuf,
    ca_der: CertificateDer<'static>,
    server_der: CertificateDer<'static>,
    server_key: Vec<u8>,
}

impl Pki {
    fn new() -> Result<Self> {
        let directory = tempdir()?;
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "loopback test CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate()?;
        let ca = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);

        let mut server_params = CertificateParams::new(vec!["localhost".into()])?;
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate()?;
        let server = server_params.signed_by(&server_key, &issuer)?;

        let mut client_params = CertificateParams::new(Vec::<String>::new())?;
        client_params
            .distinguished_name
            .push(DnType::CommonName, "loopback test client");
        client_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate()?;
        let client = client_params.signed_by(&client_key, &issuer)?;

        let ca_path = directory.path().join("ca.pem");
        let client_cert_path = directory.path().join("client.pem");
        let client_key_path = directory.path().join("client.key");
        fs::write(&ca_path, ca.pem())?;
        fs::write(&client_cert_path, client.pem())?;
        fs::write(&client_key_path, client_key.serialize_pem())?;
        Ok(Self {
            directory,
            ca_path,
            client_cert_path,
            client_key_path,
            ca_der: ca.der().clone(),
            server_der: server.der().clone(),
            server_key: server_key.serialize_der(),
        })
    }

    fn server(&self, require_client_auth: bool) -> Result<ServerConfig> {
        server_config(
            self.server_der.clone(),
            self.server_key.clone(),
            require_client_auth.then(|| self.ca_der.clone()),
        )
    }

    fn client(&self, with_client_auth: bool) -> Result<ClientConfig> {
        tls::client_config(
            Some(&self.ca_path),
            false,
            with_client_auth.then_some(self.client_cert_path.as_path()),
            with_client_auth.then_some(self.client_key_path.as_path()),
        )
    }
}

fn server_config(
    cert: CertificateDer<'static>,
    key: Vec<u8>,
    client_ca: Option<CertificateDer<'static>>,
) -> Result<ServerConfig> {
    // rust_cast also links AWS-LC. Never depend on a process-global provider.
    let provider = Arc::new(ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?;
    let builder = if let Some(ca) = client_ca {
        let mut roots = RootCertStore::empty();
        roots.add(ca)?;
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider).build()?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    Ok(builder.with_single_cert(vec![cert], PrivatePkcs8KeyDer::from(key).into())?)
}

/// Complete both the handshake and an application-data exchange. In TLS 1.3,
/// connect() alone can succeed before the client sees a client-auth rejection.
async fn exchange(
    server_config: ServerConfig,
    client_config: ClientConfig,
    server_name: &str,
) -> Result<(Result<()>, Result<()>)> {
    timeout(DEADLINE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let connector = TlsConnector::from(Arc::new(client_config));
        let server_name = ServerName::try_from(server_name.to_owned())?;
        let server = async {
            let (socket, _) = listener.accept().await?;
            let mut stream = acceptor
                .accept(socket)
                .await
                .context("server TLS handshake")?;
            let mut request = [0; 4];
            stream.read_exact(&mut request).await?;
            ensure!(request == *b"ping", "unexpected TLS request");
            stream.write_all(b"pong").await?;
            stream.flush().await?;
            Ok(())
        };
        let client = async {
            let socket = TcpStream::connect(address).await?;
            let mut stream = connector
                .connect(server_name, socket)
                .await
                .context("client TLS handshake")?;
            stream.write_all(b"ping").await?;
            stream.flush().await?;
            let mut response = [0; 4];
            stream.read_exact(&mut response).await?;
            ensure!(response == *b"pong", "unexpected TLS response");
            Ok(())
        };
        Ok(tokio::join!(server, client))
    })
    .await
    .context("loopback TLS exchange exceeded five seconds")?
}

#[tokio::test]
async fn custom_ca_authenticates_localhost_and_exchanges_data() -> Result<()> {
    let pki = Pki::new()?;
    let (server, client) = exchange(pki.server(false)?, pki.client(false)?, "localhost").await?;
    server?;
    client?;
    Ok(())
}

#[tokio::test]
async fn verified_tls_rejects_the_wrong_hostname() -> Result<()> {
    let pki = Pki::new()?;
    // The TCP address remains loopback; this name is used only for TLS identity.
    let (server, client) = exchange(
        pki.server(false)?,
        pki.client(false)?,
        "wrong.example.invalid",
    )
    .await?;
    assert!(
        client.is_err(),
        "client accepted a certificate for the wrong name"
    );
    assert!(server.is_err(), "server unexpectedly completed an exchange");
    Ok(())
}

#[tokio::test]
async fn verified_tls_rejects_an_untrusted_ca() -> Result<()> {
    let server_pki = Pki::new()?;
    let unrelated_pki = Pki::new()?;
    let (server, client) = exchange(
        server_pki.server(false)?,
        unrelated_pki.client(false)?,
        "localhost",
    )
    .await?;
    assert!(
        client.is_err(),
        "client accepted an unrelated certificate authority"
    );
    assert!(server.is_err(), "server unexpectedly completed an exchange");
    Ok(())
}

#[tokio::test]
async fn required_mtls_accepts_a_configured_client_certificate_and_key() -> Result<()> {
    let pki = Pki::new()?;
    let (server, client) = exchange(pki.server(true)?, pki.client(true)?, "localhost").await?;
    server?;
    client?;
    Ok(())
}

#[tokio::test]
async fn required_mtls_rejects_a_client_without_a_certificate() -> Result<()> {
    let pki = Pki::new()?;
    let (server, client) = exchange(pki.server(true)?, pki.client(false)?, "localhost").await?;
    assert!(server.is_err(), "server accepted an unauthenticated client");
    assert!(
        client.is_err(),
        "client completed data exchange without its required certificate"
    );
    Ok(())
}

#[tokio::test]
async fn explicit_insecure_mode_accepts_a_self_signed_wrong_name_server() -> Result<()> {
    let mut params = CertificateParams::new(vec!["self-signed.example.invalid".into()])?;
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate()?;
    let cert = params.self_signed(&key)?;
    let server_config = server_config(cert.der().clone(), key.serialize_der(), None)?;
    let client_config = tls::client_config(None, true, None, None)?;
    let (server, client) = exchange(server_config, client_config, "localhost").await?;
    server?;
    client?;
    Ok(())
}

#[test]
fn pem_and_configuration_errors_fail_before_network_io() -> Result<()> {
    let pki = Pki::new()?;
    let missing = pki.directory.path().join("missing.pem");
    let empty = pki.directory.path().join("empty.pem");
    let malformed = pki.directory.path().join("malformed.pem");
    let invalid_der = pki.directory.path().join("invalid-der.pem");
    let invalid_key = pki.directory.path().join("invalid.key");
    fs::write(&empty, "")?;
    fs::write(
        &malformed,
        "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
    )?;
    fs::write(
        &invalid_der,
        "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
    )?;
    fs::write(&invalid_key, "this is not a PEM private key")?;

    for ca in [&missing, &empty, &malformed, &invalid_der] {
        assert!(tls::client_config(Some(ca), false, None, None).is_err());
    }
    for cert in [&missing, &empty, &malformed, &invalid_der] {
        assert!(tls::client_config(
            Some(&pki.ca_path),
            false,
            Some(cert),
            Some(&pki.client_key_path)
        )
        .is_err());
    }
    for key in [&missing, &invalid_key] {
        assert!(tls::client_config(
            Some(&pki.ca_path),
            false,
            Some(&pki.client_cert_path),
            Some(key)
        )
        .is_err());
    }
    assert!(
        tls::client_config(Some(&pki.ca_path), false, Some(&pki.client_cert_path), None).is_err()
    );
    assert!(
        tls::client_config(Some(&pki.ca_path), false, None, Some(&pki.client_key_path)).is_err()
    );
    assert!(tls::client_config(Some(&pki.ca_path), true, None, None).is_err());
    Ok(())
}
