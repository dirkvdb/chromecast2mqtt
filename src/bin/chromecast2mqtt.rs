use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use anyhow::{bail, ensure, Context, Result};
use chromecast2mqtt::{
    bridge::{BridgeConfig, ChromecastMqttBridge},
    discovery::Device,
    mqtt::MqttConfig,
};
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    /// Optional legacy Python config.ini; automatically read ./config.ini when present.
    #[arg(long, env = "C2M_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, env = "C2M_MQTT_ADDRESS")]
    mqtt_addr: Option<String>,
    #[arg(long, env = "C2M_MQTT_PORT")]
    mqtt_port: Option<u16>,
    /// Enable verified MQTT TLS using system roots (port remains 1883 unless set).
    #[arg(long, env = "C2M_MQTT_TLS")]
    mqtt_tls: bool,
    /// Enable MQTT TLS and trust this PEM CA bundle instead of system roots.
    #[arg(long, env = "C2M_MQTT_CA_CERT", conflicts_with = "mqtt_insecure")]
    mqtt_ca_cert: Option<PathBuf>,
    /// Enable encrypted MQTT without authenticating the server. Prefer a CA certificate.
    #[arg(long, env = "C2M_MQTT_INSECURE")]
    mqtt_insecure: bool,
    #[arg(long, env = "C2M_MQTT_CLIENT_CERT", requires = "mqtt_client_key")]
    mqtt_client_cert: Option<PathBuf>,
    #[arg(long, env = "C2M_MQTT_CLIENT_KEY", requires = "mqtt_client_cert")]
    mqtt_client_key: Option<PathBuf>,
    #[arg(long, env = "C2M_MQTT_USER")]
    mqtt_user: Option<String>,
    /// Override SecretSpec. Prefer C2M_MQTT_PASS to avoid process-list exposure.
    #[arg(long, env = "C2M_MQTT_PASS", hide_env_values = true)]
    mqtt_pass: Option<String>,
    #[arg(long, env = "C2M_CLIENT_ID", default_value = "chromecast2mqtt")]
    mqtt_client_id: String,
    #[arg(long, env = "C2M_MQTT_BASE_TOPIC", default_value = "chromecast")]
    mqtt_base_topic: String,
    #[arg(long, env = "C2M_NO_DISCOVERY")]
    no_discovery: bool,
    /// Static device as literal friendly name=IP:port; repeat or separate with commas.
    #[arg(long = "device", env = "C2M_DEVICES", value_delimiter = ',', value_parser = parse_device)]
    devices: Vec<Device>,
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

fn parse_device(text: &str) -> std::result::Result<Device, String> {
    let (name, address) = text.split_once('=').ok_or("device must be NAME=IP:PORT")?;
    if !chromecast2mqtt::mqtt::valid_device_name(name) {
        return Err("friendly name must be a valid single MQTT topic level".into());
    }
    let address: SocketAddr = address
        .parse()
        .map_err(|_| "device address must be IP:PORT (IPv6: [IP]:PORT)")?;
    if address.port() == 0 {
        return Err("device port must be nonzero".into());
    }
    Ok(Device {
        service: format!("static:{name}"),
        name: name.into(),
        model: "Unknown".into(),
        address,
        cast_type: "cast".into(),
    })
}

impl Cli {
    fn bridge_config(self) -> Result<BridgeConfig> {
        self.bridge_config_with(chromecast2mqtt::secrets::mqtt_password)
    }

    fn bridge_config_with(
        self,
        resolve_password: impl FnOnce() -> Result<String>,
    ) -> Result<BridgeConfig> {
        let config_path = self.config.or_else(|| {
            let path = PathBuf::from("config.ini");
            path.is_file().then_some(path)
        });
        let mut mqtt = if let Some(path) = config_path {
            legacy_config(
                &std::fs::read_to_string(&path)
                    .with_context(|| format!("reading {}", path.display()))?,
            )?
        } else {
            MqttConfig::default()
        };
        if let Some(server) = self.mqtt_addr {
            mqtt.server = server;
        }
        if let Some(port) = self.mqtt_port {
            mqtt.port = port;
        }
        if let Some(user) = self.mqtt_user {
            mqtt.user = user;
        }
        let password_supplied = self.mqtt_pass.is_some() || !mqtt.password.is_empty();
        if let Some(password) = self.mqtt_pass {
            mqtt.password = password;
        }
        if !password_supplied && !mqtt.user.is_empty() {
            mqtt.password = resolve_password().context("configuring MQTT authentication")?;
        }
        mqtt.tls = self.mqtt_tls;
        mqtt.ca_cert = self.mqtt_ca_cert;
        mqtt.insecure = self.mqtt_insecure;
        mqtt.client_cert = self.mqtt_client_cert;
        mqtt.client_key = self.mqtt_client_key;
        mqtt.client_id = self.mqtt_client_id;
        mqtt.base_topic = self.mqtt_base_topic;
        mqtt.validate()?;
        Ok(BridgeConfig {
            mqtt,
            discovery: !self.no_discovery,
            devices: self.devices,
        })
    }
}

fn legacy_config(text: &str) -> Result<MqttConfig> {
    let mut mqtt = MqttConfig::default();
    let mut section = String::new();
    let mut values = HashMap::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line[1..line.len() - 1].trim().to_ascii_lowercase();
            continue;
        }
        if section != "mqtt" {
            continue;
        }
        let delimiter = line
            .find(['=', ':'])
            .with_context(|| format!("invalid config.ini entry at line {}", index + 1))?;
        let (key, value) = (&line[..delimiter], &line[delimiter + 1..]);
        let key = key.trim().to_ascii_lowercase();
        ensure!(
            !values.contains_key(&key),
            "duplicate config.ini key at line {}",
            index + 1
        );
        values.insert(key, value.trim().to_owned());
    }
    if let Some(address) = values.get("broker_address") {
        mqtt.server.clone_from(address);
    }
    if let Some(port) = values.get("broker_port") {
        mqtt.port = port.parse().context("invalid config.ini broker_port")?;
    }
    let use_auth = match values
        .get("use_auth")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        None | Some("0" | "false" | "no" | "off") => false,
        Some("1" | "true" | "yes" | "on") => true,
        _ => bail!("config.ini use_auth must be a boolean"),
    };
    if use_auth {
        mqtt.user = values.get("username").cloned().unwrap_or_default();
        mqtt.password = values.get("password").cloned().unwrap_or_default();
        ensure!(
            !mqtt.user.is_empty(),
            "config.ini use_auth requires a nonempty username"
        );
    }
    mqtt.validate()?;
    Ok(mqtt)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => tokio::select! {
                result = tokio::signal::ctrl_c() => { if let Err(error) = result { log::error!("Ctrl+C handler failed: {error}"); } },
                _ = terminate.recv() => {},
            },
            Err(error) => {
                log::error!("SIGTERM handler failed: {error}");
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_level(cli.verbosity.log_level_filter())
        .init();
    // Providers can perform blocking keyring/filesystem/network I/O. Keep that
    // work off the async runtime's worker threads and never mutate global env.
    let config = tokio::task::spawn_blocking(move || cli.bridge_config()).await??;
    log::info!(
        "Starting bridge for MQTT {}:{}; base topic {}",
        config.mqtt.server,
        config.mqtt.port,
        config.mqtt.base_topic
    );
    log::info!(
        "Cast device TLS accepts self-signed certificates; MQTT has an independent TLS policy"
    );
    ChromecastMqttBridge::new(config)?
        .run(shutdown_signal())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_ini_and_auth_policy() {
        let config = legacy_config("[mqtt]\nbroker_address = broker.local\nbroker_port = 8883\nuse_auth = 1\nusername = user\npassword = p%ss=word").unwrap();
        assert_eq!(config.server, "broker.local");
        assert_eq!(config.port, 8883);
        assert_eq!(config.password, "p%ss=word");
        assert!(!config.tls);
        assert_eq!(
            legacy_config("[mqtt]\nuse_auth: true\nusername: user\npassword: abc=def")
                .unwrap()
                .password,
            "abc=def"
        );
        assert!(legacy_config("[mqtt]\nuse_auth=true").is_err());
        assert!(legacy_config("[mqtt]\nuse_auth = maybe").is_err());
        assert!(legacy_config("[mqtt]\nbroker_port = 0").is_err());
        assert!(legacy_config("[mqtt]\nbroker_address =\n").is_err());
        assert!(legacy_config("[mqtt]\nusername=a\nusername=b").is_err());
        assert!(legacy_config("[mqtt]\nusername=a\npassword=b")
            .unwrap()
            .user
            .is_empty());
    }

    #[test]
    fn cli_tls_modes_and_pairing() {
        assert!(Cli::try_parse_from([
            "chromecast2mqtt",
            "--mqtt-ca-cert",
            "ca.pem",
            "--mqtt-insecure"
        ])
        .is_err());
        assert!(
            Cli::try_parse_from(["chromecast2mqtt", "--mqtt-client-cert", "client.pem"]).is_err()
        );
        let config = Cli::try_parse_from(["chromecast2mqtt", "--mqtt-tls", "--mqtt-port", "8883"])
            .unwrap()
            .bridge_config_with(|| Ok("fixture-password".into()))
            .unwrap();
        assert!(config.mqtt.tls);
        assert_eq!(config.mqtt.port, 8883);
    }

    #[test]
    fn static_names_are_literal_and_ipv6_is_supported() {
        let device = parse_device("Living Room Ä=[2001:db8::1]:8009").unwrap();
        assert_eq!(device.name, "Living Room Ä");
        assert_eq!(device.address.port(), 8009);
        for text in [
            "invalid",
            "a/b=192.0.2.1:8009",
            "a=host.local:8009",
            "a=192.0.2.1:0",
        ] {
            assert!(parse_device(text).is_err());
        }
    }
}
