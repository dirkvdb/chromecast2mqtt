use std::{path::PathBuf, time::Duration};

use anyhow::{ensure, Result};
use rumqttc::{
    AsyncClient, Event, Incoming, MqttOptions, Outgoing, QoS, Request, Subscribe, SubscribeFilter,
    Transport,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::tls;

pub const COMMAND_PROPERTIES: [&str; 5] = [
    "volume_level",
    "volume_muted",
    "player_position",
    "player_state",
    "rust_cast",
];

#[derive(Clone)]
pub struct MqttConfig {
    pub server: String,
    pub port: u16,
    pub tls: bool,
    pub ca_cert: Option<PathBuf>,
    pub insecure: bool,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
    pub client_id: String,
    pub user: String,
    pub password: String,
    pub base_topic: String,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            server: "127.0.0.1".into(),
            port: 1883,
            tls: false,
            ca_cert: None,
            insecure: false,
            client_cert: None,
            client_key: None,
            client_id: "chromecast2mqtt".into(),
            user: String::new(),
            password: String::new(),
            base_topic: "chromecast".into(),
        }
    }
}

impl MqttConfig {
    pub fn validate(&mut self) -> Result<()> {
        self.server = self.server.trim().to_owned();
        self.base_topic = self.base_topic.trim_matches('/').to_owned();
        ensure!(!self.server.is_empty(), "MQTT address must not be empty");
        ensure!(self.port != 0, "MQTT port must be nonzero");
        ensure!(
            self.password.is_empty() || !self.user.is_empty(),
            "MQTT password requires a username"
        );
        ensure!(
            !self.client_id.is_empty(),
            "MQTT client ID must not be empty"
        );
        ensure!(valid_topic(&self.base_topic), "invalid MQTT base topic");
        ensure!(
            !(self.ca_cert.is_some() && self.insecure),
            "CA certificate and insecure TLS conflict"
        );
        ensure!(
            self.client_cert.is_some() == self.client_key.is_some(),
            "client certificate and key must be supplied together"
        );
        Ok(())
    }

    pub fn options(&self) -> Result<MqttOptions> {
        let mut validated = self.clone();
        validated.validate()?;
        let mut options = MqttOptions::new(&validated.client_id, &validated.server, validated.port);
        options.set_keep_alive(Duration::from_secs(60));
        options.set_clean_session(true);
        if !validated.user.is_empty() {
            options.set_credentials(&validated.user, &validated.password);
        }
        if validated.tls
            || validated.ca_cert.is_some()
            || validated.insecure
            || validated.client_cert.is_some()
        {
            let config = tls::client_config(
                validated.ca_cert.as_deref(),
                validated.insecure,
                validated.client_cert.as_deref(),
                validated.client_key.as_deref(),
            )?;
            options.set_transport(Transport::tls_with_config(config.into()));
        }
        Ok(options)
    }
}

pub fn valid_topic(topic: &str) -> bool {
    !topic.is_empty()
        && !topic.split('/').any(str::is_empty)
        && !topic.bytes().any(|byte| matches!(byte, 0 | b'+' | b'#'))
        && topic.len() <= u16::MAX as usize
}

pub fn valid_device_name(name: &str) -> bool {
    valid_topic(name) && !name.contains('/')
}

/// Keep friendly names literal: spaces, case and Unicode are part of the contract.
pub fn command_target<'a>(base: &str, topic: &'a str) -> Option<(&'a str, &'a str)> {
    let relative = topic.strip_prefix(base)?.strip_prefix('/')?;
    let (name, property) = relative.split_once("/command/")?;
    (valid_device_name(name) && COMMAND_PROPERTIES.contains(&property)).then_some((name, property))
}

#[derive(Debug)]
pub enum MqttEvent {
    Message {
        topic: String,
        payload: Vec<u8>,
        retained: bool,
    },
}

pub struct MqttConnection {
    pub client: AsyncClient,
    pub events: mpsc::Receiver<MqttEvent>,
    pub connections: watch::Receiver<u64>,
    worker: JoinHandle<()>,
}

impl MqttConnection {
    pub fn new(config: &MqttConfig) -> Result<Self> {
        let (client, mut event_loop) = AsyncClient::new(config.options()?, 256);

        let base = config.base_topic.trim_matches('/').to_owned();
        let (sender, events) = mpsc::channel(256);
        let (connected, connections) = watch::channel(0);
        let worker = tokio::spawn(async move {
            let mut delay = Duration::from_secs(1);
            loop {
                let event = match event_loop.poll().await {
                    Ok(event) => event,
                    Err(error) => {
                        log::warn!(
                            "MQTT connection lost: {error}; retrying in {}s",
                            delay.as_secs()
                        );
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                        continue;
                    }
                };
                let notification = match event {
                    Event::Incoming(Incoming::ConnAck(_)) => {
                        delay = Duration::from_secs(1);
                        log::info!("MQTT connected");
                        // Prioritize subscriptions even when the request channel
                        // is full of state queued during a broker outage.
                        event_loop
                            .pending
                            .push_front(Request::Subscribe(Subscribe::new_many(
                                COMMAND_PROPERTIES.map(|property| {
                                    SubscribeFilter::new(
                                        format!("{base}/+/command/{property}"),
                                        QoS::AtMostOnce,
                                    )
                                }),
                            )));
                        // Connection transitions must not be lost behind a burst
                        // of commands: the bridge uses this to replay its snapshot.
                        connected.send_modify(|generation| *generation += 1);
                        None
                    }
                    Event::Incoming(Incoming::Publish(message)) => Some(MqttEvent::Message {
                        topic: message.topic,
                        payload: message.payload.to_vec(),
                        retained: message.retain,
                    }),
                    Event::Outgoing(Outgoing::Disconnect) => break,
                    _ => None,
                };
                if let Some(notification) = notification {
                    // Never stop polling the broker behind the publication queue.
                    // Bound command memory; an overload drops commands, not network progress.
                    if let Err(error) = sender.try_send(notification) {
                        if matches!(error, mpsc::error::TrySendError::Closed(_)) {
                            break;
                        }
                        log::warn!("MQTT input queue full; dropping event");
                    }
                }
            }
        });
        Ok(Self {
            client,
            events,
            connections,
            worker,
        })
    }

    pub async fn publish(
        &self,
        base: &str,
        name: &str,
        updates: Vec<(String, String)>,
    ) -> Result<()> {
        for (suffix, payload) in updates {
            self.client
                .publish(
                    format!("{base}/{name}/{suffix}"),
                    QoS::AtMostOnce,
                    true,
                    payload,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn close(&mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            let _ = self.client.disconnect().await;
            let _ = (&mut self.worker).await;
        })
        .await;
        self.worker.abort();
    }
}

impl Drop for MqttConnection {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_names_and_exact_command_routing() {
        assert_eq!(
            command_target(
                "chromecast",
                "chromecast/Living Room Ä/command/player_state"
            ),
            Some(("Living Room Ä", "player_state"))
        );
        for topic in [
            "chromecast/X/player_state",
            "chromecast/X/command/player_state/extra",
            "other/X/command/player_state",
            "chromecast/a/b/command/player_state",
        ] {
            assert!(command_target("chromecast", topic).is_none(), "{topic}");
        }
    }

    #[test]
    fn defaults_and_invalid_configuration() {
        let mut config = MqttConfig::default();
        config.validate().unwrap();
        assert!(matches!(
            config.options().unwrap().transport(),
            Transport::Tcp
        ));
        for topic in ["", "a//b", "a/+", "a/#", "a\0b"] {
            config.base_topic = topic.into();
            assert!(config.validate().is_err());
        }
        config = MqttConfig::default();
        config.ca_cert = Some("missing.pem".into());
        assert!(config.options().is_err());
    }
}
