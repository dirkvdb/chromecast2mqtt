use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    time::Duration,
};

use anyhow::{ensure, Result};
use rumqttc::QoS;
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

use crate::{
    cast, command,
    discovery::{Device, Discovery, DiscoveryEvent},
    mqtt::{command_target, valid_device_name, MqttConfig, MqttConnection, MqttEvent},
    status::StatusCache,
};

pub struct BridgeConfig {
    pub mqtt: MqttConfig,
    pub discovery: bool,
    pub devices: Vec<Device>,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            mqtt: MqttConfig::default(),
            discovery: true,
            devices: Vec::new(),
        }
    }
}

struct DeviceRecord {
    info: Device,
    generation: u64,
    present: bool,
    connected: bool,
    commands: mpsc::Sender<command::Command>,
    shutdown: watch::Sender<bool>,
    state: StatusCache,
}

impl Drop for DeviceRecord {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

pub struct ChromecastMqttBridge {
    config: BridgeConfig,
}

impl ChromecastMqttBridge {
    pub fn new(mut config: BridgeConfig) -> Result<Self> {
        config.mqtt.validate()?;
        ensure!(
            config.discovery || !config.devices.is_empty(),
            "enable discovery or configure at least one --device"
        );
        for device in &config.devices {
            ensure!(
                valid_device_name(&device.name),
                "device names must be a valid single MQTT topic level"
            );
        }
        Ok(Self { config })
    }

    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<()> {
        let base = self.config.mqtt.base_topic.clone();
        let mut mqtt = MqttConnection::new(&self.config.mqtt)?;
        let discovery = if self.config.discovery {
            Some(Discovery::new()?)
        } else {
            None
        };
        let (event_sender, mut events) = mpsc::channel(256);
        let mut workers = JoinSet::new();
        let mut devices: HashMap<String, DeviceRecord> = HashMap::new();
        let mut publications = Publications::default();
        let mut generation = 0;
        for device in self.config.devices {
            register(
                device,
                &mut devices,
                &mut generation,
                &event_sender,
                &mut workers,
                &mut publications,
                &base,
            );
        }
        let mut flush = tokio::time::interval(Duration::from_millis(100));
        let mut retry = tokio::time::interval(Duration::from_secs(5));
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                () = &mut shutdown => break Ok(()),
                event = async {
                    match &discovery {
                        Some(discovery) => discovery.next().await,
                        None => std::future::pending().await,
                    }
                } => match event {
                    Ok(DiscoveryEvent::Resolved(device)) => {
                        register(device, &mut devices, &mut generation, &event_sender, &mut workers, &mut publications, &base);
                    }
                    Ok(DiscoveryEvent::Removed(service)) => {
                        // mDNS can briefly disappear while a connection is healthy.
                        // Keep connected actors; reconnecting actors are retired on removal.
                        let names: Vec<_> = devices.iter_mut().filter_map(|(name, record)| {
                            if record.info.service != service { return None; }
                            record.present = false;
                            (!record.connected).then(|| name.clone())
                        }).collect();
                        for name in names {
                            devices.remove(&name);
                            publications.insert(&base, &name, vec![("connection_status".into(), "DISCONNECTED".into())]);
                        }
                    }
                    Err(error) => break Err(error),
                },
                Some((generation, event)) = events.recv() => {
                    let name = match &event {
                        cast::Event::Update { device, .. } | cast::Event::Connection { device, .. } => device.clone(),
                    };
                    let mut retire = false;
                    if let Some(record) = devices.get_mut(&name).filter(|record| record.generation == generation) {
                        let updates = match event {
                            cast::Event::Update { namespace, payload, .. } => record.state.apply(&namespace, &payload),
                            cast::Event::Connection { status, .. } => {
                                match status {
                                    "CONNECTED" => record.connected = true,
                                    "CONNECTING" | "DISCONNECTED" => record.connected = false,
                                    _ => {},
                                }
                                retire = !record.present && !record.connected;
                                record.state.set("connection_status", status)
                            },
                        };
                        publications.insert(&base, &record.info.name, updates);
                    }
                    if retire { devices.remove(&name); }
                },
                changed = mqtt.connections.changed() => {
                    if changed.is_err() { break Err(anyhow::anyhow!("MQTT worker stopped")); }
                    publications.replay();
                },
                event = mqtt.events.recv() => match event {
                    Some(MqttEvent::Message { topic, payload, retained }) => {
                        // Deliberately preserve Python's retained-command behavior.
                        if retained { log::debug!("Processing retained command on {topic}"); }
                        if let Some((name, property)) = command_target(&base, &topic) {
                            match command::parse(property, &payload) {
                                Ok(Some(command)) => {
                                    if let Some(record) = devices.get(name) {
                                        if record.commands.try_send(command).is_err() {
                                            log::warn!("Command queue unavailable for {name}; command dropped");
                                        }
                                    } else {
                                        log::warn!("Command for undiscovered device {name}; configure --device if mDNS is unavailable");
                                    }
                                }
                                Ok(None) => {},
                                Err(error) => log::warn!("Invalid command on {topic}: {error:#}"),
                            }
                        }
                    }
                    None => break Err(anyhow::anyhow!("MQTT worker stopped")),
                },
                _ = flush.tick() => publications.flush(&mqtt.client),
                _ = retry.tick() => {
                    let stopped: Vec<_> = devices.values().filter(|record| record.commands.is_closed())
                        .map(|record| record.info.clone()).collect();
                    for device in stopped {
                        log::warn!("Restarting stopped Cast worker for {}", device.name);
                        register(device, &mut devices, &mut generation, &event_sender, &mut workers, &mut publications, &base);
                    }
                },
                Some(result) = workers.join_next(), if !workers.is_empty() => {
                    if let Err(error) = result { log::warn!("Cast event relay stopped: {error}"); }
                }
            }
        };
        log::info!("Stopping bridge without stopping Cast playback");
        for (name, record) in &devices {
            let _ = record.shutdown.send(true);
            publications.insert(
                &base,
                name,
                vec![("connection_status".into(), "DISCONNECTED".into())],
            );
        }
        devices.clear();
        drop(discovery);
        // Drain relays during retirement so backpressure cannot prevent shutdown.
        let _ = tokio::time::timeout(Duration::from_secs(6), async {
            while !workers.is_empty() {
                tokio::select! {
                    _ = workers.join_next() => {},
                    _ = events.recv() => {},
                }
            }
        })
        .await;
        workers.shutdown().await;
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while !publications.0.is_empty() {
                publications.flush(&mqtt.client);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        mqtt.close().await;
        result
    }
}

#[allow(clippy::too_many_arguments)]
fn register(
    device: Device,
    devices: &mut HashMap<String, DeviceRecord>,
    generation: &mut u64,
    events: &mpsc::Sender<(u64, cast::Event)>,
    workers: &mut JoinSet<()>,
    publications: &mut Publications,
    base: &str,
) {
    if !valid_device_name(&device.name) {
        log::warn!("Skipping Cast device whose name contains invalid MQTT topic characters");
        return;
    }
    if let Some(existing) = devices.get_mut(&device.name) {
        if existing.info.service != device.service {
            log::warn!(
                "Duplicate Cast friendly name {}; keeping the existing device",
                device.name
            );
            return;
        }
        existing.present = true;
        if existing.info == device && !existing.commands.is_closed() {
            return;
        }
    }
    let renamed: Vec<_> = devices
        .iter()
        .filter(|(name, record)| record.info.service == device.service && **name != device.name)
        .map(|(name, _)| name.clone())
        .collect();
    for name in renamed {
        devices.remove(&name);
        publications.insert(
            base,
            &name,
            vec![("connection_status".into(), "DISCONNECTED".into())],
        );
    }
    *generation += 1;
    let current_generation = *generation;
    let (shutdown, receiver) = watch::channel(false);
    let (cast_events, mut cast_receiver) = mpsc::channel(64);
    let commands = cast::spawn(device.clone(), cast_events, receiver);
    let sender = events.clone();
    workers.spawn(async move {
        while let Some(event) = cast_receiver.recv().await {
            if sender.send((current_generation, event)).await.is_err() {
                break;
            }
        }
    });
    let mut state = StatusCache::new();
    state.set("friendly_name", &device.name);
    state.set("model_name", &device.model);
    state.set(
        "address",
        format!("{}:{}", device.address.ip(), device.address.port()),
    );
    state.set("cast_type", &device.cast_type);
    state.set("connection_status", "WAITING");
    publications.insert(base, &device.name, state.snapshot());
    log::info!("Discovered {} at {}", device.name, device.address);
    devices.insert(
        device.name.clone(),
        DeviceRecord {
            info: device,
            generation: current_generation,
            present: true,
            connected: false,
            commands,
            shutdown,
            state,
        },
    );
}

/// Coalesce unsent values instead of blocking discovery/shutdown behind an
/// offline broker. A reconnect also schedules a complete known-state snapshot.
#[derive(Default)]
struct Publications(BTreeMap<String, String>, BTreeMap<String, String>);

impl Publications {
    fn insert(&mut self, base: &str, name: &str, updates: Vec<(String, String)>) {
        for (suffix, value) in updates {
            let topic = format!("{base}/{name}/{suffix}");
            self.1.insert(topic.clone(), value.clone());
            self.0.insert(topic, value);
        }
    }

    fn replay(&mut self) {
        // Include retired-device tombstones: QoS 0 can lose a DISCONNECTED
        // publication accepted just before the broker connection failed.
        self.0.extend(self.1.clone());
    }

    fn flush(&mut self, client: &rumqttc::AsyncClient) {
        for _ in 0..64 {
            let Some((topic, value)) = self.0.first_key_value() else {
                break;
            };
            if client
                .try_publish(topic, QoS::AtMostOnce, true, value.as_bytes())
                .is_err()
            {
                break;
            }
            self.0.pop_first();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_publications_keep_only_the_latest_value_per_topic() {
        let mut queue = Publications::default();
        queue.insert(
            "chromecast",
            "Living Room",
            vec![("volume_level".into(), "20".into())],
        );
        queue.insert(
            "chromecast",
            "Living Room",
            vec![
                ("volume_level".into(), "30".into()),
                ("media/title".into(), String::new()),
            ],
        );
        assert_eq!(queue.0.len(), 2);
        assert_eq!(queue.0["chromecast/Living Room/volume_level"], "30");
        assert_eq!(queue.0["chromecast/Living Room/media/title"], "");
    }

    #[test]
    fn retired_device_tombstones_survive_acceptance_into_the_mqtt_queue() {
        let mut queue = Publications::default();
        queue.insert(
            "chromecast",
            "Old Name",
            vec![("connection_status".into(), "CONNECTED".into())],
        );
        queue.0.clear();
        queue.insert(
            "chromecast",
            "Old Name",
            vec![("connection_status".into(), "DISCONNECTED".into())],
        );
        queue.0.clear();
        queue.replay();
        assert_eq!(
            queue.0["chromecast/Old Name/connection_status"],
            "DISCONNECTED"
        );
    }

    #[test]
    fn configuration_requires_a_device_source() {
        let config = BridgeConfig {
            discovery: false,
            ..BridgeConfig::default()
        };
        assert!(ChromecastMqttBridge::new(config).is_err());
    }
}
