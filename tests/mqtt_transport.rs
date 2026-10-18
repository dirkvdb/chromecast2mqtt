use std::{collections::BTreeSet, future::Future, net::SocketAddr, time::Duration};

use anyhow::{bail, ensure, Context, Result};
use chromecast2mqtt::{
    bridge::{BridgeConfig, ChromecastMqttBridge},
    discovery::Device,
    mqtt::{MqttConfig, MqttConnection, MqttEvent},
};
use rumqttc::QoS;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(5);
const MAX_PACKET_SIZE: usize = 16 * 1024;
const PROPERTIES: [&str; 5] = [
    "volume_level",
    "volume_muted",
    "player_position",
    "player_state",
    "rust_cast",
];

async fn bounded(test: impl Future<Output = Result<()>>) {
    timeout(DEADLINE, test)
        .await
        .expect("loopback MQTT test exceeded five seconds")
        .unwrap();
}

struct Packet {
    header: u8,
    body: Vec<u8>,
}

async fn read_packet(socket: &mut TcpStream) -> Result<Packet> {
    let header = socket.read_u8().await.context("reading MQTT header")?;
    let mut length = 0;
    for digit in 0..4 {
        let byte = socket.read_u8().await.context("reading remaining length")?;
        length += usize::from(byte & 0x7f) << (digit * 7);
        ensure!(length <= MAX_PACKET_SIZE, "fixture MQTT packet too large");
        if byte & 0x80 == 0 {
            let mut body = vec![0; length];
            socket
                .read_exact(&mut body)
                .await
                .context("reading MQTT body")?;
            return Ok(Packet { header, body });
        }
    }
    bail!("malformed MQTT remaining length")
}

async fn write_packet(socket: &mut TcpStream, header: u8, body: &[u8]) -> Result<()> {
    ensure!(
        body.len() <= MAX_PACKET_SIZE,
        "fixture MQTT packet too large"
    );
    let mut frame = vec![header];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining != 0 {
            byte |= 0x80;
        }
        frame.push(byte);
        if remaining == 0 {
            break;
        }
    }
    frame.extend_from_slice(body);
    socket
        .write_all(&frame)
        .await
        .context("writing MQTT packet")
}

fn read_string<'a>(bytes: &mut &'a [u8]) -> Result<&'a str> {
    ensure!(bytes.len() >= 2, "missing MQTT string length");
    let length = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    *bytes = &bytes[2..];
    ensure!(bytes.len() >= length, "truncated MQTT string");
    let text = std::str::from_utf8(&bytes[..length])?;
    *bytes = &bytes[length..];
    Ok(text)
}

fn push_string(bytes: &mut Vec<u8>, text: &str) -> Result<()> {
    bytes.extend_from_slice(&u16::try_from(text.len())?.to_be_bytes());
    bytes.extend_from_slice(text.as_bytes());
    Ok(())
}

async fn connect(socket: &mut TcpStream, client_id: &str) -> Result<()> {
    let packet = read_packet(socket).await?;
    ensure!(packet.header == 0x10, "expected CONNECT");
    let mut body = packet.body.as_slice();
    ensure!(
        read_string(&mut body)? == "MQTT",
        "wrong MQTT protocol name"
    );
    ensure!(body.len() >= 4, "truncated CONNECT");
    ensure!(body[0] == 4, "expected MQTT 3.1.1");
    ensure!(
        body[1] == 0x02,
        "expected clean session with no credentials or will"
    );
    ensure!(
        u16::from_be_bytes([body[2], body[3]]) == 60,
        "wrong keepalive"
    );
    body = &body[4..];
    ensure!(read_string(&mut body)? == client_id, "wrong client ID");
    ensure!(body.is_empty(), "unexpected CONNECT payload");
    write_packet(socket, 0x20, &[0, 0]).await
}

fn expected_subscriptions(base: &str) -> BTreeSet<String> {
    PROPERTIES
        .iter()
        .map(|property| format!("{base}/+/command/{property}"))
        .collect()
}

async fn acknowledge_subscribe(socket: &mut TcpStream, packet: Packet) -> Result<Vec<String>> {
    ensure!(
        packet.header == 0x82,
        "expected QoS-1 SUBSCRIBE fixed header"
    );
    ensure!(packet.body.len() >= 2, "missing SUBSCRIBE packet ID");
    let mut body = packet.body.as_slice();
    let id = u16::from_be_bytes([body[0], body[1]]);
    ensure!(id != 0, "SUBSCRIBE packet ID must be nonzero");
    body = &body[2..];
    let mut filters = Vec::new();
    while !body.is_empty() {
        filters.push(read_string(&mut body)?.to_owned());
        ensure!(
            body.first() == Some(&0),
            "command subscription must request QoS 0"
        );
        body = &body[1..];
    }
    ensure!(!filters.is_empty(), "empty SUBSCRIBE");
    let mut response = id.to_be_bytes().to_vec();
    response.resize(2 + filters.len(), 0);
    write_packet(socket, 0x90, &response).await?;
    Ok(filters)
}

async fn subscriptions(socket: &mut TcpStream, base: &str) -> Result<()> {
    let expected = expected_subscriptions(base);
    let mut actual = BTreeSet::new();
    while actual.len() < expected.len() {
        let packet = read_packet(socket).await?;
        for filter in acknowledge_subscribe(socket, packet).await? {
            ensure!(
                expected.contains(&filter),
                "unexpected subscription {filter}"
            );
            ensure!(actual.insert(filter), "duplicate subscription");
        }
    }
    ensure!(actual == expected, "missing command subscriptions");
    Ok(())
}

struct Publication {
    topic: String,
    payload: Vec<u8>,
    retained: bool,
}

fn publication(packet: Packet) -> Result<Publication> {
    ensure!(packet.header >> 4 == 3, "expected PUBLISH");
    ensure!((packet.header >> 1) & 3 == 0, "state must use QoS 0");
    let mut body = packet.body.as_slice();
    let topic = read_string(&mut body)?.to_owned();
    Ok(Publication {
        topic,
        payload: body.to_vec(),
        retained: packet.header & 1 != 0,
    })
}

async fn publish_command(
    socket: &mut TcpStream,
    topic: &str,
    payload: &[u8],
    retained: bool,
) -> Result<()> {
    let mut body = Vec::new();
    push_string(&mut body, topic)?;
    body.extend_from_slice(payload);
    write_packet(socket, 0x30 | u8::from(retained), &body).await
}

fn config(address: SocketAddr, base: &str) -> MqttConfig {
    MqttConfig {
        server: address.ip().to_string(),
        port: address.port(),
        client_id: "loopback-fixture".into(),
        base_topic: base.into(),
        ..MqttConfig::default()
    }
}

#[tokio::test]
async fn qos_zero_subscriptions_retained_state_and_raw_command_forwarding() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = "loopback/custom-root";
        let config = config(listener.local_addr()?, base);
        let mut mqtt = MqttConnection::new(&config)?;
        let mut connections = mqtt.connections.clone();
        assert_eq!(*connections.borrow_and_update(), 0);
        let (mut broker, _) = listener.accept().await?;
        connect(&mut broker, &config.client_id).await?;
        connections.changed().await?;
        assert_eq!(*connections.borrow_and_update(), 1);
        subscriptions(&mut broker, base).await?;

        mqtt.publish(
            base,
            "Living Room Ä",
            vec![
                ("volume_level".into(), "35".into()),
                ("media/title".into(), String::new()),
            ],
        )
        .await?;
        for (suffix, expected) in [("volume_level", "35"), ("media/title", "")] {
            let state = publication(read_packet(&mut broker).await?)?;
            assert_eq!(state.topic, format!("{base}/Living Room Ä/{suffix}"));
            assert_eq!(state.payload, expected.as_bytes());
            assert!(state.retained);
        }

        for (property, retained, bytes) in [
            (
                "rust_cast",
                true,
                b" \x00\xff\x80{\"op\":\"pause\"}\n".as_slice(),
            ),
            ("player_state", false, b" PAUSE\n".as_slice()),
        ] {
            let topic = format!("{base}/Living Room Ä/command/{property}");
            publish_command(&mut broker, &topic, bytes, retained).await?;
            let Some(MqttEvent::Message {
                topic: actual,
                payload,
                retained: actual_retained,
            }) = mqtt.events.recv().await
            else {
                bail!("missing forwarded MQTT command");
            };
            assert_eq!(actual, topic);
            assert_eq!(payload, bytes);
            assert_eq!(actual_retained, retained);
        }
        mqtt.close().await;
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn full_outgoing_queue_does_not_skip_command_subscriptions() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = "loopback/saturated";
        let config = config(listener.local_addr()?, base);
        let mut mqtt = MqttConnection::new(&config)?;

        // No await before all 256 entries are queued on this current-thread runtime.
        for index in 0..256 {
            mqtt.client.try_publish(
                format!("{base}/state/{index}"),
                QoS::AtMostOnce,
                true,
                index.to_string(),
            )?;
        }
        assert!(mqtt
            .client
            .try_publish("overflow", QoS::AtMostOnce, true, "overflow")
            .is_err());

        let (mut broker, _) = listener.accept().await?;
        connect(&mut broker, &config.client_id).await?;
        // Subscriptions must be prioritized ahead of queued state after CONNACK.
        subscriptions(&mut broker, base).await?;
        for index in 0..256 {
            let state = publication(read_packet(&mut broker).await?)?;
            assert_eq!(state.topic, format!("{base}/state/{index}"));
            assert_eq!(state.payload, index.to_string().as_bytes());
            assert!(state.retained);
        }
        assert_eq!(*mqtt.connections.borrow(), 1);
        mqtt.close().await;
        Ok(())
    })
    .await;
}

async fn friendly_name(socket: &mut TcpStream, base: &str, name: &str) -> Result<()> {
    let wanted = format!("{base}/{name}/friendly_name");
    let expected = expected_subscriptions(base);
    let mut subscribed = BTreeSet::new();
    let mut received_name = false;
    while !received_name || subscribed != expected {
        let packet = read_packet(socket).await?;
        match packet.header >> 4 {
            8 => {
                for filter in acknowledge_subscribe(socket, packet).await? {
                    ensure!(
                        expected.contains(&filter),
                        "unexpected subscription {filter}"
                    );
                    subscribed.insert(filter);
                }
            }
            3 => {
                let state = publication(packet)?;
                ensure!(state.retained, "bridge state must be retained");
                if state.topic == wanted {
                    ensure!(
                        state.payload == name.as_bytes(),
                        "wrong friendly_name payload"
                    );
                    received_name = true;
                }
            }
            12 => write_packet(socket, 0xd0, &[]).await?,
            _ => bail!("unexpected packet while awaiting bridge state"),
        }
    }
    Ok(())
}

async fn settle_initial_publications(socket: &mut TcpStream) -> Result<()> {
    // Drain the initial CONNACK snapshot as well as registration publications.
    // Two bridge flush periods of quiet prevent an unsent initial duplicate
    // from masquerading as replay on the second broker connection.
    while let Ok(packet) = timeout(Duration::from_millis(250), read_packet(socket)).await {
        let packet = packet?;
        match packet.header >> 4 {
            3 => {
                publication(packet)?;
            }
            8 => {
                acknowledge_subscribe(socket, packet).await?;
            }
            12 => write_packet(socket, 0xd0, &[]).await?,
            _ => bail!("unexpected packet while draining initial state"),
        }
    }
    Ok(())
}

async fn until_disconnect(socket: &mut TcpStream) -> Result<()> {
    loop {
        let packet = read_packet(socket).await?;
        match packet.header >> 4 {
            14 => {
                ensure!(
                    packet.header == 0xe0 && packet.body.is_empty(),
                    "invalid DISCONNECT"
                );
                return Ok(());
            }
            3 => {
                publication(packet)?;
            }
            8 => {
                acknowledge_subscribe(socket, packet).await?;
            }
            12 => write_packet(socket, 0xd0, &[]).await?,
            _ => bail!("unexpected packet during bridge shutdown"),
        }
    }
}

#[tokio::test]
async fn bridge_replays_unchanged_state_after_broker_reconnect_and_shuts_down() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = "loopback/replay";
        let config = config(listener.local_addr()?, base);
        let cast_port = TcpListener::bind("127.0.0.1:0").await?;
        let unavailable_cast = cast_port.local_addr()?;
        drop(cast_port);
        let name = "Living Room Ä";
        let client_id = config.client_id.clone();
        let bridge = ChromecastMqttBridge::new(BridgeConfig {
            mqtt: config,
            discovery: false,
            devices: vec![Device {
                service: "static:loopback-fixture".into(),
                name: name.into(),
                model: "unknown".into(),
                address: unavailable_cast,
                cast_type: "cast".into(),
            }],
        })?;
        let (shutdown, stopped) = oneshot::channel();
        let broker = async {
            let (mut first, _) = listener.accept().await?;
            connect(&mut first, &client_id).await?;
            friendly_name(&mut first, base, name).await?;
            settle_initial_publications(&mut first).await?;
            drop(first);

            let (mut second, _) = listener.accept().await?;
            connect(&mut second, &client_id).await?;
            friendly_name(&mut second, base, name).await?;
            shutdown
                .send(())
                .map_err(|_| anyhow::anyhow!("bridge stopped before shutdown"))?;
            until_disconnect(&mut second).await
        };
        let (bridge_result, broker_result) = tokio::join!(
            bridge.run(async {
                let _ = stopped.await;
            }),
            broker,
        );
        broker_result?;
        bridge_result?;
        Ok(())
    })
    .await;
}
