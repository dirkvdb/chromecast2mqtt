use std::{
    io::{self, Cursor, Read, Write},
    rc::Rc,
    sync::{Arc, Mutex},
};

use rust_cast::{
    errors::Error,
    message_manager::{CastMessage, CastMessagePayload, MessageManager},
};
use serde_json::{json, Value};

const DEFAULT_RECEIVER_ID: &str = "receiver-0";
const DEFAULT_SENDER_ID: &str = "sender-0";

#[derive(Clone, Default)]
struct MockStream {
    input: Arc<Mutex<Cursor<Vec<u8>>>>,
    output: Arc<Mutex<Vec<u8>>>,
}

impl MockStream {
    fn new() -> Self {
        Self::default()
    }

    fn enqueue(&self, namespace: &str, payload: Value) {
        // Use the native codec to produce receiver fixtures, not a second Cast encoder.
        let encoded = Self::new();
        MessageManager::new(encoded.clone())
            .send(CastMessage {
                namespace: namespace.to_owned(),
                source: DEFAULT_RECEIVER_ID.to_owned(),
                destination: DEFAULT_SENDER_ID.to_owned(),
                payload: CastMessagePayload::String(payload.to_string()),
            })
            .unwrap();
        self.input
            .lock()
            .unwrap()
            .get_mut()
            .extend_from_slice(&encoded.output.lock().unwrap());
    }

    fn sent_payload(&self, index: usize) -> Value {
        let decoder = MessageManager::new(Cursor::new(self.output.lock().unwrap().clone()));
        for _ in 0..index {
            decoder.receive().unwrap();
        }
        let CastMessagePayload::String(payload) = decoder.receive().unwrap().payload else {
            panic!("expected a JSON request");
        };
        serde_json::from_str(&payload).unwrap()
    }
}

impl Read for MockStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.input.lock().unwrap().read(buffer)
    }
}

impl Write for MockStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.output.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

mod receiver {
    use super::*;
    use rust_cast::channels::receiver::*;

    const CHANNEL_NAMESPACE: &str = "urn:x-cast:com.google.cast.receiver";

    fn message(payload: Value) -> CastMessage {
        CastMessage {
            namespace: CHANNEL_NAMESPACE.to_string(),
            source: DEFAULT_RECEIVER_ID.to_string(),
            destination: DEFAULT_SENDER_ID.to_string(),
            payload: CastMessagePayload::String(payload.to_string()),
        }
    }

    fn enqueue(stream: &mut MockStream, payload: Value) {
        stream.enqueue(CHANNEL_NAMESPACE, payload);
    }

    fn status_reply(request_id: u64) -> Value {
        json!({
            "type": "RECEIVER_STATUS", "requestId": request_id,
            "status": {
                "volume": {"level": 0.25, "muted": false},
                "applications": [{
                    "appId": "wanted-app", "sessionId": "session",
                    "transportId": "transport", "displayName": "Test",
                    "statusText": "Playing"
                }]
            }
        })
    }

    fn request_id(response: ReceiverResponse) -> u64 {
        match response {
            ReceiverResponse::Status(status) => status.request_id,
            ReceiverResponse::LaunchError(error) => error.request_id,
            ReceiverResponse::InvalidRequest(error) => error.request_id,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn parse_preserves_all_reply_request_ids_without_narrowing() {
        let channel = ReceiverChannel::new(
            DEFAULT_SENDER_ID,
            DEFAULT_RECEIVER_ID,
            Rc::new(MessageManager::new(MockStream::new())),
        );
        for id in [
            0,
            1,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            1_792_288_955_061,
            u64::MAX,
        ] {
            for typ in ["RECEIVER_STATUS", "LAUNCH_ERROR", "INVALID_REQUEST"] {
                let mut payload = status_reply(id);
                payload["type"] = json!(typ);
                assert_eq!(request_id(channel.parse(&message(payload)).unwrap()), id);
            }
        }
    }

    #[test]
    fn all_reply_deserializers_report_paths_and_keep_serialization_errors() {
        let channel = ReceiverChannel::new(
            DEFAULT_SENDER_ID,
            DEFAULT_RECEIVER_ID,
            Rc::new(MessageManager::new(MockStream::new())),
        );
        for typ in ["RECEIVER_STATUS", "LAUNCH_ERROR", "INVALID_REQUEST"] {
            for invalid in [json!(-1), json!(1.5), json!("not-an-id")] {
                let mut payload = status_reply(1);
                payload["type"] = json!(typ);
                payload["requestId"] = invalid;
                payload["privateData"] = json!("DO_NOT_INCLUDE_FULL_PAYLOAD");
                let Error::Serialization(error) = channel.parse(&message(payload)).unwrap_err()
                else {
                    panic!("expected serialization error");
                };
                assert!(error.is_data());
                assert!(error.to_string().starts_with("requestId:"), "{error}");
                assert!(!error.to_string().contains("DO_NOT_INCLUDE_FULL_PAYLOAD"));
            }
        }
        let mut payload = status_reply(1);
        payload["status"]["volume"]["level"] = json!("not-a-volume");
        let error = channel.parse(&message(payload)).unwrap_err();
        assert!(
            error.to_string().starts_with("status.volume.level:"),
            "{error}"
        );
    }

    #[test]
    fn native_methods_match_small_ids_and_buffer_unrelated_large_replies() {
        for operation in ["GET_STATUS", "LAUNCH", "STOP", "SET_VOLUME"] {
            let mut stream = MockStream::new();
            let mut unrelated = Vec::new();
            // The second ID would alias request 1 if narrowed with `as u32`.
            for id in [1_792_288_955_061, u64::from(u32::MAX) + 2] {
                for typ in ["RECEIVER_STATUS", "LAUNCH_ERROR", "INVALID_REQUEST"] {
                    let mut payload = status_reply(id);
                    payload["type"] = json!(typ);
                    payload["status"]["volume"]["level"] = json!(0.75);
                    payload["status"]["applications"][0]["appId"] = json!("unrelated-app");
                    enqueue(&mut stream, payload.clone());
                    unrelated.push(payload);
                }
            }
            enqueue(&mut stream, status_reply(1));
            enqueue(&mut stream, status_reply(2));
            let manager = Rc::new(MessageManager::new(stream.clone()));
            let channel =
                ReceiverChannel::new(DEFAULT_SENDER_ID, DEFAULT_RECEIVER_ID, manager.clone());
            match operation {
                "GET_STATUS" => {
                    let status = channel.get_status().unwrap();
                    assert_eq!(status.request_id, 1);
                    assert_eq!(status.applications[0].app_id, "wanted-app");
                }
                "LAUNCH" => {
                    assert_eq!(
                        channel
                            .launch_app(&CastDeviceApp::DefaultMediaReceiver)
                            .unwrap()
                            .app_id,
                        "wanted-app"
                    );
                }
                "STOP" => channel.stop_app("session").unwrap(),
                "SET_VOLUME" => {
                    assert_eq!(channel.set_volume(0.25).unwrap().level, Some(0.25))
                }
                _ => unreachable!(),
            }
            assert_eq!(channel.get_status().unwrap().request_id, 2);
            for (index, typ) in [operation, "GET_STATUS"].into_iter().enumerate() {
                let payload = stream.sent_payload(index);
                assert_eq!(payload["requestId"], json!(index + 1));
                assert_eq!(payload["type"], json!(typ));
            }
            for payload in unrelated {
                assert_eq!(manager.receive().unwrap(), message(payload));
            }
        }
    }
}

mod media {
    use super::*;
    use rust_cast::channels::media::*;

    const CHANNEL_NAMESPACE: &str = "urn:x-cast:com.google.cast.media";

    fn message(payload: Value) -> CastMessage {
        CastMessage {
            namespace: CHANNEL_NAMESPACE.to_string(),
            source: DEFAULT_RECEIVER_ID.to_string(),
            destination: DEFAULT_SENDER_ID.to_string(),
            payload: CastMessagePayload::String(payload.to_string()),
        }
    }

    fn enqueue(stream: &mut MockStream, payload: Value) {
        stream.enqueue(CHANNEL_NAMESPACE, payload);
    }

    fn status_reply(request_id: u64) -> Value {
        json!({
            "type": "MEDIA_STATUS", "requestId": request_id,
            "status": [{
                "mediaSessionId": 1, "playerState": "PLAYING",
                "playbackRate": 1.0, "currentTime": 1.0,
                "supportedMediaCommands": 15
            }]
        })
    }

    fn request_id(response: MediaResponse) -> u64 {
        match response {
            MediaResponse::Status(status) => status.request_id,
            MediaResponse::LoadCancelled(error) => error.request_id,
            MediaResponse::LoadFailed(error) => error.request_id,
            MediaResponse::InvalidPlayerState(error) => error.request_id,
            MediaResponse::InvalidRequest(error) => error.request_id,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn parse_preserves_all_reply_request_ids_without_narrowing() {
        let channel = MediaChannel::new(
            DEFAULT_SENDER_ID,
            Rc::new(MessageManager::new(MockStream::new())),
        );
        for id in [
            0,
            1,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            1_792_288_955_061,
            u64::MAX,
        ] {
            for typ in [
                "MEDIA_STATUS",
                "LOAD_CANCELLED",
                "LOAD_FAILED",
                "INVALID_PLAYER_STATE",
                "INVALID_REQUEST",
            ] {
                let mut payload = status_reply(id);
                payload["type"] = json!(typ);
                assert_eq!(request_id(channel.parse(&message(payload)).unwrap()), id);
            }
        }
        let mut payload = status_reply(0);
        payload.as_object_mut().unwrap().remove("requestId");
        assert_eq!(request_id(channel.parse(&message(payload)).unwrap()), 0);
    }

    #[test]
    fn all_reply_deserializers_report_paths_and_keep_serialization_errors() {
        let channel = MediaChannel::new(
            DEFAULT_SENDER_ID,
            Rc::new(MessageManager::new(MockStream::new())),
        );
        for typ in [
            "MEDIA_STATUS",
            "LOAD_CANCELLED",
            "LOAD_FAILED",
            "INVALID_PLAYER_STATE",
            "INVALID_REQUEST",
        ] {
            for invalid in [json!(-1), json!(1.5), json!("not-an-id")] {
                let mut payload = status_reply(1);
                payload["type"] = json!(typ);
                payload["requestId"] = invalid;
                let Error::Serialization(error) = channel.parse(&message(payload)).unwrap_err()
                else {
                    panic!("expected serialization error");
                };
                assert!(error.is_data());
                assert!(error.to_string().starts_with("requestId:"), "{error}");
            }
        }
        let payload = json!({"type": "ERROR", "detailedErrorCode": 1_792_288_955_061_u64});
        let Error::Serialization(error) = channel.parse(&message(payload)).unwrap_err() else {
            panic!("expected serialization error");
        };
        assert!(
            error.to_string().starts_with("detailedErrorCode:"),
            "{error}"
        );
        assert!(error.to_string().contains("expected i32"), "{error}");
    }

    #[test]
    fn other_integer_overflows_keep_their_types_and_report_nested_paths() {
        let channel = MediaChannel::new(
            DEFAULT_SENDER_ID,
            Rc::new(MessageManager::new(MockStream::new())),
        );
        let large = json!(1_792_288_955_061_u64);
        for (field, expected) in [
            ("supportedMediaCommands", "u32"),
            ("mediaSessionId", "i32"),
            ("currentItemId", "u16"),
            ("loadingItemId", "u16"),
            ("preloadedItemId", "u16"),
        ] {
            let mut payload = status_reply(1);
            payload["status"][0][field] = large.clone();
            let Error::Serialization(error) = channel.parse(&message(payload)).unwrap_err() else {
                panic!("expected serialization error");
            };
            assert!(
                error
                    .to_string()
                    .starts_with(&format!("status[0].{field}:")),
                "{error}"
            );
            assert!(
                error.to_string().contains(&format!("expected {expected}")),
                "{error}"
            );
        }
        for extended in [false, true] {
            for field in [
                "metadataType",
                "season",
                "episode",
                "trackNumber",
                "discNumber",
                "width",
                "height",
                "images/0/width",
                "images/0/height",
            ] {
                let mut payload = status_reply(1);
                let mut metadata = json!({
                    "metadataType": 0, "season": 1, "episode": 1,
                    "trackNumber": 1, "discNumber": 1, "width": 1, "height": 1,
                    "images": [{"url": "https://example.invalid/image", "width": 1, "height": 1}]
                });
                *metadata.pointer_mut(&format!("/{field}")).unwrap() = large.clone();
                let media = json!({
                    "contentId": "id", "contentType": "audio/mpeg", "metadata": metadata,
                    "customData": {"private": "DO_NOT_INCLUDE_FULL_PAYLOAD"}
                });
                let location = if extended {
                    payload["status"][0]["extendedStatus"] =
                        json!({"playerState": "LOADING", "media": media});
                    "extendedStatus.media"
                } else {
                    payload["status"][0]["media"] = media;
                    "media"
                };
                let Error::Serialization(error) = channel.parse(&message(payload)).unwrap_err()
                else {
                    panic!("expected serialization error");
                };
                let field = field.replace("/0/", "[0].");
                let path = format!("status[0].{location}.metadata.{field}:");
                assert!(error.to_string().starts_with(&path), "{error}");
                assert!(error.to_string().contains("expected u32"), "{error}");
                assert!(!error.to_string().contains("DO_NOT_INCLUDE_FULL_PAYLOAD"));
            }
        }
        let mut payload = status_reply(1);
        payload["status"][0]["currentTime"] = large;
        assert!(channel.parse(&message(payload)).is_ok());
    }

    #[test]
    fn native_methods_match_small_ids_and_buffer_unrelated_large_replies() {
        for operation in [
            "GET_STATUS",
            "LOAD",
            "LOAD_WITH_OPTS",
            "LOAD_WITH_QUEUE",
            "QUEUE_LOAD",
            "PAUSE",
            "PLAY",
            "STOP",
            "SEEK",
        ] {
            let mut stream = MockStream::new();
            let mut unrelated = Vec::new();
            // The second ID would alias request 1 if narrowed with `as u32`.
            for id in [1_792_288_955_061, u64::from(u32::MAX) + 2] {
                for typ in [
                    "MEDIA_STATUS",
                    "LOAD_CANCELLED",
                    "LOAD_FAILED",
                    "INVALID_PLAYER_STATE",
                    "INVALID_REQUEST",
                ] {
                    let mut payload = status_reply(id);
                    payload["type"] = json!(typ);
                    payload["status"][0]["currentTime"] = json!(99.0);
                    enqueue(&mut stream, payload.clone());
                    unrelated.push(payload);
                }
            }
            enqueue(&mut stream, status_reply(1));
            enqueue(&mut stream, status_reply(2));
            let manager = Rc::new(MessageManager::new(stream.clone()));
            let channel = MediaChannel::new(DEFAULT_SENDER_ID, manager.clone());
            let media = Media {
                content_id: "wanted-media".to_string(),
                stream_type: StreamType::Buffered,
                content_type: "audio/mpeg".to_string(),
                metadata: None,
                duration: None,
            };
            let queue = MediaQueue {
                items: vec![QueueItem {
                    media: media.clone(),
                }],
                start_index: 0,
                queue_type: QueueType::Playlist,
            };
            let entry = match operation {
                "GET_STATUS" => {
                    let mut status = channel.get_status("transport", None).unwrap();
                    assert_eq!(status.request_id, 1);
                    status.entries.remove(0)
                }
                "LOAD" | "LOAD_WITH_OPTS" | "LOAD_WITH_QUEUE" | "QUEUE_LOAD" => {
                    let mut status = match operation {
                        "LOAD" => channel.load("transport", "session", &media),
                        "LOAD_WITH_OPTS" => channel.load_with_opts(
                            "transport",
                            "session",
                            &media,
                            LoadOptions::default(),
                        ),
                        "LOAD_WITH_QUEUE" => channel.load_with_queue(
                            "transport",
                            "session",
                            &media,
                            Some(&queue),
                            LoadOptions::default(),
                        ),
                        "QUEUE_LOAD" => channel.load_queue("transport", "session", &queue),
                        _ => unreachable!(),
                    }
                    .unwrap();
                    assert_eq!(status.request_id, 1);
                    status.entries.remove(0)
                }
                "PAUSE" => channel.pause("transport", 1).unwrap(),
                "PLAY" => channel.play("transport", 1).unwrap(),
                "STOP" => channel.stop("transport", 1).unwrap(),
                "SEEK" => channel.seek("transport", 1, Some(1.0), None).unwrap(),
                _ => unreachable!(),
            };
            assert_eq!(entry.current_time, Some(1.0));
            assert_eq!(channel.get_status("transport", None).unwrap().request_id, 2);
            let sent_type = match operation {
                "LOAD_WITH_OPTS" | "LOAD_WITH_QUEUE" => "LOAD",
                _ => operation,
            };
            for (index, typ) in [sent_type, "GET_STATUS"].into_iter().enumerate() {
                let payload = stream.sent_payload(index);
                assert_eq!(payload["requestId"], json!(index + 1));
                assert_eq!(payload["type"], json!(typ));
            }
            for payload in unrelated {
                assert_eq!(manager.receive().unwrap(), message(payload));
            }
        }
    }
}
