use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    Volume { value: f64, relative: bool },
    Mute(bool),
    Seek(f64),
    Resume,
    Pause,
    Stop,
    Load(Box<MediaLoad>),
    Native(NativeCommand),
}

/// Operations exposed by rust_cast itself. Media and queue DTO validation and
/// conversion belong to the backend; this module only validates their envelopes.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCommand {
    #[serde(deserialize_with = "native_unit")]
    GetReceiverStatus,
    LaunchApp {
        app_id: String,
    },
    StopApp {
        session_id: Option<String>,
    },
    SetVolume {
        level: Option<f32>,
        muted: Option<bool>,
    },
    BroadcastMessage {
        namespace: String,
        message: Value,
    },
    #[serde(deserialize_with = "native_unit")]
    GetMediaStatus,
    #[serde(alias = "load_with_opts")]
    Load {
        media: Value,
        options: Option<Value>,
    },
    LoadQueue {
        queue: Value,
    },
    LoadWithQueue {
        media: Value,
        queue: Value,
        options: Option<Value>,
    },
    #[serde(deserialize_with = "native_unit")]
    Play,
    #[serde(deserialize_with = "native_unit")]
    Pause,
    #[serde(deserialize_with = "native_unit")]
    Stop,
    Seek {
        current_time: Option<f64>,
        resume_state: Option<String>,
    },
    Connect {
        destination: String,
    },
    Disconnect {
        destination: String,
    },
    #[serde(deserialize_with = "native_unit")]
    Ping,
    #[serde(deserialize_with = "native_unit")]
    Pong,
}

fn native_unit<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<(), D::Error> {
    // Serde ignores unknown fields on internally tagged unit variants even with
    // deny_unknown_fields, so deserialize their arguments as an empty struct.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Arguments {}

    Arguments::deserialize(deserializer).map(|_| ())
}

impl NativeCommand {
    fn validate(&self) -> Result<()> {
        match self {
            Self::LaunchApp { app_id } => {
                ensure!(!app_id.trim().is_empty(), "app_id must not be empty");
            }
            Self::StopApp {
                session_id: Some(session_id),
            } => {
                ensure!(
                    !session_id.trim().is_empty(),
                    "session_id must not be empty"
                );
            }
            Self::SetVolume {
                level: Some(level), ..
            } => {
                ensure!(
                    level.is_finite() && (0.0..=1.0).contains(level),
                    "native volume level must be finite and between 0 and 1"
                );
            }
            Self::BroadcastMessage { namespace, .. } => {
                ensure!(
                    namespace.starts_with("urn:x-cast:") && namespace.len() > "urn:x-cast:".len(),
                    "namespace must start with urn:x-cast: and include a name"
                );
            }
            Self::Load { media, options } => {
                ensure!(media.is_object(), "native media must be an object");
                if let Some(options) = options {
                    ensure!(
                        options.is_object(),
                        "native load options must be an object or null"
                    );
                }
            }
            Self::LoadQueue { queue } => {
                ensure!(queue.is_object(), "native queue must be an object");
            }
            Self::LoadWithQueue {
                media,
                queue,
                options,
            } => {
                ensure!(media.is_object(), "native media must be an object");
                ensure!(queue.is_object(), "native queue must be an object");
                ensure!(
                    options.as_ref().is_none_or(Value::is_object),
                    "native load options must be an object or null"
                );
            }
            Self::Seek {
                current_time,
                resume_state,
            } => {
                if let Some(current_time) = current_time {
                    ensure!(
                        current_time.is_finite() && *current_time >= 0.0,
                        "native current_time must be finite and nonnegative"
                    );
                }
                if let Some(resume_state) = resume_state {
                    ensure!(
                        matches!(resume_state.as_str(), "PLAYBACK_START" | "PLAYBACK_PAUSE"),
                        "resume_state must be PLAYBACK_START or PLAYBACK_PAUSE"
                    );
                }
            }
            Self::Connect { destination } | Self::Disconnect { destination } => {
                ensure!(
                    !destination.trim().is_empty(),
                    "destination must not be empty"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MediaLoad {
    pub url: String,
    // An ordinary Option would also accept an absent key. Python requires this
    // argument even when its explicitly supplied value is null.
    #[serde(deserialize_with = "required_content_type")]
    pub content_type: Option<String>,
    pub title: Option<String>,
    pub thumb: Option<String>,
    pub current_time: Option<f64>,
    #[serde(default = "default_autoplay", deserialize_with = "nullable_autoplay")]
    pub autoplay: bool,
    #[serde(
        default = "default_stream_type",
        deserialize_with = "nullable_stream_type"
    )]
    pub stream_type: String,
    pub metadata: Option<Value>,
    #[serde(default, deserialize_with = "supported_subtitles")]
    pub subtitles: Option<String>,
    #[serde(
        default = "default_subtitles_lang",
        deserialize_with = "supported_subtitles_lang"
    )]
    pub subtitles_lang: String,
    #[serde(
        default = "default_subtitles_mime",
        deserialize_with = "supported_subtitles_mime"
    )]
    pub subtitles_mime: String,
    #[serde(
        default = "default_subtitle_id",
        deserialize_with = "supported_subtitle_id"
    )]
    pub subtitle_id: i64,
    #[serde(default, deserialize_with = "supported_enqueue")]
    pub enqueue: bool,
}

fn required_content_type<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

fn default_only<'de, D, T>(
    deserializer: D,
    default: T,
    option: &str,
) -> std::result::Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + PartialEq,
{
    if let Some(value) = Option::<T>::deserialize(deserializer)?
        && value != default
    {
        return Err(serde::de::Error::custom(format!(
            "unsupported media option {option}: only its default or null is supported"
        )));
    }
    Ok(default)
}

fn supported_enqueue<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<bool, D::Error> {
    default_only(deserializer, false, "enqueue")
}

fn supported_subtitles<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    default_only(deserializer, None, "subtitles")
}

fn supported_subtitles_lang<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    default_only(deserializer, default_subtitles_lang(), "subtitles_lang")
}

fn supported_subtitles_mime<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    default_only(deserializer, default_subtitles_mime(), "subtitles_mime")
}

fn supported_subtitle_id<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    default_only(deserializer, default_subtitle_id(), "subtitle_id")
}

fn nullable_autoplay<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or_else(default_autoplay))
}

fn nullable_stream_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_else(default_stream_type))
}

fn default_autoplay() -> bool {
    true
}

fn default_stream_type() -> String {
    "BUFFERED".into()
}

fn default_subtitles_lang() -> String {
    "en-US".into()
}

fn default_subtitles_mime() -> String {
    "text/vtt".into()
}

fn default_subtitle_id() -> i64 {
    1
}

impl MediaLoad {
    fn new(url: String, content_type: Option<String>) -> Self {
        Self {
            url,
            content_type,
            title: None,
            thumb: None,
            current_time: None,
            autoplay: default_autoplay(),
            stream_type: default_stream_type(),
            metadata: None,
            subtitles: None,
            subtitles_lang: default_subtitles_lang(),
            subtitles_mime: default_subtitles_mime(),
            subtitle_id: default_subtitle_id(),
            enqueue: false,
        }
    }
}

/// Parse a legacy property name or `rust_cast` (without `command/`).
///
/// Volume values are percentage points, not Cast's normalized 0..1 values.
/// Unknown properties, empty payloads, and unsupported mute values are ignored.
/// Malformed UTF-8, numbers, or media schemas return an error.
pub fn parse(property: &str, payload: &[u8]) -> Result<Option<Command>> {
    let text = std::str::from_utf8(payload)
        .context("command payload is not UTF-8")?
        .trim();
    if text.is_empty() {
        return Ok(None);
    }

    let command = match property {
        "volume_muted" => match text {
            "0" => Command::Mute(false),
            "1" => Command::Mute(true),
            _ => return Ok(None),
        },
        "volume_level" => Command::Volume {
            value: numeric_value(text)?,
            relative: text.starts_with(['+', '-']),
        },
        "player_position" => Command::Seek(numeric_value(text)?),
        "player_state" => match text {
            "RESUME" => Command::Resume,
            "PAUSE" => Command::Pause,
            "STOP" => Command::Stop,
            "SKIP" | "REWIND" | "PREV" | "NEXT" => {
                bail!("unsupported player_state command {text}: no matching rust_cast method");
            }
            _ => Command::Load(Box::new(parse_media(text)?)),
        },
        "rust_cast" => {
            let native: NativeCommand =
                serde_json::from_str(text).context("invalid rust_cast command")?;
            native.validate()?;
            Command::Native(native)
        }
        _ => return Ok(None),
    };
    Ok(Some(command))
}

fn numeric_value(text: &str) -> Result<f64> {
    let value: f64 = text.parse().context("command requires a number")?;
    ensure!(value.is_finite(), "command requires a finite number");
    Ok(value.trunc())
}

fn parse_media(text: &str) -> Result<MediaLoad> {
    let load = if text.starts_with('{') {
        serde_json::from_str::<MediaLoad>(text).context("invalid media object")?
    } else if text.starts_with('[') {
        let (url, content_type): (String, Option<String>) =
            serde_json::from_str(text).context("media array must contain a URL and MIME type")?;
        MediaLoad::new(url, content_type)
    } else {
        MediaLoad::new(
            text.to_owned(),
            mime_guess::from_path(text).first_raw().map(str::to_owned),
        )
    };
    if let Some(current_time) = load.current_time {
        ensure!(
            current_time.is_finite(),
            "media current_time must be finite"
        );
    }
    if let Some(metadata) = &load.metadata {
        ensure!(
            metadata.is_object(),
            "media metadata must be an object or null"
        );
    }
    Ok(load)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn command(property: &str, text: &str) -> Command {
        parse(property, text.as_bytes()).unwrap().unwrap()
    }

    fn media(text: &str) -> MediaLoad {
        match command("player_state", text) {
            Command::Load(load) => *load,
            other => panic!("expected a media load, got {other:?}"),
        }
    }

    #[test]
    fn empty_and_unknown_properties_are_ignored() {
        for property in [
            "volume_level",
            "volume_muted",
            "player_position",
            "player_state",
            "rust_cast",
        ] {
            for text in ["", " \t\r\n", "\u{2003}"] {
                assert_eq!(parse(property, text.as_bytes()).unwrap(), None);
            }
        }
        assert_eq!(parse("unknown", b"PAUSE").unwrap(), None);
        assert_eq!(parse("command/player_state", b"PAUSE").unwrap(), None);
    }

    #[test]
    fn invalid_utf8_is_an_error() {
        assert!(parse("player_state", &[0xff]).is_err());
    }

    #[test]
    fn mute_only_accepts_zero_or_one_after_trimming() {
        assert_eq!(command("volume_muted", " 0\n"), Command::Mute(false));
        assert_eq!(command("volume_muted", "\t1 "), Command::Mute(true));
        for text in ["true", "false", "01", "1.0", "-1", "+1", "2"] {
            assert_eq!(parse("volume_muted", text.as_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn volume_truncates_without_normalizing_or_clamping() {
        for (text, value, relative) in [
            ("55", 55.0, false),
            (" 55.9 ", 55.0, false),
            ("0.5", 0.0, false),
            ("+5.9", 5.0, true),
            ("-5.9", -5.0, true),
            ("-0.9", 0.0, true),
            ("1e2", 100.0, false),
            ("-1e2", -100.0, true),
            ("12345", 12345.0, false),
        ] {
            assert_eq!(
                command("volume_level", text),
                Command::Volume { value, relative }
            );
        }
    }

    #[test]
    fn seek_is_absolute_and_truncates_toward_zero() {
        assert_eq!(command("player_position", "+12.9"), Command::Seek(12.0));
        assert_eq!(command("player_position", "-12.9"), Command::Seek(-12.0));
        assert_eq!(command("player_position", "1.5e2"), Command::Seek(150.0));
    }

    #[test]
    fn invalid_and_nonfinite_numbers_are_errors() {
        for property in ["volume_level", "player_position"] {
            for text in ["no", "+", "--1", "1 2", "NaN", "inf", "-inf", "1e999"] {
                assert!(
                    parse(property, text.as_bytes()).is_err(),
                    "{property}: {text}"
                );
            }
        }
    }

    #[test]
    fn exact_player_keywords_are_recognized() {
        for (text, expected) in [
            ("RESUME", Command::Resume),
            ("PAUSE", Command::Pause),
            ("STOP", Command::Stop),
        ] {
            assert_eq!(command("player_state", &format!(" \n{text}\t")), expected);
        }
    }

    #[test]
    fn unknown_keywords_and_quoted_json_strings_are_literal_urls() {
        for text in [
            "pause",
            "PLAY",
            "PLAYING",
            "null",
            "\"https://example.test/a.mp3\"",
        ] {
            assert_eq!(media(text).url, text);
        }
    }

    #[test]
    fn plain_urls_use_mime_guess_without_a_fallback() {
        let load = media(" \nhttps://example.test/song.mp3\t");
        assert_eq!(load.url, "https://example.test/song.mp3");
        assert_eq!(load.content_type.as_deref(), Some("audio/mpeg"));
        assert_eq!(media("https://example.test/stream").content_type, None);
        assert_eq!(
            media("https://example.test/song.mp3?token=x").content_type,
            None
        );
    }

    #[test]
    fn all_load_forms_share_connector_defaults() {
        let expected = MediaLoad::new(
            "https://example.test/a.mp3".into(),
            Some("audio/mpeg".into()),
        );
        for text in [
            "https://example.test/a.mp3",
            r#"["https://example.test/a.mp3", "audio/mpeg"]"#,
            r#"{"url":"https://example.test/a.mp3","content_type":"audio/mpeg"}"#,
        ] {
            let load = media(text);
            assert_eq!(load, expected);
            assert!(load.autoplay);
            assert!(!load.enqueue);
            assert_eq!(load.stream_type, "BUFFERED");
            assert_eq!(load.subtitles_lang, "en-US");
            assert_eq!(load.subtitles_mime, "text/vtt");
            assert_eq!(load.subtitle_id, 1);
        }
    }

    #[test]
    fn object_requires_content_type_but_accepts_explicit_null() {
        assert!(parse("player_state", br#"{"url":"a.mp3"}"#).is_err());
        assert!(serde_json::from_value::<MediaLoad>(json!({"url": "a.mp3"})).is_err());
        assert_eq!(
            media(r#"{"url":"a.mp3","content_type":null}"#).content_type,
            None
        );
        assert_eq!(media(r#"["a.mp3",null]"#).content_type, None);
    }

    #[test]
    fn explicit_json_mime_is_never_guessed_or_overridden() {
        assert_eq!(
            media(r#"["a.mp3","custom/type"]"#).content_type.as_deref(),
            Some("custom/type")
        );
        assert_eq!(media(r#"["a.mp3",""]"#).content_type.as_deref(), Some(""));
    }

    #[test]
    fn every_supported_media_option_is_preserved() {
        let value = json!({
            "url": "https://example.test/video", "content_type": "video/mp4",
            "title": "Title", "thumb": "https://example.test/image.png",
            "current_time": 1.75, "autoplay": false, "stream_type": "LIVE",
            "metadata": {"artist": "Artist", "nested": {"number": 2}},
            "subtitles": null, "subtitles_lang": "en-US",
            "subtitles_mime": "text/vtt", "subtitle_id": 1, "enqueue": false
        });
        let load = media(&value.to_string());
        assert_eq!(serde_json::to_value(load).unwrap(), value);
    }

    #[test]
    fn optional_nulls_are_accepted() {
        let load = media(
            r#"{
            "url":"stream","content_type":null,"title":null,"thumb":null,
            "current_time":null,"metadata":null,"subtitles":null
        }"#,
        );
        assert_eq!(load, MediaLoad::new("stream".into(), None));
    }

    #[test]
    fn unsupported_legacy_keywords_return_explicit_errors() {
        for keyword in ["SKIP", "REWIND", "PREV", "NEXT"] {
            let error = parse("player_state", format!(" {keyword}\n").as_bytes()).unwrap_err();
            assert!(error.to_string().contains("unsupported"));
            assert!(error.to_string().contains(keyword));
        }
    }

    #[test]
    fn unsupported_legacy_load_options_are_rejected_by_serde_and_parse() {
        for (key, value) in [
            ("enqueue", json!(true)),
            ("subtitles", json!("https://example.test/sub.vtt")),
            ("subtitles", json!("")),
            ("subtitles_lang", json!("de-DE")),
            ("subtitles_mime", json!("application/x-subrip")),
            ("subtitle_id", json!(2)),
        ] {
            let mut object = json!({"url": "stream", "content_type": null});
            object[key] = value;
            assert!(
                serde_json::from_value::<MediaLoad>(object.clone()).is_err(),
                "{object}"
            );
            let error = parse("player_state", object.to_string().as_bytes()).unwrap_err();
            assert!(
                format!("{error:#}").contains("unsupported media option"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn default_and_null_legacy_options_are_accepted_for_migration() {
        for object in [
            json!({
                "url": "stream", "content_type": null, "subtitles": null,
                "subtitles_lang": "en-US", "subtitles_mime": "text/vtt",
                "subtitle_id": 1, "enqueue": false
            }),
            json!({
                "url": "stream", "content_type": null, "subtitles": null,
                "subtitles_lang": null, "subtitles_mime": null,
                "subtitle_id": null, "enqueue": null,
                "autoplay": null, "stream_type": null
            }),
        ] {
            assert_eq!(
                media(&object.to_string()),
                MediaLoad::new("stream".into(), None)
            );
        }
    }

    fn native(value: Value) -> NativeCommand {
        match command("rust_cast", &value.to_string()) {
            Command::Native(native) => native,
            other => panic!("expected a native command, got {other:?}"),
        }
    }

    #[test]
    fn native_unit_operations_use_exact_snake_case_tags() {
        for (op, expected) in [
            ("get_receiver_status", NativeCommand::GetReceiverStatus),
            ("get_media_status", NativeCommand::GetMediaStatus),
            ("play", NativeCommand::Play),
            ("pause", NativeCommand::Pause),
            ("stop", NativeCommand::Stop),
            ("ping", NativeCommand::Ping),
            ("pong", NativeCommand::Pong),
        ] {
            assert_eq!(native(json!({"op": op})), expected);
            assert!(
                serde_json::from_value::<NativeCommand>(json!({"op": op, "extra": 1})).is_err()
            );
            assert!(parse(
                "rust_cast",
                json!({"op": op.to_uppercase()}).to_string().as_bytes()
            )
            .is_err());
            assert!(parse(
                "rust_cast",
                json!({"op": op, "extra": 1}).to_string().as_bytes()
            )
            .is_err());
        }
    }

    #[test]
    fn native_receiver_and_connection_operations_preserve_fields() {
        assert_eq!(
            native(json!({"op": "launch_app", "app_id": "CC1AD845"})),
            NativeCommand::LaunchApp {
                app_id: "CC1AD845".into()
            }
        );
        assert_eq!(
            native(json!({"op": "stop_app", "session_id": "session"})),
            NativeCommand::StopApp {
                session_id: Some("session".into())
            }
        );
        assert_eq!(
            native(json!({"op": "set_volume", "level": 0.5, "muted": true})),
            NativeCommand::SetVolume {
                level: Some(0.5_f32),
                muted: Some(true)
            }
        );
        for level in [0.0_f32, 1.0_f32] {
            assert_eq!(
                native(json!({"op": "set_volume", "level": level})),
                NativeCommand::SetVolume {
                    level: Some(level),
                    muted: None
                }
            );
        }
        let message = json!({"nested": ["message", true, null]});
        assert_eq!(
            native(
                json!({"op": "broadcast_message", "namespace": "urn:x-cast:example", "message": message})
            ),
            NativeCommand::BroadcastMessage {
                namespace: "urn:x-cast:example".into(),
                message
            }
        );
        assert_eq!(
            native(json!({"op": "connect", "destination": "receiver-0"})),
            NativeCommand::Connect {
                destination: "receiver-0".into()
            }
        );
        assert_eq!(
            native(json!({"op": "disconnect", "destination": "transport-1"})),
            NativeCommand::Disconnect {
                destination: "transport-1".into()
            }
        );
    }

    #[test]
    fn native_media_and_queue_dtos_are_preserved_for_backend_conversion() {
        let media = json!({"contentId": "https://example.test/a.mp3", "contentType": "audio/mpeg", "streamType": "BUFFERED"});
        let options = json!({"autoplay": false, "currentTime": 1.75});
        let queue = json!({"items": [{"media": media}], "startIndex": 0, "queueType": "PLAYLIST"});
        assert_eq!(
            native(json!({"op": "load", "media": media, "options": options})),
            NativeCommand::Load {
                media: media.clone(),
                options: Some(options)
            }
        );
        assert_eq!(
            native(json!({"op": "load_queue", "queue": queue})),
            NativeCommand::LoadQueue {
                queue: queue.clone()
            }
        );
        assert_eq!(
            native(json!({"op": "load_with_queue", "media": media, "queue": queue})),
            NativeCommand::LoadWithQueue {
                media: media.clone(),
                queue: queue.clone(),
                options: None
            }
        );
        assert!(matches!(
            native(
                json!({"op":"load_with_queue", "media":media, "queue":queue, "options":{"autoplay":false}})
            ),
            NativeCommand::LoadWithQueue {
                options: Some(_),
                ..
            }
        ));
        assert!(matches!(
            native(json!({"op":"load_with_opts", "media":media, "options":{"currentTime":1.5}})),
            NativeCommand::Load {
                options: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn native_optional_arguments_can_be_omitted_or_null() {
        assert_eq!(
            native(json!({"op": "stop_app"})),
            NativeCommand::StopApp { session_id: None }
        );
        assert_eq!(
            native(json!({"op": "stop_app", "session_id": null})),
            NativeCommand::StopApp { session_id: None }
        );
        assert_eq!(
            native(json!({"op": "set_volume"})),
            NativeCommand::SetVolume {
                level: None,
                muted: None
            }
        );
        assert_eq!(
            native(json!({"op": "set_volume", "level": null, "muted": null})),
            NativeCommand::SetVolume {
                level: None,
                muted: None
            }
        );
        assert_eq!(
            native(json!({"op": "seek"})),
            NativeCommand::Seek {
                current_time: None,
                resume_state: None
            }
        );
        assert_eq!(
            native(json!({"op": "seek", "current_time": null, "resume_state": null})),
            NativeCommand::Seek {
                current_time: None,
                resume_state: None
            }
        );
        for object in [
            json!({"op": "load", "media": {}}),
            json!({"op": "load", "media": {}, "options": null}),
        ] {
            assert_eq!(
                native(object),
                NativeCommand::Load {
                    media: json!({}),
                    options: None
                }
            );
        }
    }

    #[test]
    fn native_seek_keeps_fractional_seconds_and_validates_resume_state() {
        for resume_state in ["PLAYBACK_START", "PLAYBACK_PAUSE"] {
            assert_eq!(
                native(json!({"op": "seek", "current_time": 1.75, "resume_state": resume_state})),
                NativeCommand::Seek {
                    current_time: Some(1.75),
                    resume_state: Some(resume_state.into())
                }
            );
        }
        assert_eq!(
            native(json!({"op": "seek", "current_time": 0})),
            NativeCommand::Seek {
                current_time: Some(0.0),
                resume_state: None
            }
        );
    }

    #[test]
    fn malformed_native_schemas_and_out_of_bounds_values_are_errors() {
        for object in [
            json!(null),
            json!([]),
            json!("ping"),
            json!({}),
            json!({"op": 1}),
            json!({"op": "skip"}),
            json!({"op": "launch_app"}),
            json!({"op": "launch_app", "app_id": null}),
            json!({"op": "launch_app", "app_id": " "}),
            json!({"op": "launch_app", "app_id": "app", "extra": true}),
            json!({"op": "stop_app", "session_id": ""}),
            json!({"op": "set_volume", "level": -0.1}),
            json!({"op": "set_volume", "level": 1.1}),
            json!({"op": "set_volume", "level": "0.5"}),
            json!({"op": "set_volume", "muted": "true"}),
            json!({"op": "broadcast_message", "namespace": "example", "message": {}}),
            json!({"op": "broadcast_message", "namespace": "urn:x-cast:", "message": {}}),
            json!({"op": "broadcast_message", "namespace": "urn:x-cast:example"}),
            json!({"op": "load", "media": null}),
            json!({"op": "load", "media": []}),
            json!({"op": "load", "media": {}, "options": []}),
            json!({"op": "load_queue", "queue": []}),
            json!({"op": "load_with_queue", "media": {}}),
            json!({"op": "load_with_queue", "media": {}, "queue": null}),
            json!({"op": "seek", "current_time": -1}),
            json!({"op": "seek", "resume_state": "PAUSED"}),
            json!({"op": "seek", "resume_state": "playback_start"}),
            json!({"op": "connect", "destination": ""}),
            json!({"op": "disconnect", "destination": " "}),
            json!({"op": "disconnect", "destination": "receiver-0", "extra": false}),
        ] {
            assert!(
                parse("rust_cast", object.to_string().as_bytes()).is_err(),
                "{object}"
            );
        }
        for text in [
            "{bad",
            r#"{"op":"set_volume","level":1e39}"#,
            r#"{"op":"seek","current_time":1e999}"#,
        ] {
            assert!(parse("rust_cast", text.as_bytes()).is_err(), "{text}");
        }
        for level in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(NativeCommand::SetVolume {
                level: Some(level),
                muted: None
            }
            .validate()
            .is_err());
        }
        for current_time in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(NativeCommand::Seek {
                current_time: Some(current_time),
                resume_state: None
            }
            .validate()
            .is_err());
        }
    }

    #[test]
    fn invalid_media_schemas_do_not_fall_back_to_urls() {
        for text in [
            "{bad",
            "[bad",
            "[]",
            r#"["url"]"#,
            r#"["url","mime",1]"#,
            r#"[null,"mime"]"#,
            r#"["url",3]"#,
            r#"{}"#,
            r#"{"content_type":"audio/mpeg"}"#,
            r#"{"url":3,"content_type":null}"#,
            r#"{"url":"x","content_type":3}"#,
            r#"{"url":"x","content_type":null,"extra":true}"#,
            r#"{"url":"x","content_type":null,"autoplay":"false"}"#,
            r#"{"url":"x","content_type":null,"current_time":"1"}"#,
            r#"{"url":"x","content_type":null,"current_time":1e999}"#,
            r#"{"url":"x","content_type":null,"subtitle_id":1.5}"#,
            r#"{"url":"x","content_type":null,"metadata":[]}"#,
        ] {
            assert!(parse("player_state", text.as_bytes()).is_err(), "{text}");
        }
    }
}
