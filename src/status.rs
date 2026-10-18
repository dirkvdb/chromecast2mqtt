use std::collections::BTreeMap;

use serde_json::{json, Value};

pub const RECEIVER_NAMESPACE: &str = "urn:x-cast:com.google.cast.receiver";
pub const MEDIA_NAMESPACE: &str = "urn:x-cast:com.google.cast.media";
const IDLE_APP_ID: &str = "E8C28D3C";

/// Last published strings, plus the media state needed to merge partial updates.
/// Snapshot entries include empty payloads so a reconnect can clear retained data.
#[derive(Default)]
pub struct StatusCache {
    values: BTreeMap<String, String>,
    media: MediaState,
}

impl StatusCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Map a complete Cast message to changed, relative MQTT topic suffixes.
    /// Messages without a status of the appropriate shape are ignored.
    pub fn apply(&mut self, namespace: &str, payload: &Value) -> Vec<(String, String)> {
        match namespace {
            RECEIVER_NAMESPACE => self.apply_receiver(payload),
            MEDIA_NAMESPACE => self.apply_media(payload),
            _ => Vec::new(),
        }
    }

    pub fn set(
        &mut self,
        suffix: impl Into<String>,
        payload: impl Into<String>,
    ) -> Vec<(String, String)> {
        let suffix = suffix.into();
        let payload = payload.into();
        if self.values.get(&suffix) == Some(&payload) {
            return Vec::new();
        }
        self.values.insert(suffix.clone(), payload.clone());
        vec![(suffix, payload)]
    }

    /// Return the complete known state, sorted by suffix, without changing deduplication.
    pub fn snapshot(&self) -> Vec<(String, String)> {
        self.values
            .iter()
            .map(|(suffix, payload)| (suffix.clone(), payload.clone()))
            .collect()
    }

    fn apply_receiver(&mut self, payload: &Value) -> Vec<(String, String)> {
        let Some(status) = payload.get("status").and_then(Value::as_object) else {
            return Vec::new();
        };
        let app = status
            .get("applications")
            .and_then(Value::as_array)
            .and_then(|apps| apps.first());
        let volume = status.get("volume");
        let mut values = vec![
            ("current_app", field(app, "displayName", Value::Null)),
            ("volume_level", field(volume, "level", json!(1.0))),
            ("volume_muted", field(volume, "muted", json!(false))),
            ("connection_status", json!("CONNECTED")),
        ];
        let app_id = app.and_then(|app| app.get("appId"));
        if app_id.is_none_or(Value::is_null) || app_id.and_then(Value::as_str) == Some(IDLE_APP_ID)
        {
            // The Python receiver callback overrides only the published player
            // fields, not metadata or the library's underlying media status.
            values.extend([
                ("player_state", json!("IDLE")),
                ("player_position", Value::Null),
                ("player_duration", Value::Null),
            ]);
        }
        self.publish(values)
    }

    fn apply_media(&mut self, payload: &Value) -> Vec<(String, String)> {
        let Some(statuses) = payload.get("status").and_then(Value::as_array) else {
            return Vec::new();
        };
        if statuses.is_empty() {
            // An explicit absence of a media session must not leave retained
            // metadata from the preceding session behind.
            self.media = MediaState::idle();
        } else if let Some(status) = statuses[0].as_object() {
            if let Some(value) = status.get("playerState") {
                self.media.player_state = value.clone();
            }
            if let Some(value) = status.get("currentTime") {
                self.media.current_time = value.clone();
            }

            let media = status
                .get("media")
                .filter(|value| value.as_object().is_some_and(|map| !map.is_empty()))
                .or_else(|| {
                    status
                        .get("extendedStatus")
                        .and_then(|extended| extended.get("media"))
                        .filter(|value| value.is_object())
                });
            if let Some(media) = media {
                if let Some(value) = media.get("duration") {
                    self.media.duration = value.clone();
                }
                if let Some(value) = media.get("contentId") {
                    self.media.content_id = value.clone();
                }
                if let Some(value) = media.get("contentType") {
                    self.media.content_type = value.clone();
                }
                if let Some(value) = media.get("metadata") {
                    // PyChromecast preserves an omitted metadata object, but
                    // replaces it wholesale when one is supplied.
                    self.media.metadata = value.clone();
                }
            }
        } else {
            return Vec::new();
        }
        self.publish(self.media.values())
    }

    fn publish(&mut self, values: Vec<(&str, Value)>) -> Vec<(String, String)> {
        let mut updates = Vec::new();
        for (suffix, value) in values {
            updates.extend(self.set(suffix, serialize_value(&value)));
        }
        updates
    }
}

struct MediaState {
    player_state: Value,
    current_time: Value,
    duration: Value,
    content_id: Value,
    content_type: Value,
    metadata: Value,
}

impl Default for MediaState {
    fn default() -> Self {
        Self {
            player_state: json!("UNKNOWN"),
            current_time: json!(0.0),
            duration: Value::Null,
            content_id: Value::Null,
            content_type: Value::Null,
            metadata: json!({}),
        }
    }
}

impl MediaState {
    fn idle() -> Self {
        Self {
            player_state: json!("IDLE"),
            current_time: Value::Null,
            ..Self::default()
        }
    }

    fn values(&self) -> Vec<(&'static str, Value)> {
        let image = self
            .metadata
            .get("images")
            .and_then(Value::as_array)
            .and_then(|images| images.iter().find_map(|image| image.get("url")))
            .cloned()
            .unwrap_or(Value::Null);
        vec![
            ("player_state", self.player_state.clone()),
            ("player_position", self.current_time.clone()),
            ("player_duration", self.duration.clone()),
            (
                "media/title",
                field(Some(&self.metadata), "title", Value::Null),
            ),
            (
                "media/album_name",
                field(Some(&self.metadata), "albumName", Value::Null),
            ),
            (
                "media/artist",
                field(Some(&self.metadata), "artist", Value::Null),
            ),
            (
                "media/album_artist",
                field(Some(&self.metadata), "albumArtist", Value::Null),
            ),
            (
                "media/track",
                field(Some(&self.metadata), "track", Value::Null),
            ),
            ("media/images", image),
            ("media/content_type", self.content_type.clone()),
            ("media/content_url", self.content_id.clone()),
        ]
    }
}

fn field(object: Option<&Value>, key: &str, default: Value) -> Value {
    object
        .and_then(|object| object.get(key))
        .cloned()
        .unwrap_or(default)
}

/// Python's generic MQTT conversion: every float in 0..=1 is scaled by 100,
/// including timestamps. Integer JSON numbers deliberately bypass that rule.
pub fn serialize_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => if *value { "1" } else { "0" }.into(),
        Value::String(value) => value.clone(),
        Value::Number(number) if number.is_f64() => {
            let mut value = number.as_f64().expect("JSON float is finite");
            if (0.0..=1.0).contains(&value) {
                value *= 100.0;
            }
            let rounded = value.round_ties_even();
            if rounded == 0.0 {
                "0".into()
            } else {
                format!("{rounded:.0}")
            }
        }
        Value::Number(number) => number.to_string(),
        // Python uses str(), not JSON encoding, for container-valued fields.
        Value::Array(_) | Value::Object(_) => python_repr(value),
    }
}

fn python_repr(value: &Value) -> String {
    match value {
        Value::Null => "None".into(),
        Value::Bool(value) => if *value { "True" } else { "False" }.into(),
        Value::Number(number) => number.to_string(),
        Value::String(value) => {
            let quote = if value.contains('\'') && !value.contains('"') {
                '"'
            } else {
                '\''
            };
            let mut result = String::from(quote);
            for ch in value.chars() {
                match ch {
                    '\\' => result.push_str("\\\\"),
                    '\n' => result.push_str("\\n"),
                    '\r' => result.push_str("\\r"),
                    '\t' => result.push_str("\\t"),
                    ch if ch == quote => {
                        result.push('\\');
                        result.push(ch);
                    }
                    ch if ch.is_control() => {
                        let code = u32::from(ch);
                        if code <= 0xff {
                            result.push_str(&format!("\\x{code:02x}"));
                        } else {
                            result.push_str(&format!("\\u{code:04x}"));
                        }
                    }
                    ch => result.push(ch),
                }
            }
            result.push(quote);
            result
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(python_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{}: {}",
                        python_repr(&Value::String(key.clone())),
                        python_repr(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(cache: &StatusCache) -> BTreeMap<String, String> {
        cache.snapshot().into_iter().collect()
    }

    fn media(cache: &mut StatusCache, status: Value) -> Vec<(String, String)> {
        cache.apply(
            MEDIA_NAMESPACE,
            &json!({"type": "MEDIA_STATUS", "status": [status]}),
        )
    }

    fn receiver(cache: &mut StatusCache, status: Value) -> Vec<(String, String)> {
        cache.apply(
            RECEIVER_NAMESPACE,
            &json!({"type": "RECEIVER_STATUS", "status": status}),
        )
    }

    fn playing() -> Value {
        json!({
            "playerState": "PLAYING", "currentTime": 10.5,
            "media": {
                "duration": 120.5, "contentId": "https://example.test/song",
                "contentType": "audio/mpeg", "metadata": {
                    "title": "Song", "albumName": "Album", "artist": "Artist",
                    "albumArtist": "Album artist", "track": 1,
                    "images": [{"height": 100}, {"url": "https://example.test/first"},
                               {"url": "https://example.test/second"}]
                }
            }
        })
    }

    #[test]
    fn new_and_default_start_without_published_state() {
        assert!(StatusCache::new().snapshot().is_empty());
        assert!(StatusCache::default().snapshot().is_empty());
    }

    #[test]
    fn set_deduplicates_and_snapshot_is_sorted_and_non_destructive() {
        let mut cache = StatusCache::new();
        assert_eq!(
            cache.set("model_name", "Model"),
            vec![("model_name".into(), "Model".into())]
        );
        cache.set("address".to_owned(), "127.0.0.1:8009".to_owned());
        cache.set("media/title", "");
        let expected = vec![
            ("address".into(), "127.0.0.1:8009".into()),
            ("media/title".into(), "".into()),
            ("model_name".into(), "Model".into()),
        ];
        assert_eq!(cache.snapshot(), expected);
        assert_eq!(cache.snapshot(), expected);
        assert!(cache.set("model_name", "Model").is_empty());
        assert_eq!(cache.set("model_name", "Other").len(), 1);
    }

    #[test]
    fn generic_serialization_preserves_integers_and_strings() {
        for (value, expected) in [
            (Value::Null, ""),
            (json!(true), "1"),
            (json!(false), "0"),
            (json!(0), "0"),
            (json!(1), "1"),
            (json!(-1), "-1"),
            (json!(u64::MAX), "18446744073709551615"),
            (json!("001"), "001"),
            (json!(" café \n"), " café \n"),
        ] {
            assert_eq!(serialize_value(&value), expected, "{value}");
        }
    }

    #[test]
    fn floats_scale_inclusive_unit_interval_and_round_ties_even() {
        for (value, expected) in [
            (0.0, "0"),
            (-0.0, "0"),
            (0.35, "35"),
            (0.75, "75"),
            (1.0, "100"),
            (1.2, "1"),
            (2.5, "2"),
            (3.5, "4"),
            (0.125, "12"),
            (-0.5, "0"),
            (-1.5, "-2"),
            (1e20, "100000000000000000000"),
        ] {
            assert_eq!(serialize_value(&json!(value)), expected, "{value}");
        }
    }

    #[test]
    fn container_values_use_python_style_not_json() {
        assert_eq!(
            serialize_value(&json!([true, null, 0.5, "x"])),
            "[True, None, 0.5, 'x']"
        );
        assert_eq!(serialize_value(&json!({"x": false})), "{'x': False}");
        assert_eq!(
            serialize_value(&json!(["it's", "a\nb", "\\"])),
            "[\"it's\", 'a\\nb', '\\\\']"
        );
    }

    #[test]
    fn receiver_publishes_receiver_volume_not_media_volume() {
        let mut cache = StatusCache::new();
        let updates = receiver(
            &mut cache,
            json!({
                "volume": {"level": 0.35, "muted": true},
                "applications": [{"appId": "active", "displayName": "Music"}]
            }),
        );
        assert_eq!(
            updates,
            vec![
                ("current_app".into(), "Music".into()),
                ("volume_level".into(), "35".into()),
                ("volume_muted".into(), "1".into()),
                ("connection_status".into(), "CONNECTED".into()),
            ]
        );
        media(
            &mut cache,
            json!({"volume": {"level": 0.99, "muted": false}}),
        );
        assert_eq!(state(&cache)["volume_level"], "35");
        assert_eq!(state(&cache)["volume_muted"], "1");
    }

    #[test]
    fn empty_receiver_uses_library_defaults_and_idle_override() {
        let mut cache = StatusCache::new();
        receiver(&mut cache, json!({}));
        let snapshot = state(&cache);
        assert_eq!(snapshot["current_app"], "");
        assert_eq!(snapshot["volume_level"], "100");
        assert_eq!(snapshot["volume_muted"], "0");
        assert_eq!(snapshot["connection_status"], "CONNECTED");
        assert_eq!(snapshot["player_state"], "IDLE");
        assert_eq!(snapshot["player_position"], "");
        assert_eq!(snapshot["player_duration"], "");
        assert!(!snapshot.contains_key("media/title"));
    }

    #[test]
    fn receiver_messages_are_not_merged_like_media_messages() {
        let mut cache = StatusCache::new();
        receiver(&mut cache, json!({"volume": {"level": 0.2, "muted": true}}));
        receiver(&mut cache, json!({"applications": [{"appId": "active"}]}));
        assert_eq!(state(&cache)["volume_level"], "100");
        assert_eq!(state(&cache)["volume_muted"], "0");
    }

    #[test]
    fn media_metadata_fields_and_first_image_map_to_exact_suffixes() {
        let mut cache = StatusCache::new();
        let updates = media(&mut cache, playing());
        assert_eq!(
            updates,
            vec![
                ("player_state".into(), "PLAYING".into()),
                ("player_position".into(), "10".into()),
                ("player_duration".into(), "120".into()),
                ("media/title".into(), "Song".into()),
                ("media/album_name".into(), "Album".into()),
                ("media/artist".into(), "Artist".into()),
                ("media/album_artist".into(), "Album artist".into()),
                ("media/track".into(), "1".into()),
                ("media/images".into(), "https://example.test/first".into()),
                ("media/content_type".into(), "audio/mpeg".into()),
                (
                    "media/content_url".into(),
                    "https://example.test/song".into()
                ),
            ]
        );
        assert!(media(&mut cache, playing()).is_empty());
    }

    #[test]
    fn fresh_media_status_uses_library_defaults() {
        let mut cache = StatusCache::new();
        media(&mut cache, json!({}));
        let snapshot = state(&cache);
        assert_eq!(snapshot["player_state"], "UNKNOWN");
        assert_eq!(snapshot["player_position"], "0");
        for suffix in [
            "player_duration",
            "media/title",
            "media/images",
            "media/content_url",
        ] {
            assert_eq!(snapshot[suffix], "");
        }
    }

    #[test]
    fn partial_media_status_preserves_metadata_and_media_information() {
        let mut cache = StatusCache::new();
        media(&mut cache, playing());
        assert_eq!(
            media(&mut cache, json!({"currentTime": 12.4})),
            vec![("player_position".into(), "12".into()),]
        );
        assert_eq!(
            media(&mut cache, json!({"media": {"duration": 200}})),
            vec![("player_duration".into(), "200".into()),]
        );
        let snapshot = state(&cache);
        assert_eq!(snapshot["media/title"], "Song");
        assert_eq!(snapshot["media/images"], "https://example.test/first");
        assert_eq!(snapshot["media/content_type"], "audio/mpeg");
        assert_eq!(snapshot["media/content_url"], "https://example.test/song");
        assert_eq!(snapshot["player_state"], "PLAYING");
    }

    #[test]
    fn supplied_metadata_replaces_instead_of_deep_merging() {
        let mut cache = StatusCache::new();
        media(&mut cache, playing());
        media(&mut cache, json!({"media": {"metadata": {"title": "New"}}}));
        assert_eq!(state(&cache)["media/title"], "New");
        assert_eq!(state(&cache)["media/artist"], "");
        assert_eq!(state(&cache)["media/images"], "");
        assert_eq!(state(&cache)["media/content_type"], "audio/mpeg");
        media(&mut cache, json!({"media": {"metadata": {}}}));
        assert_eq!(state(&cache)["media/title"], "");
    }

    #[test]
    fn explicit_nulls_clear_values_instead_of_preserving_them() {
        let mut cache = StatusCache::new();
        media(&mut cache, playing());
        media(
            &mut cache,
            json!({
                "currentTime": null,
                "media": {"duration": null, "contentId": null, "contentType": null, "metadata": null}
            }),
        );
        for suffix in [
            "player_position",
            "player_duration",
            "media/content_url",
            "media/content_type",
            "media/title",
        ] {
            assert_eq!(state(&cache)[suffix], "");
        }
    }

    #[test]
    fn first_image_url_key_wins_even_if_empty_or_null() {
        for url in [Value::Null, json!(""), json!("first")] {
            let mut cache = StatusCache::new();
            media(
                &mut cache,
                json!({"media": {"metadata": {"images": [
                    {"width": 100}, {"url": url}, {"url": "second"}
                ]}}}),
            );
            assert_eq!(state(&cache)["media/images"], serialize_value(&url));
        }
        let mut cache = StatusCache::new();
        media(
            &mut cache,
            json!({"media": {"metadata": {"images": [{"width": 100}]}}}),
        );
        assert_eq!(state(&cache)["media/images"], "");
    }

    #[test]
    fn timestamps_have_the_same_float_scaling_bug_as_volume() {
        let mut cache = StatusCache::new();
        media(
            &mut cache,
            json!({"currentTime": 0.75, "media": {"duration": 1.0}}),
        );
        assert_eq!(state(&cache)["player_position"], "75");
        assert_eq!(state(&cache)["player_duration"], "100");
        media(
            &mut cache,
            json!({"currentTime": 1, "media": {"duration": 1}}),
        );
        assert_eq!(state(&cache)["player_position"], "1");
        assert_eq!(state(&cache)["player_duration"], "1");
    }

    #[test]
    fn track_uses_track_not_track_number_and_generic_serialization() {
        let mut cache = StatusCache::new();
        media(
            &mut cache,
            json!({"media": {"metadata": {"trackNumber": 7}}}),
        );
        assert_eq!(state(&cache)["media/track"], "");
        media(&mut cache, json!({"media": {"metadata": {"track": 1.0}}}));
        assert_eq!(state(&cache)["media/track"], "100");
    }

    #[test]
    fn receiver_idle_clears_only_published_player_fields() {
        for app in [
            json!({}),
            json!({"appId": null}),
            json!({"appId": IDLE_APP_ID}),
        ] {
            let mut cache = StatusCache::new();
            media(&mut cache, playing());
            receiver(&mut cache, json!({"applications": [app]}));
            assert_eq!(state(&cache)["player_state"], "IDLE");
            assert_eq!(state(&cache)["player_position"], "");
            assert_eq!(state(&cache)["player_duration"], "");
            assert_eq!(state(&cache)["media/title"], "Song");
            assert_eq!(
                state(&cache)["media/content_url"],
                "https://example.test/song"
            );
            // A later partial media callback can restore its retained underlying state.
            media(&mut cache, json!({"currentTime": 20}));
            assert_eq!(state(&cache)["player_state"], "PLAYING");
            assert_eq!(state(&cache)["player_duration"], "120");
        }
    }

    #[test]
    fn empty_media_status_resets_session_and_clears_metadata() {
        let mut cache = StatusCache::new();
        receiver(&mut cache, json!({"volume": {"level": 0.35}}));
        media(&mut cache, playing());
        let empty = json!({"type": "MEDIA_STATUS", "status": []});
        cache.apply(MEDIA_NAMESPACE, &empty);
        let snapshot = state(&cache);
        assert_eq!(snapshot["player_state"], "IDLE");
        for suffix in [
            "player_position",
            "player_duration",
            "media/title",
            "media/images",
            "media/content_type",
            "media/content_url",
        ] {
            assert_eq!(snapshot[suffix], "");
        }
        assert_eq!(snapshot["volume_level"], "35");
        assert_eq!(snapshot["connection_status"], "CONNECTED");
        assert!(cache.apply(MEDIA_NAMESPACE, &empty).is_empty());
        media(&mut cache, json!({"playerState": "PLAYING"}));
        assert_eq!(state(&cache)["media/title"], "");
        assert_eq!(state(&cache)["media/content_url"], "");
    }

    #[test]
    fn extended_status_media_is_a_fallback_only() {
        let mut cache = StatusCache::new();
        media(
            &mut cache,
            json!({"media": {}, "extendedStatus": {"media": {
                "contentId": "extended", "metadata": {"title": "Loading"}
            }}}),
        );
        assert_eq!(state(&cache)["media/title"], "Loading");
        assert_eq!(state(&cache)["media/content_url"], "extended");
        media(
            &mut cache,
            json!({
                "media": {"contentId": "normal"},
                "extendedStatus": {"media": {"contentId": "ignored"}}
            }),
        );
        assert_eq!(state(&cache)["media/content_url"], "normal");
        assert_eq!(state(&cache)["media/title"], "Loading");
    }

    #[test]
    fn only_first_app_and_first_media_status_are_used() {
        let mut cache = StatusCache::new();
        receiver(
            &mut cache,
            json!({"applications": [
                {"appId": "active", "displayName": "First"}, {"displayName": "Second"}
            ]}),
        );
        assert_eq!(state(&cache)["current_app"], "First");
        cache.apply(
            MEDIA_NAMESPACE,
            &json!({"status": [
                {"currentTime": 20}, {"currentTime": 99}
            ]}),
        );
        assert_eq!(state(&cache)["player_position"], "20");
    }

    #[test]
    fn deduplication_compares_serialized_values_not_input_types() {
        let mut cache = StatusCache::new();
        media(&mut cache, json!({"currentTime": 12.2}));
        assert!(media(&mut cache, json!({"currentTime": 12.4})).is_empty());
        assert!(media(&mut cache, json!({"currentTime": 12})).is_empty());
        assert_eq!(
            media(&mut cache, json!({"currentTime": 13})),
            vec![("player_position".into(), "13".into()),]
        );
    }

    #[test]
    fn unknown_namespaces_and_non_status_messages_are_ignored() {
        let mut cache = StatusCache::new();
        assert!(cache.apply("unknown", &json!({"status": []})).is_empty());
        for payload in [
            json!({}),
            json!({"type": "LOAD_FAILED"}),
            json!({"status": null}),
        ] {
            assert!(cache.apply(RECEIVER_NAMESPACE, &payload).is_empty());
            assert!(cache.apply(MEDIA_NAMESPACE, &payload).is_empty());
        }
        assert!(cache
            .apply(RECEIVER_NAMESPACE, &json!({"status": []}))
            .is_empty());
        assert!(cache
            .apply(MEDIA_NAMESPACE, &json!({"status": [null]}))
            .is_empty());
        assert!(cache.snapshot().is_empty());
    }
}
