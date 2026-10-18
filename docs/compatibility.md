# Compatibility, native schemas, and validation

This is an independently written Rust bridge with a Python-compatible topic tree and a deliberately limited legacy command interface. The execution backend delegates to `rust_cast` 0.21; it does not implement missing Cast features by constructing protocol messages. Source contracts live in [`command.rs`](../src/command.rs), [`cast/mod.rs`](../src/cast/mod.rs), and [`status.rs`](../src/status.rs).

## Compatibility boundary

| Area | Contract / difference from the Python connector |
|---|---|
| Topics | Default `chromecast/<literal friendly name>/...`; configurable root. No case/space/Unicode normalization. Invalid single-level MQTT names are rejected/skipped rather than creating broken topics. |
| Identity | Friendly names must be unique. Duplicate services with the same name keep the existing device. |
| Delivery | MQTT 3.1.1; QoS 0 state and subscriptions; retained state; no Last Will or command reply topic. Retained commands execute normally. |
| Reconnect | Known-state snapshot replay (including retired-device disconnection tombstones) and latest-value-per-topic publication coalescing, rather than Python's FIFO/no-full-replay behavior. No delivery guarantee at QoS 0. |
| Discovery | Continuous `_googlecast._tcp.local.` browsing with updates/removals; deterministic IPv4 preference, scoped IPv6 fallback. Discovery name fallback is the service fullname; missing model is empty. Cast type is inferred from model/capabilities, not queried through PyChromecast. |
| Device changes | Address/name changes retire the old actor. Connected actors survive temporary mDNS removal; disconnected actors can be removed. Old retained metadata is not globally deleted on rename/removal. |
| Unknown target | Commands do not create a new friendly-name lookup connection. Discover the device or configure `--device`. |
| Static devices | IP `SocketAddr` only, no DNS hostnames; initial model `Unknown`, cast type `cast`. |
| Shutdown | Ctrl-C/SIGTERM retire connections without media STOP. `DISCONNECTED` publication is best effort; process crashes have no Last Will. |
| Scope | No Home Assistant discovery, app-specific PyChromecast controllers, arbitrary raw-frame tunnel, or simulated queue navigation/subtitles. |

## Legacy command details

The four properties under `command/` are `volume_level`, `volume_muted`, `player_position`, and `player_state`. UTF-8 text is trimmed first; empty payloads are ignored.

- Volume and position use finite floating-point parsing followed by truncation toward zero. Volume `55.9` means 55%; `+5.9` means +5 percentage points; `-5` is relative. The backend clamps effective volume to 0–1. Position is absolute, including a leading `+`; negative positions are rejected by the backend. Seek resumes playback through `PLAYBACK_START`.
- Mute accepts only `0`/`1`; other values are ignored.
- `RESUME`, `PAUSE`, and `STOP` are case-sensitive. `SKIP`, `REWIND`, `PREV`, and `NEXT` return explicit unsupported errors. Other nonempty text, including lowercase keywords, follows the legacy URL path.
- A plain URL uses `mime_guess`; no HTTP lookup or URL query cleanup occurs. MIME maps need not match Python/system `mimetypes`. Unknown MIME/null MIME falls back to `video/mp4` during legacy conversion.
- An array must be exactly `[string URL, string-or-null MIME]`.
- An object requires `url: string` and a present `content_type: string|null`. Unknown fields and invalid types are errors. JSON scalars/quoted URLs are not decoded as a separate media format.

Legacy object options:

| Field | Default / allowed behavior |
|---|---|
| `title`, `thumb` | Null; supplied title overrides metadata title, thumbnail appends an image URL |
| `current_time` | Null becomes 0 seconds in the backend; otherwise finite, nonnegative f64 |
| `autoplay` | True; null also selects the default |
| `stream_type` | `BUFFERED`; null selects default; supported values `NONE`, `BUFFERED`, `LIVE` |
| `metadata` | Null or the typed metadata object described below, not arbitrary PyChromecast custom data |
| `enqueue` | Only absent, false, or null; true is unsupported |
| `subtitles` | Only absent/null; even an empty string is unsupported |
| `subtitles_lang` | Only absent, `en-US`, or null |
| `subtitles_mime` | Only absent, `text/vtt`, or null |
| `subtitle_id` | Only absent, integer 1, or null |

Unsupported options are rejected, not silently ignored. Providing artist/album fields requires music metadata (`metadataType: 3`); the default generic type cannot hold those fields.

## Native command schema

Topic: `<base>/<friendly name>/command/rust_cast`.

Every payload is an object internally tagged by `op`, using the exact snake_case operation names below. Unknown fields are rejected, including for zero-argument operations. Optional outer fields may be omitted or null. Nested DTOs are validated separately by the backend; they are **not arbitrary Cast message bodies**.

| `op` | Other top-level fields | Native operation / behavior |
|---|---|---|
| `get_receiver_status` | None | Receiver status request |
| `launch_app` | `app_id: string` | Launch app, then refresh receiver status |
| `stop_app` | `session_id?: string` | Stop specified or current receiver app session |
| `set_volume` | `level?: number`, `muted?: boolean` | Level is finite f32 in **0–1** |
| `broadcast_message` | `namespace: string`, `message: JSON value` | Library receiver broadcast method; namespace must begin `urn:x-cast:` and contain a name |
| `get_media_status` | None | Request status of current media application |
| `load` (alias `load_with_opts`) | `media: Media`, `options?: LoadOptions` | Library `load`, or `load_with_opts` when options are supplied |
| `load_queue` | `queue: Queue` | Library queue load, **not append/enqueue** |
| `load_with_queue` | `media: Media`, `queue: Queue`, `options?: LoadOptions` | Library combined load, with default options when omitted |
| `play`, `pause`, `stop` | None | Control current media session; stop does not stop the receiver app |
| `seek` | `current_time?: number`, `resume_state?: string` | Time is finite, nonnegative, f32-representable; state is `PLAYBACK_START` or `PLAYBACK_PAUSE` |
| `connect`, `disconnect` | `destination: string` | Cast virtual connection destination, not a device IP and not bridge shutdown |
| `ping`, `pong` | None | Library heartbeat methods |

App IDs, supplied session IDs, and destinations must be nonempty. Native seek keeps fractional seconds, unlike legacy position parsing. Media commands require an appropriate app/session; load operations can start the default media receiver. There is no separate MQTT response stream for arbitrary broadcasts or heartbeat requests.

### Media DTO

Nested keys use **camelCase**, unlike outer `app_id`, `session_id`, and `current_time`:

```json
{
  "op": "load",
  "media": {
    "contentId": "https://example.net/song.mp3",
    "contentType": "audio/mpeg",
    "streamType": "BUFFERED",
    "duration": 180,
    "metadata": {
      "metadataType": 3,
      "title": "Song",
      "artist": "Artist",
      "albumName": "Album",
      "trackNumber": 1,
      "images": [{"url": "https://example.net/cover.jpg", "width": 512, "height": 512}]
    }
  },
  "options": {"currentTime": 0, "autoplay": true}
}
```

Required: nonempty `contentId`, nonempty `contentType`, and `streamType` (`NONE`, `BUFFERED`, `LIVE`). Optional/nullable: `metadata` and `duration`. Duration must be finite, nonnegative, and representable as f32. No additional media fields (`tracks`, `customData`, etc.) are accepted.

`LoadOptions` accepts only `currentTime` (default 0, finite/nonnegative f64) and `autoplay` (default true). Omit/null the entire `options` field to use the library's `load` method. Supplying `{}` uses `load_with_opts` with defaults. Null individual option values are invalid.

### Queue DTO

```json
{
  "op": "load_queue",
  "queue": {
    "items": [
      {"media": {"contentId": "https://example.net/a.mp3", "contentType": "audio/mpeg", "streamType": "BUFFERED"}},
      {"media": {"contentId": "https://example.net/b.mp3", "contentType": "audio/mpeg", "streamType": "BUFFERED"}}
    ],
    "startIndex": 0,
    "queueType": "PLAYLIST"
  }
}
```

All three queue fields are required. `items` must be nonempty; each item accepts only a required `media` DTO. `startIndex` is a u16 integer (0–65535) within the item list. Supported `queueType` values are `ALBUM`, `PLAYLIST`, `AUDIOBOOK`, `RADIO_STATION`, `PODCAST_SERIES`, `TV_SERIES`, `VIDEO_PLAYLIST`, `LIVE_TV`, `MOVIE`. Per-item timing, repeat/shuffle, queue mutation, and next/previous operations are not exposed by these DTOs.

### Metadata DTO

Metadata types 0–3 support `images` (default `[]`); each image has required `url: string`, and optional u32 `width`/`height` that must be supplied together. The crate's photo metadata type does not support images, so type 4 rejects `images` and legacy `thumb`. Unknown fields and fields belonging to a different metadata type are rejected—even when their value is null.

| `metadataType` | Optional fields supported by that type |
|---|---|
| 0 (default), generic | `title`, `subtitle`, `releaseDate` |
| 1, movie | `title`, `subtitle`, `studio`, `releaseDate` |
| 2, TV show | `seriesTitle`, `episodeTitle`, `season`, `episode`, `originalAirDate` |
| 3, music track | `title`, `albumName`, `albumArtist`, `artist`, `composer`, `trackNumber`, `discNumber`, `releaseDate` |
| 4, photo | `title`, `artist`, `location`, `latitude`, `longitude`, `width`, `height`, `creationDateTime` |

Text fields are optional strings. Season, episode, track/disc numbers and dimensions are unsigned 32-bit integers. Photo latitude/longitude are f64 values and must be paired; photo dimensions must also be paired. Unsupported metadata types and arbitrary extension fields are rejected. Date strings are passed through, not interpreted by the bridge. The upstream crate currently encodes TV `episodeTitle` as `subtitle`; the bridge does not work around this with custom wire messages.

## State mapping and limitations

The topic list in the README matches the Python topic tree, but values come only from fields retained by `rust_cast`'s typed response parser. Unknown app extensions cannot be recovered by this bridge; unsupported response shapes can fail to parse. This is not byte-for-byte PyChromecast status compatibility.

| State suffix | Source / caveat |
|---|---|
| `friendly_name`, `model_name`, `address`, `cast_type` | Discovery/static-device information; cast type inference can differ from PyChromecast. Address is formatted as `IP:port`, without IPv6 brackets. |
| `connection_status` | Bridge/worker lifecycle: `WAITING`, `CONNECTING`, `CONNECTED`, `DISCONNECTED`, `ERROR`; receiver status also writes `CONNECTED`. Not Python's entire status/failure-counter vocabulary. |
| `current_app` | First receiver application's display name, not its ID |
| `volume_level`, `volume_muted` | Receiver volume, not media-stream volume; absent receiver values default to 1.0/false |
| `player_state`, `player_position`, `player_duration` | First media status entry; partial updates preserve preceding values. Raw current time, not extrapolated wall-clock position. |
| `media/title`, `album_name`, `artist`, `album_artist` | Typed metadata `title`, `albumName`, `artist`, `albumArtist`; availability depends on metadata type |
| `media/track` | Legacy mapper reads `track`; native typed music metadata emits `trackNumber`, so this topic may remain empty |
| `media/images` | First image with a URL key, not JSON and not an image collection |
| `media/content_type`, `media/content_url` | Typed media `contentType` and `contentId` |

Serialization deliberately preserves the Python generic conversion:

- Null → empty payload; booleans → `1`/`0`; strings unchanged; integer numbers unchanged.
- **Every float in inclusive 0–1 is multiplied by 100**, then rounded ties-to-even. Other floats are rounded ties-to-even without scaling.
- Thus a floating timestamp `0.75` publishes `75`, `1.0` publishes `100`, integer `1` publishes `1`, `2.5` publishes `2`, and `3.5` publishes `4`.
- Native time/volume values use f32 in several library types; precision and integer-versus-float representation can differ from Python's raw JSON. Do not assume subsecond fidelity.

Metadata is preserved if omitted, and replaced wholesale when an object is supplied. Typed metadata may omit null members; partial app updates can therefore leave previous state until an explicit reset. Extended-status media is used as a fallback when ordinary media is absent/empty. Unknown/unmapped fields never get extra MQTT topics.

No app/idle-app (`E8C28D3C`) receiver status writes `IDLE` and empty position/duration, but does not itself erase metadata. A later media update can override those player values. Explicit empty media status (`status: []`) clears the media cache to idle, deliberately improving on Python's stale empty-status behavior. Fresh media defaults are `UNKNOWN`, position 0, and otherwise empty. Topics are emitted as events become available, not as a guaranteed complete startup record.

The backend processes updates and polls status approximately every 30 seconds; this is not a once-per-second position clock. Socket/heartbeat failures reconnect with bounded backoff. Parser/execution errors are logged and may publish `ERROR`; schema errors do not create a command reply. Full queues can drop commands, and QoS 0 is not durable delivery.

## Native response parsing and timeout diagnostics

The build-applied `rust_cast` 0.21 patch accepts `u64` inbound reply IDs while retaining native `u32` outgoing request IDs and lossless correlation. This permits timestamp-sized unsolicited IDs without clamping them or mistaking them for a pending request. Other numeric fields remain strictly typed. Parser errors include field paths (for example, `status[0].supportedMediaCommands`); no full media payload logging is added. See [patch provenance and regression tests](../patches/rust_cast/README.md).

An error such as `invalid value: integer …, expected u32` without a field path does not prove which field overflowed. Likewise, later timeouts on other devices do not establish that one device's parser error caused them.

The worker polls idle sockets every 250 ms for responsiveness. Once the first plaintext Cast bytes arrive, the rest of that frame gets a bounded 10-second receive budget. Frame progress does not keep extending that budget, and synchronous operations keep their own absolute 10-second budget. Partial-frame timeouts still reconnect because retrying the upstream parser after consuming part of a frame would lose framing. Deadline errors identify the phase and bytes consumed; status polls and app attachment add operation context. Increasing timeouts blindly is not a substitute for identifying the failing phase.

## Hardware validation checklist

No real-device interoperability or MQTT TLS/mTLS deployment is certified by the documentation or CI. Before production use, record device model, firmware, receiver app, broker version, network/container setup, and results for:

- [ ] mDNS appearance, address change, rename, duplicate name, removal, reconnect, IPv4/IPv6, and static devices.
- [ ] All state topics against actual receiver/media behavior, especially missing metadata, track number, images, idle/empty status, and float/time conversion.
- [ ] Legacy volume/mute/seek/resume/pause/stop and URL/array/object loads with explicit MIME.
- [ ] Expected rejection of unsupported keywords, enqueue, subtitles, malformed DTOs, and unknown fields.
- [ ] Every native operation, app/session handling, queue load variants, fractional seeks, and broadcast behavior on an appropriate receiver.
- [ ] Broker outage/restart, cached-state replay, retained-command replay risks, queue overload, and unique client IDs.
- [ ] Plaintext MQTT, verified system/custom CA TLS, hostname/CA rejection, mTLS, incomplete key-pair rejection, and explicit insecure-mode warnings.
- [ ] Docker host-network multicast, certificate mount permissions for UID 10001, and static-device operation without discovery.
- [ ] Ctrl-C/SIGTERM during active playback and during reconnect; playback continues, connections close, and shutdown does not hang.

Keep the broker's command ACL restricted to trusted publishers. Neither plaintext MQTT nor self-signed Cast-device TLS supplies authenticated control of the whole path.
