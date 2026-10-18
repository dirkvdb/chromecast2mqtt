# chromecast2mqtt

A Rust MQTT bridge for Google Cast devices, using [`rust_cast`](https://github.com/azasypkin/rust-cast) for Cast commands and [`mdns-sd`](https://github.com/keepsimple1/mdns-sd) for continuous discovery.

`rust_cast` 0.21 was selected for its established MIT-licensed implementation and maintenance history (latest release: December 2025); it has a synchronous API, isolated in one worker per device. The bridge does not promise broader PyChromecast features than that crate provides.

The native dependency uses a narrow [compatibility patch](patches/rust_cast/README.md): inbound reply IDs can exceed 32 bits, and deserialization errors identify the failing JSON field. Only the patch is tracked—not the full crate. The build preparation step verifies the pinned upstream archive and applies the patch under ignored `.build/rust_cast`; outgoing operations remain library-native.

The scope is **the native operations exposed by `rust_cast`**, plus a small compatibility interface for `chromecast-mqtt-connector`. Missing library features are not recreated with handcrafted Cast messages. This is not a complete PyChromecast replacement, a raw Cast-protocol tunnel, or a Home Assistant discovery integration. Hardware interoperability still needs validation; unit tests are not a claim of device certification.

## Build and run

Use Rust 1.97 for the documented build/container setup. A C/C++ compiler, CMake, make, and Perl are needed for the AWS-LC dependency pulled in by `rust_cast`.

```sh
# Debian/Ubuntu build prerequisites
sudo apt-get update
sudo apt-get install -y build-essential cmake perl pkg-config ca-certificates curl patch util-linux
sh scripts/prepare-rust-cast.sh
cargo build --locked --release
./target/release/chromecast2mqtt --mqtt-addr 127.0.0.1
```

`just prepare` performs the same preparation step; Just build/run/check/test recipes, Devenv activation, and Docker do it automatically. Run it once before bare Cargo commands or rust-analyzer in a fresh checkout. Build preparation never modifies Cargo's registry cache; an unchanged archive/patch fingerprint is a no-op. Builds expect the committed `Cargo.lock`. Keep the bridge, broker, and Cast devices on networks that can reach each other. mDNS uses multicast UDP 5353; Cast control normally uses TCP 8009 (or the discovered/configured port). Devices must also be able to fetch any media URLs you send them.

To bypass multicast discovery, configure literal IP socket addresses:

```sh
./target/release/chromecast2mqtt --no-discovery \
  --device 'Living Room=192.0.2.10:8009' \
  --device 'Bedroom=[2001:db8::10]:8009'
```

`--device` is repeatable and comma-delimited; hostnames are not accepted. Static devices initially use model `Unknown` and cast type `cast`. With discovery disabled, at least one static device is required. Ctrl-C and SIGTERM shut down the bridge gracefully **without stopping playback**.

## Configuration

CLI flags override environment variables; CLI/environment settings override INI values and defaults. An explicit `--config`/`C2M_CONFIG` selects an INI file; otherwise `config.ini` in the working directory is loaded if present.

| Flag | Environment variable | Default / purpose |
|---|---|---|
| `--mqtt-addr` | `C2M_MQTT_ADDRESS` | `127.0.0.1` |
| `--mqtt-port` | `C2M_MQTT_PORT` | `1883`, including when TLS is enabled |
| `--mqtt-user` | `C2M_MQTT_USER` | No authentication |
| `--mqtt-pass` | `C2M_MQTT_PASS` | SecretSpec fallback when a username is configured and no password was supplied |
| `--mqtt-client-id` | `C2M_CLIENT_ID` | `chromecast2mqtt`; use a unique ID per instance |
| `--mqtt-base-topic` | `C2M_MQTT_BASE_TOPIC` | `chromecast` |
| `--mqtt-tls` | `C2M_MQTT_TLS` | Enable TLS with system roots and hostname verification |
| `--mqtt-ca-cert` | `C2M_MQTT_CA_CERT` | PEM CA bundle; enables TLS with these roots instead of system roots |
| `--mqtt-insecure` | `C2M_MQTT_INSECURE` | Explicitly disable MQTT server certificate/identity validation; enables TLS |
| `--mqtt-client-cert` | `C2M_MQTT_CLIENT_CERT` | PEM mTLS certificate chain; enables TLS |
| `--mqtt-client-key` | `C2M_MQTT_CLIENT_KEY` | PEM mTLS private key; requires the certificate too |
| `--config` | `C2M_CONFIG` | Optional Python-compatible INI file |
| `--no-discovery` | `C2M_NO_DISCOVERY` | Disable continuous mDNS discovery |
| `--device` | `C2M_DEVICES` | Repeat/comma-separated `friendly name=IP:port` entries |

Boolean environment values use `true`/`false`. See `.env.example` for Docker environment settings; the binary does **not** automatically load `.env`.

Compatible INI keys:

```ini
[mqtt]
broker_address = 127.0.0.1
broker_port = 1883
use_auth = false
# username = example
# password = example
```

`use_auth` controls the legacy INI credentials; enabling it requires a nonempty username. A password without a username is rejected rather than silently connecting anonymously. Prefer environment variables or a protected INI file over passwords in command-line arguments. Keep `.env`, credentials, and private keys out of version control.

### SecretSpec

The application resolves missing MQTT credentials directly through the **SecretSpec Rust SDK**. `just run` is plain `cargo run`; no `secretspec run` wrapper or shell-wide password injection is needed. [`secretspec.toml`](secretspec.toml) is embedded in the binary and declares `C2M_MQTT_PASS` in both `default` and `development` profiles. `SECRETSPEC_FILE` can select an external manifest.

#### Development: desktop keyring

Devenv's SecretSpec integration selects the desktop `keyring` provider and `development` profile. On interactive activation, it prompts for missing required secrets and stores them in your desktop's secret service (on Linux) or native keychain:

```sh
# Prompts securely for C2M_MQTT_PASS if it is missing, then saves it in the keyring.
devenv shell
just secrets-check
just run --mqtt-addr mqtt.example.net --mqtt-user chromecast2mqtt --mqtt-port 8883 --mqtt-tls
```

The SDK reads from the desktop provider when the bridge starts. A desktop secret service and an accessible/unlocked session keyring must be available. Devenv exports the selected provider/profile, but this project does not map resolved secret values into the shell environment. Provider/profile CLI or environment overrides apply consistently to both Devenv and the SDK.

`just secrets-check` deliberately uses `--no-prompt`: it reports missing credentials, while interactive `devenv shell` handles initial provisioning. After changing this configuration, re-enter/reload Devenv to trigger that provisioning. You can also provision or update the selected credential explicitly with `secretspec set C2M_MQTT_PASS`. Noninteractive Devenv activation fails when required secrets are missing; provision them beforehand. Plain Cargo/Just build/check/test commands outside Devenv do not require stored credentials.

Keep non-secret settings such as the broker address and `C2M_MQTT_USER` in CLI arguments or your existing `.env` configuration. Remove `C2M_MQTT_PASS` from development `.env` files so the SDK uses the desktop provider instead. Store credentials under the selected profile; `development` inherits the declaration from `default`, not a promise to share stored values between profiles.

#### Docker: environment provider

The image sets `SECRETSPEC_PROVIDER=env` and `SECRETSPEC_PROFILE=default`. Supply `C2M_MQTT_USER` and `C2M_MQTT_PASS` through your deployment environment or Docker environment file; no desktop keyring or SecretSpec CLI is needed in the container:

```sh
docker run --rm --network host --env-file .env chromecast2mqtt:local
```

To forward a password already supplied to your host environment by a deployment secret manager, use `--env C2M_MQTT_PASS` rather than putting its value on the command line. Mount TLS certificate files as described below. Do not bake credentials into the image or commit environment files.

Explicit `--mqtt-pass` / `C2M_MQTT_PASS` values (including an explicitly empty password), or an existing nonempty INI password, bypass SDK resolution and retain their existing precedence. The SDK is consulted only when a username is configured and no password was supplied; anonymous MQTT does not access a secret provider. Missing credentials/provider failures fail startup without prompting and never silently fall back to anonymous authentication. `--help` and `--version` do not access providers. Outside Devenv/Docker, provider/profile selection follows SecretSpec's configuration and `SECRETSPEC_PROVIDER` / `SECRETSPEC_PROFILE`.

If you used the old template, provision the secret under project `chromecast2mqtt` with name `C2M_MQTT_PASS` in the selected profile; the former `energy-display` / `MQTT_PASSWORD` entry is not automatically migrated or deleted. Avoid `secretspec get`/`export` in logs because they can print secret values.

### MQTT TLS

TLS never changes the port implicitly: select `--mqtt-port 8883` explicitly if your broker uses it. Client certificate and key must be supplied together. Custom CA and insecure mode are mutually exclusive.

```sh
./target/release/chromecast2mqtt --mqtt-addr mqtt.example.net --mqtt-port 8883 --mqtt-tls
./target/release/chromecast2mqtt --mqtt-addr mqtt.example.net --mqtt-port 8883 \
  --mqtt-ca-cert /certs/ca.pem --mqtt-client-cert /certs/client.pem --mqtt-client-key /certs/client.key
```

Default MQTT transport is **plaintext MQTT 3.1.1**. `--mqtt-insecure` is for explicit, trusted-network testing only and permits man-in-the-middle attacks. Cast-device TLS is separate: devices present self-signed certificates, so their identity is not validated. Enabling verified MQTT TLS does not authenticate Cast devices.

## MQTT interface

State topics are rooted at `<base>/<friendly name>/`; the default base is `chromecast`. Friendly names retain their exact spaces, case, and Unicode—no slugification. Names must fit a single valid MQTT topic level; empty names and names containing `/`, `+`, `#`, or NUL are rejected/skipped. Use unique friendly names.

```text
chromecast/<friendly name>/
  friendly_name
  model_name
  address
  connection_status
  cast_type
  current_app
  volume_level
  volume_muted
  player_state
  player_position
  player_duration
  media/title
  media/album_name
  media/artist
  media/album_artist
  media/track
  media/images
  media/content_type
  media/content_url
```

Publications and command subscriptions use **QoS 0**. State publications are **retained**, deduplicated by serialized value, and replayed from the known-state cache on broker reconnect. Empty retained payloads clear the broker's retained value. There is no Last Will, per-command reply topic, or Home Assistant auto-discovery. Retained incoming commands are processed again on subscription/reconnect: **do not retain commands unless replay is intentional**.

State is the best available projection of `rust_cast`'s typed receiver/media fields, not every field sent by every app. In particular, `media/images` contains only the first image URL, `media/track` can be empty because the legacy mapper expects `track` rather than native `trackNumber`, and time/volume serialization preserves a Python float-conversion quirk. See [compatibility and native schemas](docs/compatibility.md) before relying on exact payloads.

### Legacy commands

Publish to `<base>/<friendly name>/command/<property>`:

| Property | Payload |
|---|---|
| `volume_level` | Absolute percentage `55`, or relative `+5` / `-5` |
| `volume_muted` | Exactly `0` or `1` |
| `player_position` | Absolute seconds; numeric fractions are truncated |
| `player_state` | Exact uppercase `RESUME`, `PAUSE`, `STOP`, or a media load |

Media loads accept a plain URL (MIME guessed locally), a two-element JSON array, or a JSON object:

```text
https://example.net/music.mp3
["https://example.net/music.mp3", "audio/mpeg"]
{"url":"https://example.net/music.mp3","content_type":"audio/mpeg","title":"Music"}
```

An object must include both `url` and `content_type`; the latter may be null. Unknown MIME ultimately falls back to `video/mp4` in the backend, so explicit MIME is recommended. Supported optional fields include `title`, `thumb`, `current_time`, `autoplay`, `stream_type`, and typed `metadata`; see the compatibility guide for restrictions.

**Explicitly unsupported:** `SKIP`, `REWIND`, `PREV`, `NEXT`, legacy `enqueue: true`, subtitle tracks, and nondefault subtitle configuration. These are rejected rather than simulated. `STOP` stops media; native `stop_app` stops the receiver app. Native seek exists, but no legacy rewind/skip emulation is provided.

### Native `rust_cast` commands

Publish a JSON object to `<base>/<friendly name>/command/rust_cast`:

```json
{"op":"set_volume","level":0.35,"muted":false}
```

```json
{"op":"load","media":{"contentId":"https://example.net/movie.mp4","contentType":"video/mp4","streamType":"BUFFERED","metadata":{"metadataType":0,"title":"Movie"}},"options":{"currentTime":0,"autoplay":true}}
```

All exposed operations: `get_receiver_status`, `launch_app`, `stop_app`, `set_volume`, `broadcast_message`, `get_media_status`, `load`, `load_queue`, `load_with_queue`, `play`, `pause`, `stop`, `seek`, `connect`, `disconnect`, `ping`, `pong`.

`load_with_opts` is also accepted as an alias for `load`; `load_with_queue` accepts optional load `options`. Outer arguments are snake_case; nested media, queue, options, and metadata DTOs are camelCase. Native volume uses **0–1**, not the legacy 0–100 scale. The [complete schema and examples](docs/compatibility.md#native-command-schema) distinguish native queue loading from unsupported legacy enqueue.

## Docker

```sh
docker build -t chromecast2mqtt:local .
cp .env.example .env
# Edit .env for your broker/network; do not commit credentials.
docker run --rm --name chromecast2mqtt --network host --env-file .env chromecast2mqtt:local
```

Linux host networking is the simplest way to preserve mDNS multicast; ordinary Docker bridge port mappings are not an mDNS relay. On other platforms, check host-network/multicast support or use static `--device` entries with `--no-discovery`. Allow UDP 5353 multicast and outbound Cast/broker connections through firewalls. Do not publish TCP 8009 from this container: it is a client of the device's port.

For a custom MQTT CA, mount certificates read-only and use container paths:

```sh
docker run --rm --network host --env-file .env \
  --mount type=bind,src=/absolute/path/to/certs,dst=/certs,readonly \
  chromecast2mqtt:local --mqtt-port 8883 --mqtt-ca-cert /certs/ca.pem
```

The Debian Bookworm runtime runs as UID/GID `10001:10001` and includes system CA certificates. Mounted certificates/keys must be readable by that UID; keep private keys protected. Docker stop sends SIGTERM. No playback-stop command is sent during shutdown.

## Development and licensing

```sh
just check       # formatting check, Clippy, nextest, doctests
just test-cargo  # fallback when cargo-nextest is unavailable
just build      # cargo build --locked --release
```

Install `just`, `cargo-nextest`, and the Rust `rustfmt`/`clippy` components first. CI checks formatting, linting, tests, and a non-publishing Docker build. The separate Docker workflow publishes to GHCR only on `v*` tags or explicit manual dispatch; tag builds publish the Git tag and SHA, manual builds publish `latest` and SHA. No hardware validation is implied by CI. A [manual test checklist](docs/compatibility.md#hardware-validation-checklist) covers that separate step.

New Rust code is MIT-licensed; see [LICENSE](LICENSE). The external Python `chromecast-mqtt-connector` is an API/behavior reference under **MPL-2.0**, not GPL-2.0; its source is not copied or relicensed here. Dependencies retain their own licenses: `rust_cast` is MIT and `mdns-sd` is Apache-2.0 OR MIT.
