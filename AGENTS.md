# Contributor and agent guidance

## Scope and architecture

- This is an independent Rust implementation. The external Python
  `chromecast-mqtt-connector` is a behavior/API reference under MPL-2.0, not code to
  copy into this MIT-licensed project.
- `src/command.rs`: legacy command parsing and internally tagged `NativeCommand`.
- `src/cast/mod.rs`: `rust_cast` execution, typed DTO conversion, lifecycle and status projection.
- `src/status.rs`: partial-media merging, Python-compatible serialization and deduplication.
- `src/discovery.rs`: continuous mDNS resolution/removal and device information.
- `src/mqtt.rs`, `src/tls.rs`: MQTT transport, routing, credentials and TLS policy.
- `src/bridge.rs`: orchestration, device retirement, publication coalescing and reconnect replay.

## Non-negotiable compatibility/security boundaries

- Expose native `rust_cast` methods. Do not recreate unsupported functionality
  with handcrafted Cast JSON/protobuf or infer undocumented protocol operations.
- Legacy SKIP/REWIND/PREV/NEXT, enqueue and subtitle support are explicitly out of
  scope. Native queue loading is not legacy append/enqueue or navigation.
- Keep outer native operation fields snake_case and nested DTO fields camelCase.
  Reject unknown/unsupported fields rather than dropping user intent silently.
- Friendly names are literal MQTT topic levels; do not slugify or normalize them.
- Preserve QoS 0, retained/deduplicated state, snapshot replay, and the documented
  generic float conversion unless an explicit compatibility change is requested.
- Never stop media playback as a side effect of bridge shutdown or device retirement.
- MQTT TLS uses verified roots/identity unless insecure mode is explicitly selected.
  Cast-device self-signed TLS is a separate policy, not a reason to weaken MQTT TLS.
- Do not log passwords, private keys, or full configuration structs containing secrets.
  Do not commit .env, config.ini credentials, certificate private keys, or local captures.

## Changes and validation

- Respect the task's file ownership boundaries; other contributors/agents may be
  implementing adjacent modules concurrently. Do not format or edit their files
  as an incidental cleanup, or change Cargo/devenv files without authorization.
- Keep command/state regression tests close to their modules. Test schema errors,
  partial updates, float/int distinctions, deduplication and lifecycle boundaries.
- Use `just check` for formatting, strict Clippy, nextest and doctests.
  `just test-cargo` is the test fallback without nextest. `just build` is a locked
  release build. Do not bypass a missing/outdated lockfile with an unlocked CI build.
- `just prepare` applies the tracked rust_cast patch to checksum-verified upstream
  source in ignored `.build/rust_cast`; Just recipes and Devenv prepare it automatically.
  Run preparation before bare Cargo commands in a fresh checkout. Edit the patch,
  not generated dependency source, and never patch Cargo's shared registry cache.
- The container build uses Rust 1.97 and requires C/C++, make, CMake and Perl for
  transitive AWS-LC compilation. Runtime is unprivileged Debian with CA roots.
- Keep README/compatibility schemas and .env.example synchronized with CLI and DTO
  changes. Do not claim hardware, container networking, or broker TLS validation
  unless it was actually performed; use docs/compatibility.md's hardware checklist.
- Explain intentional Python differences and any validation that could not run.
