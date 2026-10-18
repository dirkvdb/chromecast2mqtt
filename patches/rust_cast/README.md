# rust_cast compatibility patch

Only the unified patch is tracked, not a vendored crate. `just prepare` runs
`scripts/prepare-rust-cast.sh` to materialize the patched dependency under the
ignored `.build/rust_cast` path used by Cargo's `[patch.crates-io]` override.
`just run`, build/test/check recipes, Devenv activation, and Docker builds prepare
it automatically. Before using bare Cargo commands or rust-analyzer in a fresh
checkout, run `just prepare` once. Do not edit generated source; update the patch.

## Provenance and reproducibility

- Upstream: <https://github.com/azasypkin/rust-cast>, MIT, crate **0.21.0**.
- Packaged source commit: `3a81b6cd1dd4bc52e8b321d8762bad07cdbf10d6`.
- Archive: <https://static.crates.io/crates/rust_cast/rust_cast-0.21.0.crate>.
- SHA-256: `235d09067a35143d271fdeaf28298b4f729b5da50abdbf3b9e6395262d8bc7a9`.

Preparation reuses a matching archive in the Cargo cache when available, or
fetches the pinned archive with a bounded download. It verifies the checksum
before extraction and applies `compatibility.patch` with zero fuzz. It never
modifies Cargo's registry cache. A lock prevents concurrent preparations, and a
fingerprint of the archive and patch makes unchanged preparations a no-op.
Generated source generations are retained until `.build` is removed so another
build does not lose its inputs. The upstream MIT license is preserved in the
extracted source and copied into the runtime image.

The preparation script needs `sh`, coreutils, `tar`, gzip, GNU `patch`, and
`flock` (util-linux); `curl` and CA roots are needed on a cache miss. Devenv, CI,
and Docker provision these tools. Locked Rust builds continue to use the root
`Cargo.lock`; preparation does not update dependency resolution.

## Changes

- Accept `u64` inbound receiver/media reply IDs. Timestamp-sized IDs must not
  overflow, truncate, or alias a pending native `u32` request ID.
- Keep the native outgoing counter, request parameters, operations, and all other
  numeric field types unchanged. Correlation widens the outgoing ID losslessly.
- Add `serde_path_to_error` to report the failing JSON field while preserving
  `Error::Serialization`. No full-payload logging is added.

The original `expected u32` warning alone does not establish whether the failing
field was `requestId`, `supportedMediaCommands`, or metadata. The new diagnostics
identify the field if other overflows remain. No new Cast functionality or
handcrafted outbound protocol operations are introduced.

## Regression tests and removal

`tests/rust_cast.rs` exercises the actual patched dependency through its public
native channels: all eight inbound ID variants, all synchronous receiver/media
method paths, large/small-ID correlation, preserved buffered messages, and
nested error paths. The tests run as part of `just check` or directly after
preparation:

```sh
just prepare
cargo test --locked --test rust_cast
```

Replace this patch with an upstream release or pinned revision once equivalent
lossless ID handling, correlation, and path diagnostics pass these tests. As
checked on 2026-10-18, the latest published crate is 0.21.0; upstream `main`'s
0.22.0 version bump alone does not contain the fix. These scripted tests do not
claim hardware validation or establish the cause of later device timeouts.
