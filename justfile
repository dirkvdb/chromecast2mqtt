
default:
    @just --list

# Materialize checksum-verified upstream source and apply the tracked patch.
prepare:
    @sh scripts/prepare-rust-cast.sh

fmt: prepare
    cargo fmt --all

fmt-check: prepare
    cargo fmt --all -- --check

clippy: prepare
    cargo clippy --locked --all-targets --all-features -- -D warnings

test: prepare
    cargo nextest run --locked --all-targets --all-features
    cargo test --locked --doc --all-features

# Includes doctests and does not require cargo-nextest.
test-cargo: prepare
    cargo test --locked --all-features

check: fmt-check clippy test

build: prepare
    cargo build --locked --release

# The binary resolves missing credentials through the SecretSpec Rust SDK.
run *args: prepare
    cargo run --locked --bin chromecast2mqtt -- {{args}}

# Check the same provider/profile selection as the SDK, without prompting.
secrets-check:
    secretspec check --no-prompt

docker:
    docker build -t chromecast2mqtt:local .
