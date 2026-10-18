FROM rust:1.97-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates cmake perl pkg-config curl patch util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock secretspec.toml ./
COPY patches/rust_cast/ ./patches/rust_cast/
COPY scripts/prepare-rust-cast.sh ./scripts/prepare-rust-cast.sh
RUN sh scripts/prepare-rust-cast.sh
COPY src/ ./src/

RUN cargo build --locked --release --bin chromecast2mqtt

FROM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.title="chromecast2mqtt" \
      org.opencontainers.image.description="Google Cast MQTT bridge using native rust_cast operations" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Container credentials come from injected environment variables, never a desktop keyring.
ENV SECRETSPEC_PROVIDER=env \
    SECRETSPEC_PROFILE=default

WORKDIR /app
COPY --from=builder /build/target/release/chromecast2mqtt /usr/local/bin/chromecast2mqtt
COPY LICENSE /usr/share/doc/chromecast2mqtt/copyright
COPY --from=builder /build/.build/rust_cast/LICENSE /usr/share/doc/chromecast2mqtt/rust_cast-LICENSE
USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/chromecast2mqtt"]
