# ── Stage 1: Build ────────────────────────────────────────────────────────────
# musl/scratch is NOT viable here: rdkafka uses librdkafka which requires
# libssl and libsasl2 at link time. Without the `cmake` feature those are
# dynamic system libs; even with `cmake` the openssl-sys musl build path adds
# significant complexity for marginal gain. bookworm-slim ships these as tiny
# shared libs and produces a reproducible, < 120 MB final image.
FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake libssl-dev pkg-config libsasl2-dev g++ make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY bin/ bin/
COPY migrations/ migrations/

RUN cargo build --release -p rules-engine

# ── Stage 2: Minimal runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rules-engine /usr/local/bin/rules-engine
COPY migrations/ /migrations/

EXPOSE 8080
ENTRYPOINT ["rules-engine"]
