FROM rust:1.89 AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY bin/ bin/
COPY migrations/ migrations/

RUN cargo build --release -p rules-engine

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/rules-engine /usr/local/bin/rules-engine
COPY migrations/ /migrations/

EXPOSE 8080
ENTRYPOINT ["rules-engine"]
