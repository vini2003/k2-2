# Multi-stage build for k2 server/client
FROM rust:1.81-bullseye AS builder
WORKDIR /app

# Pre-copy manifest for cache-friendly builds
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo fetch

# Now copy full source
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim AS runtime
WORKDIR /data

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/k2 /usr/local/bin/k2

EXPOSE 5757

# Default to CLI help; override in `docker run` args
ENTRYPOINT ["k2"]
CMD ["--help"]
