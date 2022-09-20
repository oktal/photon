FROM rust:1.63.0 as builder
WORKDIR /usr/src/rte-refresh-token
COPY . .
RUN cargo install --path .
 
FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/rte-refresh-token /usr/local/bin/rte-refresh-token
CMD ["rte-refresh-token"]
