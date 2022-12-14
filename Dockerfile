FROM rust:1.63.0 as builder
WORKDIR /usr/src/photon
COPY . .
RUN cargo install --path .
 
FROM debian:buster-slim
RUN apt-get update && apt-get install -y openssl ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/photon /usr/local/bin/photon
CMD ["photon"]
