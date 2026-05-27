FROM rust:1.94-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p pgapp-server

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/pgapp-server /usr/local/bin/pgapp-server
EXPOSE 50051 8080
CMD ["pgapp-server"]
