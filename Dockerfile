FROM rust:1.94-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p pgapp-server

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/pgapp-server /usr/local/bin/pgapp-server
ENV PGAPP_BIND_ADDR=0.0.0.0:50051 \
    PGAPP_ADMIN_BIND_ADDR=0.0.0.0:8080 \
    PGAPP_ENABLE_NOTIFY=true \
    PGAPP_ENABLE_AUTH=false \
    PGAPP_MAX_REDELIVERY_COUNT=0 \
    PGAPP_DLQ_RETENTION_DAYS=0 \
    PGAPP_MAX_SCHEMA_BYTES=262144
EXPOSE 50051 8080
CMD ["pgapp-server"]
