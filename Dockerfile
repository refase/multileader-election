FROM rust:1.65-alpine as builder

WORKDIR /rdb-server
COPY . .
RUN apk add build-base libressl-dev protobuf-dev
RUN cargo build --release

FROM rust:latest
COPY --from=builder /rdb-server/target/release/election /server/

WORKDIR /server
CMD ["./election"]