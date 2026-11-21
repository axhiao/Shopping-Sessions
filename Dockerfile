# Shopping Sessions: a single static Rust binary, no runtime dependencies.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo ./.cargo
COPY src ./src
# x86-64-v3 = AVX2/BMI2 (the scorers are Cascade Lake or newer). Static so the
# runtime image can be empty.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --features diag --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping /shopping

FROM scratch
COPY --from=build /shopping /shopping
USER 1000:1000
ENTRYPOINT ["/shopping"]
