# Shopping Sessions, DIAGNOSTIC build (never scored: it fails the dev check on
# purpose and returns a profile of the pipeline in the differing row).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo ./.cargo
COPY src ./src
COPY diag ./diag
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
# line tables only: same code as the scored build, plus address -> line info
ENV CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
RUN cargo build --release --features diag --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shopping /shopping \
 && objdump -d --no-show-raw-insn /shopping | awk '/^ +[0-9a-f]+:\t/ { sub(":", "", $1); print "0x" $1 }' > /addrs \
 && addr2line -e /shopping -a -i -f -C < /addrs | awk -f diag/lines.awk > /lines.tbl \
 && wc -l /addrs /lines.tbl

FROM scratch
COPY --from=build /shopping /shopping
COPY --from=build /lines.tbl /lines.tbl
USER 1000:1000
ENTRYPOINT ["/shopping"]
