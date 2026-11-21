# Shopping Sessions

A single static Rust binary (no dependencies, 1.6 MB `FROM scratch` image) for the
Databricks *Shopping Sessions* buildathon track: it turns the click-stream CSVs in
`$DATA_DIR` into the two Parquet tables in `$OUTPUT_DIR` exactly as `rules.md` specifies.

| Dataset (generator) | Local, bare | Local, Docker with scorer flags pinned to 4 cores / 8 threads |
| --- | --- | --- |
| 1M events (dev, seed 1) | 0.13 s | 0.35 s |
| 20M events (practice size) | 0.82 s | 1.15 s |
| 60M events (final size) | 2.40 s | 2.77 s |

(AMD Threadripper PRO 5955WX; the scorer's Xeons will differ.)

## Pipeline

1. **Small inputs**: manifest, `products.csv` (category ids sorted byte-wise, `unknown`
   included, so comparing ids compares names), `campaigns.csv`.
2. **Link map (all threads)**: link files are parsed in parallel; IDs go into a concurrent
   open-addressing table and a lock-free union-find links roots by slot index. A parallel
   atomic-min picks each person's byte-wise smallest ID and the table becomes the final
   `user id -> person name` map. A 2 MB bit filter marks the IDs whose person is *not*
   themselves, so most rows never probe the table.
3. **Parse (all threads)**: files are memory-mapped (with zero pages after EOF so SIMD code
   may read past the end) and cut into 4 MB chunks. An AVX2 scanner walks `,` `\n` `"` and
   validates the CSV dialect; timestamps are parsed with SWAR (`ingest_ts` is trusted F1).
   Strings up to 15 bytes are packed exactly into 128-bit keys (big-endian, so key order is
   byte order); longer ones are hashed and kept in a side table.
   - every event copy emits a 16-byte dedup record (72-bit id fingerprint, ingest seconds,
     payload location) into one of N event-id partitions;
   - every *valid* copy emits a 24-byte payload (person, ts, purchase index), its person
     resolved through a two-stage prefetch pipeline, into one of N person partitions;
     purchases put (price, category) in a small side table;
   - valid clicks are resolved and partitioned by person the same way.
   Partition writes go through write-combining buffers flushed with streaming stores.
4. **Dedup**: per event-id partition, a tagged hash table keeps the copy with the smallest
   `ingest_ts`; losing copies are marked dead in place in the payload buffers.
5. **Sessions**: per person partition, local person ids, a counting sort by person, a sort
   of packed `ts << 32 | x` values per person, then one pass that cuts sessions, attributes
   them with a sliding 7-day click window (latest click, smallest `click_id` on ties;
   distinct campaigns assist), and aggregates `user_daily` rows and per-thread
   `campaign_daily` counters.
6. **Output**: a minimal hand-written Parquet writer (PLAIN, uncompressed, one row group);
   each thread writes its own `user_daily` part.

Memory: record buffers come from per-thread bump arenas that are released with one
`MADV_DONTNEED` per thread at the end (and input mappings chunk by chunk), so the kernel has
little to tear down at exit.

## Build and test

```sh
cargo build --release
cargo test --release                                                   # parser equivalence tests
python3 scripts/run_fixtures.py fixtures -- ./target/release/shopping  # 36 edge cases
DATA_DIR=dev OUTPUT_DIR=out ./target/release/shopping && python3 scripts/compare.py out dev-answer
scripts/crosscheck.sh REFERENCE_BIN ./target/release/shopping DATASET...  # diff two builds
docker build -t shopping-sessions . && sh scripts/docker_run.sh dev out
```

`SS_VERBOSE=1` prints phase timings to stderr. The test scripts need `polars`.
