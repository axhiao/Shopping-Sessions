# Shopping Sessions

A single static Rust binary (no dependencies) for the Databricks *Shopping Sessions*
buildathon track: it turns the click-stream CSVs in `$DATA_DIR` into the two Parquet
tables in `$OUTPUT_DIR` exactly as `rules.md` specifies.

## Pipeline

1. **Small inputs**: manifest, `products.csv` (category ids sorted byte-wise, `unknown`
   included, so comparing ids compares names), `campaigns.csv`.
2. **Link map (all threads)**: link files are parsed in parallel; IDs go into a concurrent
   open-addressing table and a lock-free union-find links roots by slot index. A parallel
   atomic-min then picks each person's byte-wise smallest ID, and the table becomes the
   final `user id -> person name` map.
3. **Parse (all threads)**: files are memory-mapped (an anonymous zero page after EOF lets the
   SIMD scanner read past the end) and split into 4 MB chunks. An AVX2 scanner finds
   `,` `\n` `"` with one shuffle+compare per 32 bytes; fields are validated against the
   CSV dialect while walking the structural characters. Strings of up to 15 bytes are packed
   exactly into 128-bit keys (big-endian, so key order is byte order); longer ones are hashed
   and kept in a side table.
   - every event copy emits a 24-byte dedup record (96-bit id fingerprint, ingest time,
     payload location) into one of 1024 event-id partitions;
   - every *valid* copy also emits a 32-byte payload, already resolved to its person (batched,
     prefetched link-map lookups), into one of 1024 person partitions;
   - valid clicks are resolved and partitioned by person the same way.
   Partition writes go through per-partition write-combining buffers flushed with streaming
   stores.
4. **Dedup**: per event-id partition, a small hash table keeps the copy with the smallest
   `ingest_ts`; losing copies are marked dead in place in the payload buffers.
5. **Sessions**: per person partition, local person ids, a counting sort by person, a sort by
   `ts` per person, then one pass that cuts sessions, attributes them with a sliding 7-day
   click window (latest click, smallest `click_id` on ties; distinct campaigns assist), and
   aggregates `user_daily` rows and per-thread `campaign_daily` counters.
6. **Output**: a minimal hand-written Parquet writer (PLAIN, uncompressed, one row group);
   each thread writes its own `user_daily` part.

## Build, test

```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- ./target/release/shopping   # 36 edge cases
DATA_DIR=dev OUTPUT_DIR=out ./target/release/shopping && python3 scripts/compare.py out dev-answer
docker build -t shopping-sessions . && sh scripts/docker_run.sh dev out
```

`SS_VERBOSE=1` prints phase timings to stderr.
