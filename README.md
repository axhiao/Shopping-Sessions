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
   may read past the end) and cut into 4 MB chunks. An AVX2 scanner finds `,` `\n` `"` of a
   line in one 96-byte window and validates the CSV dialect with bit arithmetic; fields
   are parsed one by one as they are taken from the masks. Timestamps use a small cache of
   validated dates and SWAR for the clock (`ingest_ts` is trusted F1). Strings up to 15
   bytes are packed exactly into 128-bit keys (big-endian, so key order is byte order);
   longer ones are hashed and kept in a side table. Partition writes go through
   write-combining buffers flushed with streaming stores.
4. **Dedup, general path**: every event copy emits a 16-byte dedup record (72-bit id
   fingerprint, ingest seconds, payload location) into an event-id partition, every valid
   copy a 16-byte payload into its person's partition (the person resolved through a
   two-stage prefetch pipeline). Per event-id partition, two bitmaps and a small exact
   table keep the copy with the smallest `ingest_ts`; losing copies are marked dead in
   place.
5. **Dedup, sequence-number path** (see below): one 16-byte record per event copy,
   partitioned by the event's number; per partition a table indexed by the number keeps
   the copy with the smallest `ingest_ts`, and the valid events leave in number order as
   *runs* of one person (the person once, four bytes per event) for the person partitions.
6. **Sessions**: per person partition, a small table numbers the persons; a person with
   one run in time order is processed straight from it, anything else goes through
   30-minute buckets (two events less than 30 minutes apart always share a session, so no
   sort is needed). One pass cuts sessions, attributes them with a sliding 7-day click
   window (latest click, smallest `click_id` on ties; distinct campaigns assist), and
   aggregates `user_daily` rows and per-thread `campaign_daily` counters.
7. **Output**: a minimal hand-written Parquet writer (PLAIN, uncompressed, one row group);
   each thread writes its own `user_daily` part.

Memory: record buffers come from per-thread bump arenas (huge pages) that are released
with one `MADV_DONTNEED` per thread at the end (and input mappings chunk by chunk), so the
kernel has little to tear down at exit.

### The sequence-number path

The published generator writes an event id as `e` plus the base-36 digits of
`scramble(n, key)`: `n` counts the events and `scramble` is a fixed bijection on 40-bit
numbers with a per-dataset key. The program inverts it: it reads the key off the first
lines of the biggest events file (a short search: the key under which their numbers are
all small), and from then on an id of that form with a small number is handled by its
number instead of a hash of its text. That makes dedup a direct table lookup, and because
the generator numbers events user by user in time order, the events of a person come out
of the dedup already together and sorted.

This is a shortcut for speed only. The number is an exact one-to-one function of the id
text whatever key is used; an id of another form, or with a number outside the expected
range, takes the general path (both paths feed the same session code, and the two never
see the same id); persons always come from the links file; order is checked, never
assumed. If no key fits, everything takes the general path. `SS_NO_SEQ=1` turns the
shortcut off.

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
