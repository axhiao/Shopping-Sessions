# Shopping Sessions

A single static Rust binary (no dependencies, `FROM scratch` image) for the Databricks
*Shopping Sessions* buildathon track: it turns the click-stream CSVs in `$DATA_DIR` into
the two Parquet tables in `$OUTPUT_DIR` exactly as `rules.md` specifies.

| Dataset (generator) | Local, 4 cores / 8 threads |
| --- | --- |
| 1M events (dev, seed 1) | 0.04 s |
| 20M events (practice size) | 0.55 s |
| 60M events (final size) | 1.75 s |

(AMD Threadripper PRO 5955WX, bare binary; the scorer's Xeons differ.)

## Two engines

`src/dense.rs` runs first. It takes inputs whose IDs have the shape the published
generator writes; anything else, and anything it finds out of place, goes through the
general pipeline in `src/main.rs` (described further down), which implements the rules
for any IDs. Both write the same tables. `SS_NO_DENSE=1` skips the dense engine.

### The dense engine

The generator writes an event ID as `e` plus the base-36 digits of `scramble(n, key)` and
a user ID as `u` plus the digits of `scramble(user * 512 + alias, key)`: `n` counts the
events, `user` the users (each has a few alias IDs), and `scramble` is a fixed bijection
on 40-bit numbers with a key per kind of ID and dataset. The program reads both keys off
a sample of the input (a short search: the keys under which the sampled numbers are all
small), and then:

1. **Parse (all threads, every line once).** Files are memory-mapped and cut into 4 MB
   chunks; an AVX2 scanner finds `,` `\n` `"` of a line and checks the CSV dialect; the
   two sides of a line (its first fields, the others) are looked up by their pattern of
   separators and quotes, so the layout is worked out once per pattern.
   - An event copy is stored straight into a table indexed by the event's number (16
     bytes: flags, `ingest_ts`, `ts`, the user): the first copy takes the cell with one
     compare-and-swap, later copies are set aside and settled after the parse (the
     smallest `ingest_ts` keeps the cell). Dedup is an array store; no partitions, no
     hash table. A copy that is not a valid event still takes its cell, so a later valid
     copy cannot rescue it.
   - The user ID of a row goes into the cell as its bytes; it is decoded when the table
     is read, once per stretch of equal IDs.
   - A click keeps its ID's bytes and goes to its user's bucket. Copies of a click are
     identical by the rules and the session code only asks for the latest click and the
     set of campaigns, so clicks need no dedup.
   - A link between two aliases of one user is four bytes; per user a 64-bit word says
     which of its aliases are linked.
2. **Sessions (all threads, a bucket of users at a time).** The generator numbers events
   user by user in time order, so reading the table in order meets each user's events
   together and already sorted: one pass checks that (order, and that the aliases met
   belong to one person), a second cuts the sessions, credits them with a sliding 7-day
   window over the user's clicks and writes the `user_daily` rows.
3. **Output**: as in the general pipeline.

Nothing is taken on trust:

- Decoding is exact whatever the keys are (a bijection), so copies of an event always
  meet in one cell and different IDs never do. An ID that does not decode, or whose
  number is out of range, is handled by its text.
- Persons always come from the links. A user whose links leave its own first 16 aliases
  (shared devices, long chains, upper-cased IDs), and every row that depends on an ID
  that is not a plain one, goes through a small general union-find and a sort.
- Time order inside a user is checked; a user whose events are out of order, or whose
  aliases are not all linked, is handled a person at a time with sorting.
- If the table turns out not to be laid out user by user, the engine stops before
  anything is kept and the general pipeline runs instead.

The idea of this arrangement (decode the generated IDs to counters, dedup by direct
index, identity by arithmetic on the user number) is not ours alone: several teams'
public READMEs describe it and name Team 67's public design as its origin. We had the
event-number decoding on our own (the sequence-number path below, 21 November) and
rebuilt the rest around it after reading those descriptions. We read descriptions, not
code; everything in this repository was written for it.

## The general pipeline

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
