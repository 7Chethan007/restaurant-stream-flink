# restaurant-stream-flink

Java Flink job for the [restaurant-stream pipeline](https://github.com/7Chethan007/restaurant-stream-infra#readme).
The only piece of this project that touches the actual restaurant dataset
and does the matching.

## What this repo owns

- **Flink's official Kinesis connector** reading from the **input stream**
  (`restaurant-queries`), pointed at Floci's local endpoint. Unlike GCP
  Pub/Sub, Kinesis has first-class Flink support — no hand-written source
  function needed, just the stock `FlinkKinesisConsumer`.
- **Broadcast state**: `zomato.csv` (51.7k restaurants) loaded once at job
  start, held in memory, readable by every task — no external lookup DB.
- **Matching logic**: keyword/field match of the incoming text against
  restaurant name, location, and cuisine (no embeddings/semantic search).
- A sink (`FlinkKinesisProducer`) publishing the matched result to the
  **output stream** (`restaurant-results`).

## Known gotchas from the dataset (see root repo's README for full notes)

- `reviews_list` / `menu_item` columns are Python-repr strings, not JSON —
  irrelevant here unless the match logic later needs review text.
- `rate` needs cleanup (`"4.1/5"` → `4.1`, handle `NEW`/`-`) before any
  rating-based filtering.

## Running it locally

Requires `restaurant-stream-infra`'s stack running first (`docker compose up -d`
there, then `./scripts/create-streams.sh`) — the job connects to Floci at
`http://floci:4566` via the shared `restaurant-stream-net` Docker network,
with dummy credentials (`test`/`test`, same as any Kinesis-via-Floci client).

## Status

Not yet built. Planned stack: Flink DataStream API (Java), Maven/Gradle
build producing a fat JAR submitted to a local JobManager + TaskManager
pair (own `docker-compose.yml`, joining `restaurant-stream-net`).
