# restaurant-stream-flink

Java Flink job for the [restaurant-stream pipeline](https://github.com/7Chethan007/restaurant-stream-infra#readme).
The only piece of this project that touches the actual restaurant dataset
and does the matching.

## What this repo owns

- A custom Pub/Sub source (`RichSourceFunction` wrapping the
  [Pub/Sub Java client](https://cloud.google.com/pubsub/docs/reference/libraries))
  reading from the **input topic** (`restaurant-queries`). Flink has no
  official Pub/Sub connector (that's an Apache Beam feature), so this is a
  hand-written source, not a stock connector.
- **Broadcast state**: `zomato.csv` (51.7k restaurants) loaded once at job
  start, held in memory, readable by every task — no external lookup DB.
- **Matching logic**: keyword/field match of the incoming text against
  restaurant name, location, and cuisine (no embeddings/semantic search).
- A sink publishing the matched result to the Pub/Sub **output topic**
  (`restaurant-results`).

## Known gotchas from the dataset (see root repo's README for full notes)

- `reviews_list` / `menu_item` columns are Python-repr strings, not JSON —
  irrelevant here unless the match logic later needs review text.
- `rate` needs cleanup (`"4.1/5"` → `4.1`, handle `NEW`/`-`) before any
  rating-based filtering.

## Deployment

Runs as a JobManager + TaskManager pair on the GCE VM provisioned by
`restaurant-stream-infra`, via Docker Compose alongside the backend. This
repo's own `.github/workflows` builds the job JAR/image and redeploys via
SSH on push to `main`.

## Status

Not yet built. Planned stack: Flink DataStream API (Java), Maven/Gradle
build producing a fat JAR submitted to the JobManager.
