# Online/Offline Feature Store (Java)

A feature store that computes features from a streaming event source, serves them at low
latency over a **Java gRPC** API (online), and stores **point-in-time-correct** history for
model training (offline).

Synthetic events stream through **Kafka** into a **Flink** feature-computation job. The
latest feature per entity is served from **Redis** behind a **gRPC** API; the full history
lands in **S3 as Parquet** for training. As-of joins eliminate data leakage, and batching
online lookups cut serving **p99 latency ~9×** (p50 ~20×).


**Stack:** Java · Kafka · Flink · Redis · S3/Parquet · gRPC/protobuf · Docker · AWS

---

## Architecture

```mermaid
flowchart LR
  P[Kafka Producer<br/>synthetic events] --> K[Kafka topic: events]
  K --> F[Flink job<br/>30s windowed aggregation]
  F --> R[Redis<br/>ONLINE: latest per entity]
  F --> S[S3 Parquet<br/>OFFLINE: full history, by date]
  R --> G[Java gRPC service<br/>GetFeatures / GetFeaturesBatch]
  G --> C[Client / load test]
  S --> J[Point-in-time as-of join]
  J --> T[Training dataset]
```

Two paths come out of Flink and never mix:
- **Online** (Redis → gRPC): *"what is this entity's feature right now?"* — optimized for latency.
- **Offline** (S3 → as-of join): *"what was it at time T?"* — optimized for correctness.

---

## The hard problems

### 1. Point-in-time correctness (no data leakage)

When building training data, a label at time **T** must only see feature values that
existed **at or before T** — never a value computed afterward. Using a later value leaks the
future; the model looks great offline and fails in production.

The fix is an **as-of join** (merge-on-nearest-backward): for each label at T, attach the
most recent feature with `feature_timestamp ≤ T`. Proof from
[`pit-join/pit_join.py`](pit-join/pit_join.py) — user-1's feature changes 10.0 → 99.0 at t=200:

| entity_id | label_ts | as-of value | naive "use latest" | leaked? |
|-----------|----------|-------------|--------------------|---------|
| user-1    | 150      | **10.0**  | 99.0               | yes — naive leaks the future |
| user-1    | 250      | 99.0        | 99.0               | no |

The naive join hands the t=150 label a value that didn't exist until t=200. The as-of join
gives the correct 10.0. This only stays offline: Redis keeps latest-only, so the historical
value 10.0 survives only in S3.

### 2. Online-serving latency (the N+1 lookup bottleneck)

Serving features for **K** entities the naive way means **K** separate Redis round-trips in
series — the classic N+1 problem. The fix pipelines all K lookups into a **single** round-trip.

Measured with [`LoadTest.java`](grpc-service/src/main/java/com/featurestore/grpc/LoadTest.java)
— 50 entities/batch, 2,000 iterations per mode, warmed up, timed with `System.nanoTime()`:

| mode | p50 | p99 |
|------|-----|-----|
| Before (N+1 lookups) | 6.49 ms | 11.42 ms |
| After (pipelined batch) | 0.32 ms | 1.27 ms |

**~20× faster at p50, and roughly an order of magnitude at p99.** The round-trips were the cost
— payload, gRPC framing, and parsing are identical between the two modes, so the gap isolates
exactly the fix. p50 is stable across runs (~20–22×); p99 is tail-latency and varies with
GC/scheduling/load, so treat it as an order-of-magnitude win rather than a fixed number. See
[`benchmarks/`](benchmarks/) for a repeated-trial capture.

### 3. Schema evolution (catching breaking upstream changes)

A feature is only as trustworthy as the events feeding it. If an upstream producer renames or
retypes a field, the naive JSON path *silently* absorbs it — a renamed `amount` deserializes to
its default `0.0`, and that zero flows straight into Redis and the Parquet training history. The
model then trains on quietly corrupted data with nothing in the logs to explain it.

The fix is an explicit, **versioned data contract**
([`FeatureSchema.java`](flink-job/src/main/java/com/featurestore/flink/FeatureSchema.java)):
every event carries a `schemaVersion`, and each payload is validated against it *before* it's
mapped to a feature ([`EventDeserializer.java`](flink-job/src/main/java/com/featurestore/flink/EventDeserializer.java)) —
field names, types, and version all checked at the write path. Anything that fails is dropped
and counted, never written through.

**Demonstrating it:** run the producer with `SCHEMA_BREAK=true` and it injects events that rename
`amount → amt`, simulating an upstream team changing the contract without coordinating. Before
validation those would have written `amount=0.0` into the store; now the Flink job rejects them
with a clear log line and they never reach Redis or Parquet:

```
SCHEMA REJECT v1 (rejected so far: 1): missing/non-numeric amount -> {"schemaVersion":1,"entityId":"user-3","amt":481.20,...}
```

Scoped deliberately: one feature, one schema version, one caught break — not a general-purpose
schema registry (that's the managed-service upgrade below).

---

## How to run it locally

Prerequisites: Docker, JDK 17+, Maven, AWS CLI configured (`aws configure`) with S3 access.
**Never commit AWS credentials** — they're read from your environment / AWS config and are
`.gitignore`d.

```bash
# 1. infra
docker compose up -d                 # Kafka + Redis;  redis-cli ping → PONG

# 2. stream events   (add SCHEMA_BREAK=true to demo schema validation rejecting bad payloads)
cd producer && mvn -q clean package && java -jar target/producer-1.0-SNAPSHOT.jar

# 3. compute features → Redis (online) + S3 (offline)
cd flink-job && mvn -q clean package && java -jar target/flink-job-1.0-SNAPSHOT.jar

# 4. serve + benchmark
cd grpc-service && mvn -q clean package -DskipTests
java -cp target/grpc-service-1.0-SNAPSHOT.jar com.featurestore.grpc.FeatureServer   # terminal A
java -cp target/grpc-service-1.0-SNAPSHOT.jar com.featurestore.grpc.LoadTest        # terminal B

# 5. build leak-free training data
cd pit-join && python pit_join.py --demo     # PIT proof only, no AWS
                # or: python pit_join.py      # + reads S3 history → training.parquet
```

Config you'll set: the S3 bucket name (in `pit_join.py` / the Flink S3 sink) and AWS
credentials via your environment.
