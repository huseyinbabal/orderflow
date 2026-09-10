# OrderFlow Kafka @ 10,000 msg/s — DigitalOcean perf test

**Date:** 2026-09-10 · **Cluster:** DOKS `orderflow-perf` (fra1, k8s 1.36.3), 10 nodes, torn down after the run.
**Goal:** push 10k orders/s through the OrderFlow HTTP → Avro → Kafka path and see whether the
consumer groups keep up. Plus: does the "production" producer tuning (`linger.ms` / `lz4` /
`batch.size`) actually help here?

---

## Cluster layout (isolated node pools)

| Pool | Nodes | Size | Runs |
|---|---|---|---|
| `kafka` | 3 | s-4vcpu-8gb + 50 GB block vol | 3-broker KRaft, RF=3, min.insync=2 |
| `app` | 2 | s-4vcpu-8gb | `orderflow` API ×3 (producer), `orderflow-consumer` ×6 |
| `load` | 2 | c-4 (CPU-opt) | k6-operator, 6 distributed runners |
| `monitoring` | 1 | s-4vcpu-8gb | Prometheus + kafka-exporter |
| `system` | 2 | s-2vcpu-4gb | Postgres, Redis, RabbitMQ, schema-registry, coredns |

Each pool tainted + `nodeSelector`-pinned so the load generator, brokers, and monitoring never
share CPU. `orders` topic: **48 partitions, RF=3**. Consumers: 6 pods × `concurrency=8` =
**48 consumers per group** (one partition each).

k6 profile: `ramping-arrival-rate`, 20s + 40s ramp → **sustain at 10,000 req/s** → 20s down.
Message ≈ small JSON order → Avro (~200–300 B on the wire).

---

## Result 1 — can the pipeline take 10k/s?

| Stage | Verdict at 10k/s |
|---|---|
| **HTTP intake + Avro + produce** (untuned) | ✅ ~10,000 msg/s sustained, **HTTP p95 ≈ 5 ms**, 0 errors |
| **Kafka brokers (RF=3, acks=all)** | ✅ never the bottleneck — broker nodes idle-ish |
| **`analytics` consumer group** (light work: one log line) | ✅ **keeps up** — group lag stays ~0 the whole run |
| **`fulfillment` consumer group** (`Thread.sleep(20)` per msg) | ❌ **cannot keep up** |

### Why `fulfillment` falls behind

`KafkaOrderConsumers.fulfillment()` does `Thread.sleep(20)` per message (deliberately, "makes
consumer lag visible under load"). With 48 consumer threads:

```
48 threads × (1000 ms / 20 ms) = 48 × 50 = ~2,400 msg/s   ← measured drain rate: 2,401 msg/s
```

At 10,000/s ingest the group accrues lag at **~7,600 msg/s**. Measured lag growth during the
6-minute baseline sustain:

| elapsed | ~30 s | ~75 s | ~110 s | end of 6 min |
|---|---|---|---|---|
| `fulfillment` lag | 3 k | 320 k | 570 k | **~3,000,000** |
| `analytics` lag | ~1 k | ~3 k | ~3 k | ~0 |

After load stops, `fulfillment` drains at ~2,400/s → **~21 minutes to clear a 6-minute spike.**

### What it would take for `fulfillment` to keep up at 10k/s

- Cut the per-message work (the 20 ms sleep is the whole story), **or**
- ~**200 partitions** and ~200 consumer threads (`10000 ÷ 50`), **or**
- Make the handler async / batch the downstream call instead of blocking 20 ms each.

The broker and the producer are fine. The bottleneck is **consumer processing time**, and no
amount of Kafka tuning moves it.

---

## Result 2 — producer tuning: before / after

Same 10k/s target, same everything, only the producer config changed.

| | **Baseline** (defaults) | **"Tuned"** (`linger.ms=10`, `lz4`, `batch.size=65536`) |
|---|---|---|
| Producer config | linger 0, batch 16 KB, no compression, acks=all | linger 10 ms, batch 64 KB, **lz4**, acks=all |
| **Sustained throughput into Kafka** | **~10,000 msg/s** | **~7,500–8,000 msg/s** (window avg 7,900; high variance) |
| **HTTP p95 latency** | **~5 ms** | **~1,350 ms** |
| HTTP median latency | ~1 ms | ~420 ms |
| HTTP max latency | ~2.0 s | 11–15 s |
| k6 dropped iterations / runner | ~9 | **~154,000** |
| Error rate | 0 | 0 |
| Client-effective good-put | ~10,000/s | **~5,800/s** (k6 completed-request rate) |

### The tuning made it **worse** here — why

The "classic" advice (`linger.ms` + big batches + compression → higher throughput) assumes a
**dedicated producer loop**. This path is an HTTP request that hands off to `KafkaTemplate` on
**CPU-limited pods** (1.5 vCPU each, ×3):

- **`lz4` compression** burns producer CPU on every batch — on throttled pods that directly caps rate.
- **`linger.ms=10` + `batch.size=64 KB` across 48 partitions with random keys** — batches rarely
  fill, so every batch eats the full 10 ms wait; with `acks=all`+RF=3 round-trips the 32 MB
  producer buffer backs up, and `send()` starts blocking. That's where the 100s-of-ms HTTP
  latency comes from.
- k6's arrival-rate executor needs `rate × latency` VUs to hold 10k/s; latency went 5 ms → 420 ms
  so it needed ~85× more VUs, hit the cap, and **dropped ~920k iterations total**.

**Takeaway:** tune for your actual bottleneck. At 10k/s on this hardware the untuned producer
had no bottleneck to fix, and the tuning bundle added a deep queue in front of a
latency-sensitive request path. `linger.ms` earns its keep when a backend producer is
throughput-bound and nobody is waiting on the individual `send()`.

---

## Cost

10-node cluster + 3× 50 GB block volumes + basic registry, **fra1, hourly billing**, alive
~2 h 20 m (a lot of that was debugging two infra snags, below).

≈ **$2.0–2.5 total.** Cluster, droplets, volumes, and registry all deleted and verified gone.

## Infra snags hit (for next time)

1. **DO block volumes have a `lost+found` dir** → Kafka refuses `log.dirs` at the volume root.
   Fix: point `KAFKA_LOG_DIRS` at a subdirectory (`/var/lib/kafka/data/logs`).
2. **Image arch** — built on Apple Silicon (arm64), DOKS nodes are amd64 → `exec format error`.
   Fix: `docker buildx build --platform linux/amd64`.
3. **`cp-schema-registry` + a k8s Service named `schema-registry`** → k8s injects
   `SCHEMA_REGISTRY_PORT=tcp://…` which the CP entrypoint reads as a deprecated config and
   exits 1. Fix: `enableServiceLinks: false` on the pod.
4. **k6-operator** splits the *script's* configured rate across runners via execution segments —
   don't pre-divide by `parallelism` in the script.
5. `TestRun` with `cleanup: post` deletes runner pods immediately → the k6 end-of-test summary
   is lost. Omit it if you need the logs.

## Files

`k8s/doks/` — the full manifest set (`00-infra` → `40-k6`), the 10k k6 script, and the
`doctl kubernetes cluster create` invocation is in the session log. Re-runnable.
