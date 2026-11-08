---
title: "03 — Kafka with Camel"
subtitle: "camel-quarkus-kafka producer and consumer, keys, acks, offsets, breakOnFirstError, headers, Avro with Confluent Schema Registry. Camel 4.22 / Camel Quarkus 3.40 / CP 8.4. Written 2026-11-08."
---

# What this module is for

- Read a Camel Kafka endpoint URI and say what the producer or consumer will do: which key, which
  acks, when the offset is committed, what happens on an error.
- Explain the **blocked partition**: why one bad record can stop all records behind it, and what to
  do about it in an operational flow.
- Wire Avro with Confluent Schema Registry into a Camel route, and say how that fits a governed
  setup ("no schema, no topic").
- Choose between Camel Kafka, Quarkus Messaging, the plain Kafka client and Kafka Streams/Flink for
  a given integration flow.

Kafka itself (partitions, ISR, idempotent producer, consumer groups, transactions) is in the
[Confluent course](https://github.com/fps4/confluent-crash-course) (clone it next to this one): [07 Kafka internals](../../confluent-crash-course/07-kafka-internals/notes.md)
§2.5–2.6 and [notes-clients-and-storage](../../confluent-crash-course/07-kafka-internals/notes-clients-and-storage.md).
This module covers only the Camel side.

---

# 0. First, a breaking change you must know (Camel 4.21+)

**The Kafka header names changed in Camel 4.21.** Before, `KafkaConstants.KEY` was the string
`kafka.KEY`. From 4.21 on (so in our 4.22.1) it is `CamelKafkaKey`. Same for all headers:

| Constant (Java) | Camel ≤ 4.20 | Camel ≥ 4.21 (this lab) |
|---|---|---|
| `KafkaConstants.KEY` | `kafka.KEY` | `CamelKafkaKey` |
| `KafkaConstants.PARTITION_KEY` | `kafka.PARTITION_KEY` | `CamelKafkaPartitionKey` |
| `KafkaConstants.PARTITION` / `OFFSET` / `TOPIC` | `kafka.PARTITION` / … | `CamelKafkaPartition` / `CamelKafkaOffset` / `CamelKafkaTopic` |
| `KafkaConstants.OVERRIDE_TOPIC` | `kafka.OVERRIDE_TOPIC` | `CamelKafkaOverrideTopic` |
| `KafkaConstants.KAFKA_RECORD_META` | `kafka.RECORD_META` | `CamelKafkaRecordMeta` |
| `KafkaConstants.MANUAL_COMMIT` | `CamelKafkaManualCommit` | unchanged |

Why: the old `kafka.*` names were not filtered at HTTP boundaries, so an HTTP caller could send a
header `kafka.OVERRIDE_TOPIC` and redirect a REST→Kafka route to another topic. The `Camel*` prefix
is stripped by the default header filters.

Consequences:

- **Use the constant, never the string.** `setHeader(KafkaConstants.KEY, ...)` works in every
  version. `setHeader("kafka.KEY", ...)` or YAML `name: kafka.KEY` on 4.21+ **does not set the key**:
  the record goes out with a null key, and the header itself is dropped by the Kafka header filter
  (it filters the `kafka.` prefix).
- Most blog posts, Stack Overflow answers and Red Hat docs (Red Hat build of Camel 4.18 [V]) still
  show `kafka.KEY`. On Red Hat's build the old names are still right. Check which line your platform runs.
- This also explains what we saw in the lab: a log of `${header.kafka.KEY}` after `.to("kafka:...")`
  printed nothing. The producer does **not** remove the key header (it reads it with `getHeader`,
  checked in the 4.22.1 source). The header was simply called `CamelKafkaKey`, and the Simple
  syntax was wrong too (see §6). [V: re-run the log with `${header.CamelKafkaKey}` to confirm.]

---

# 1. The producer

```java
.setHeader(KafkaConstants.KEY, simple("${body.orderId}"))      // same order -> same partition
.to("kafka:orders.order-status.updated.v1"
    + "?valueSerializer=io.confluent.kafka.serializers.KafkaAvroSerializer"
    + "&schemaRegistryURL={{esb.schema-registry-url}}"
    + "&requestRequiredAcks=all&enableIdempotence=true")
```

| Option / header | Default (4.22) | What it does | Note |
|---|---|---|---|
| header `CamelKafkaKey` (`KafkaConstants.KEY`) | none | Record key | Key = the entity whose order matters (orderId, itemTag) |
| `key` (endpoint option) | none | Fixed key for all records | Overrides the header. Almost always wrong (see lab step 6) |
| header `CamelKafkaPartitionKey` / option `partitionKey` | none | Force a partition number | Rarely right; bypasses the partitioner |
| header `CamelKafkaOverrideTopic` | none | Send to another topic | Removed by the producer after use (one-time header) |
| `requestRequiredAcks` | `all` | Kafka `acks` | Set it anyway — makes the intent visible in review |
| `enableIdempotence` | `true` | Kafka `enable.idempotence` | Removes retry duplicates from one producer session only |
| `keySerializer` / `valueSerializer` | `StringSerializer` | Serde class names | Key stays a plain string in this lab; only the value is Avro |
| `synchronous` | `false` | Block the calling thread until the ack | See below |
| `recordMetadata` | `false` | Put `List<RecordMetadata>` in header `CamelKafkaRecordMeta` | `RecordMetadata.toString()` = `topic-partition@offset` |
| `additionalProperties.<kafka.prop>` | — | Any Kafka or serializer property | e.g. `additionalProperties.auto.register.schemas=false` |

**Sync vs async.** Default (`synchronous=false`) is *not* fire-and-forget. Camel uses its
asynchronous routing engine: the send returns at once, the thread is free, and the route
**continues only when the Kafka callback arrives** (ack or error). So in `PartnerApiRoute` the 202
is returned only after the broker confirmed the write; if Kafka is down, `onException` returns 503.
`synchronous=true` gives the same result but blocks a thread per in-flight send. Use it only when
something downstream needs the same thread (for example a JTA transaction). [V for the JTA case]

**What the producer gives back.** Nothing in the body. With `recordMetadata=true` you get the
header `CamelKafkaRecordMeta` (a list, one entry per record sent). Useful for an audit log
("event X stored at partition 0, offset 42"), not needed for the 202.

**Batch send.** If the body is a `List` (and `useIterator`, default true), each element becomes one
record. The key comes from each element's `CamelKafkaKey` header or the endpoint `key`
(fixed in 4.22).

---

# 2. The consumer

```java
from("kafka:orders.order-status.updated.v1"
    + "?groupId=esb-status-board&autoOffsetReset=earliest"
    + "&valueDeserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer"
    + "&schemaRegistryURL={{esb.schema-registry-url}}&specificAvroReader=true"
    + "&breakOnFirstError=true")
```

| Option | Default (4.22) | What it does | Watch out |
|---|---|---|---|
| `groupId` | none (random UUID) | Consumer group | Always set it. One group per *destination*, not per app (see "In practice") |
| `autoOffsetReset` | `latest` | Where a new group starts | `earliest` for a new consumer that must see history |
| `consumersCount` | `1` | Number of `KafkaConsumer` threads in this route | Useful up to the partition count. Our lab topic has **1 partition**, so 1 is the max that does work |
| `maxPollRecords` | Kafka default 500 | Records per poll | Long processing × 500 > `max.poll.interval.ms` (5 min) → rebalance |
| `pollTimeoutMs` | `5000` | Poll timeout | |
| `autoCommitEnable` | `true` | Kafka auto commit in the background | See §3 |
| `allowManualCommit` | `false` | Puts `KafkaManualCommit` in header `CamelKafkaManualCommit` | Use with `autoCommitEnable=false` |
| `kafkaManualCommitFactory` | sync | Sync or async manual commit | `DefaultKafkaManualAsyncCommitFactory` = commit in next poll loop |
| `breakOnFirstError` | `false` | Stop processing the rest of the poll on an error, retry from the failed record | §4 — the most important option in this module |
| `batching` | `false` | Body = `List<Exchange>` of one poll | §3 |
| `isolationLevel` | `read_uncommitted` | Kafka `isolation.level` | `read_committed` if producers use transactions |

4.22 fix worth knowing: without a `groupId`, older versions gave **each** consumer thread its own
random group, so with `consumersCount=3` every record was processed three times. Fixed in 4.22 —
but the right answer is still: set `groupId`.

**Threading.** Each of the `consumersCount` threads owns one `KafkaConsumer` and runs the route for
its records **one at a time**, in partition order. That is what keeps per-key order. Do not add
`.threads()` or `seda:` after `from(kafka)` if order matters — it breaks ordering and makes
commits run ahead of processing.

---

# 3. Offset commit modes

| Mode | Config | When is the offset committed | Failure result |
|---|---|---|---|
| Auto commit (default) | `autoCommitEnable=true` | In the background, every `auto.commit.interval.ms` (5 s), for what was *polled* | A crash can lose records that were polled but not finished, or replay up to 5 s |
| Auto commit + `breakOnFirstError` | as in `esb-lab` | Camel does not commit past the failed record; it seeks back | Failed record retried forever (§4) |
| Manual | `allowManualCommit=true&autoCommitEnable=false` + `commit()` | When your route calls `KafkaManualCommit.commit()` | You decide: commit after the side effect = at-least-once |
| Batching | `batching=true` (+ manual commit) | Commit the last exchange of the list = whole batch | Camel does not commit a failed batch → the whole batch is re-processed |

Manual commit in Java:

```java
.process(e -> e.getIn()
        .getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class)
        .commit())
```

**Batching consumer** (`batching=true`): Camel puts the records of one poll in the body as a
`List<Exchange>`; size is limited by `maxPollRecords`, and `batchingIntervalMs` closes a batch on
time. Good for bulk writes (JDBC batch insert, one call to an analytics API). Bad for per-record
error handling: one bad record fails the batch unless you handle it inside. [V: how
`breakOnFirstError` behaves in batching mode]

---

# 4. `breakOnFirstError` — and the blocked partition

What it does in Camel 4: when the route fails for a record (after the error handler gave up and
did not mark it handled), the consumer **stops processing the rest of that poll**, does not commit
past the failed record, **seeks back** to the failed record's offset and polls again. So the same
record is retried, then the next ones. The Camel docs: "This message will be continually retried
until it can be processed without an error."

Without it (`false`): the error is logged, the record is skipped, and the offset moves on. The
record is **lost** for this consumer group.

**Observed in the lab (2026-11-08, `StatusBoardRoute`, webhook stubbed to return 503):**

1. `DefaultErrorHandler` made 5 attempts (1 + 4 redeliveries, back-off 1 s, 2 s, 4 s, 8 s), then
   logged ERROR.
2. `KafkaRecordStreamingProcessor` logged *"Will seek consumer to offset …"*, the record was
   polled again, and the cycle repeated **with no end**: 20 webhook calls in about 70 s.
3. A valid `SO-1001234` event behind it on the same partition was **not processed** until the 503
   stub was removed. Then both went through and consumer-group lag dropped to 0.
4. Each re-poll re-ran the **whole route**, so the JMS status-board command was sent again each
   cycle: duplicates on `esb.status-board.cmd`.

So the trade-off is plain:

| Choice | Data loss | Blocking | Duplicates |
|---|---|---|---|
| `breakOnFirstError=false` | Yes — failed record skipped | No | Within the redelivery attempts only |
| `breakOnFirstError=true` | No | **Yes — the partition stops** for this group | Yes — every cycle re-runs all side effects |
| Dead letter after N attempts (`deadLetterChannel("kafka:...dlt")`) | No (record parked) | No | Within the attempts only |
| Retry topic, then DLT | No | No | Yes; order per key is lost for retried records |

Rule: `breakOnFirstError=true` is right only when **order matters more than progress** and the
failure is short (a broker restart). For a partner webhook it is wrong: one partner's outage stops
the status board. Module 05 builds the dead-letter and idempotent consumer side.

---

# 5. Headers: Kafka ↔ Camel

| Direction | What happens by default |
|---|---|
| Kafka → Camel (consumer) | Every Kafka record header becomes a Camel header, **value as `byte[]`**. Plus Camel's own: `CamelKafkaTopic`, `CamelKafkaPartition`, `CamelKafkaOffset`, `CamelKafkaKey`, `CamelKafkaTimestamp` |
| Camel → Kafka (producer) | Every Camel header of type String, Integer, Long, Double, Boolean or `byte[]` becomes a Kafka record header — **except** names starting with `Camel`, `camel`, `org.apache.camel` or `kafka.` (`KafkaHeaderFilterStrategy`) |
| Custom | `headerFilterStrategy=#myFilter`, `headerSerializer` / `headerDeserializer` for types |

What this means in a real route:

- **HTTP headers leak into Kafka.** In `PartnerApiRoute` the REST request headers
  (`User-Agent`, `Accept`, maybe `Authorization`) are Camel headers. Unless they are removed, they
  are written as Kafka record headers. [V: check in kafka-ui in lab step 2]. In a partner-facing
  route: `.removeHeaders("*")` (or an allowlist) before `.to("kafka:...")`, then set only the
  headers that belong to the contract (for example CloudEvents `ce_id`, `ce_type`).
- **Kafka headers leak into the next transport.** In `StatusBoardRoute`, Kafka record headers
  become JMS properties and HTTP headers. The `Camel*` ones are filtered by JMS and HTTP; custom
  ones are not. `esb-lab` had `.removeHeaders("kafka.*")` — on 4.22 this matched nothing (the
  names are now `CamelKafka*`), so it was removed.
- On the consumer side a header like `traceparent` arrives as `byte[]`; convert it with
  `${headerAs(traceparent,String)}`, in a processor, or once for all with `headerDeserializer`.

---

# 6. Simple language gotchas (seen in the lab)

| Write | Means | Result |
|---|---|---|
| `${header.kafka.KEY}` | header `kafka`, then call `.KEY` on it (OGNL) | Empty or error. **Wrong** for any header name with a dot |
| `${header[kafka.KEY]}` | header named `kafka.KEY` | Right syntax for dotted names — but on 4.21+ no such header exists |
| `${header.CamelKafkaKey}` | header `CamelKafkaKey` | Right on 4.21+ (no dot, so dot form is fine) |
| `${header[ce.type]}` | your own dotted header | Use brackets for every dotted name |
| `${body.orderId}` | calls `getOrderId()` on the body | Works for Avro classes and beans with getters. **Public fields do not work** (verified 2026-11-08). Java records (`orderId()`) [V] |

---

# 7. Avro with Confluent Schema Registry

The pieces in `esb-lab/pom.xml`: `camel-quarkus-kafka`, `quarkus-avro` (code generation),
`quarkus-confluent-registry-avro` (native/registration glue), `io.confluent:kafka-avro-serializer`
8.4.0 from `packages.confluent.io/maven`, plus `quarkus-rest-client` and `jackson-dataformat-csv`
that the Confluent serializer needs.

| Piece | Setting | Note |
|---|---|---|
| Code generation | `.avsc` in `src/main/avro` → classes in `target/generated-sources/avsc` | `quarkus-avro`, runs in `generate-code`; dev mode regenerates on save |
| Producer | `valueSerializer=io.confluent.kafka.serializers.KafkaAvroSerializer` + `schemaRegistryURL=...` | `schemaRegistryURL` is a Camel option, mapped to `schema.registry.url` |
| Consumer | `valueDeserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer` + `specificAvroReader=true` | Without `specificAvroReader` the body is a `GenericRecord`, and `${body.status}` does not work |
| Registration | `auto.register.schemas` — Confluent default `true` | Lab: `true`, the serializer registered `orders.order-status.updated.v1-value` on first send (observed). Governed: `false` + `use.latest.version=true`, schemas registered from CI [V for the exact pair] |
| Subject name | `TopicNameStrategy` (default) → `<topic>-value` | One schema line per topic. `RecordNameStrategy` only if a topic carries several event types |
| Compatibility | sr-a default `BACKWARD` | New schema must read old data: add fields **with defaults**; never add a required field |
| Test | `schemaRegistryURL=mock://esb-test` | In-memory registry inside the serializer (module 06) |

In a governed setup set `additionalProperties.auto.register.schemas=false` on every producer
endpoint (or as a component default). Then a producer whose schema is not registered fails on the
first send: "no schema, no topic" is enforced by the client, and broker-side schema ID validation
(Confluent course module 08 §3) catches clients that skip the serializer.

Wire format reminder: magic byte `0` + 4-byte schema ID + Avro binary. The schema ID is
registry-specific — the reason Schema Linking exists for the Confluent Cloud side.

---

# 8. Which Kafka client for which job

| | Camel Kafka (`camel-quarkus-kafka`) | Quarkus Messaging (`quarkus-messaging-kafka`, SmallRye) | Plain Kafka client | Kafka Streams / Flink |
|---|---|---|---|---|
| Model | Route: endpoint → EIPs → endpoint | `@Incoming` / `@Outgoing` methods, reactive streams | Your own poll loop | Stateful stream processing (joins, windows, state stores) |
| Strong at | **Protocol bridging**: Kafka ↔ JMS/HTTP/file/SFTP/SOAP, mapping, routing, many EIPs | Kafka-native microservices; per-message ack, built-in DLQ and delayed-retry topic strategies [V] | Full control, lowest overhead | Aggregations over time, enrichment joins, exactly-once Kafka→Kafka |
| Weak at | Stateful processing; error handling semantics need care (§4) | Many non-Kafka protocols | Everything is your code (retries, commits, metrics) | Side effects to HTTP/JMS; ops cost (Flink cluster) |
| Example | Partner REST → Kafka; Kafka → Artemis status-board queue; legacy scan queue → Kafka (Kafka inside the ESB) | An order service reacting to `order-status` events | A high-volume IoT bridge with custom back-pressure | "Scans per order in the last 10 min"; join order status with a location reference table |

Short answer: **Camel where the job is integration (two or more protocols), Quarkus
Messaging or Streams where the job is Kafka-in, Kafka-out business logic.** A Camel route that grows
state and windows should become a Streams/Flink job.

---

# 9. Exactly-once? No — at-least-once plus idempotent consumers

- Camel + Kafka gives **at-least-once** end to end: the producer may re-send after a timeout
  (idempotence removes that only within one producer session), and the consumer re-processes
  after a crash or a `breakOnFirstError` seek (as observed in §4).
- camel-kafka supports **producer transactions** (`transacted=true` or `transactionalId=...`;
  if both, `transactionalId` wins). Kafka does not allow a transactional producer to be used by
  several threads at once (`ProducerFencedException`). [V: whether the consumer offset is sent in
  the same transaction — the plain-Kafka read-process-write pattern]
- Kafka EOS covers Kafka → Kafka only. A JMS send or an HTTP call is outside it. XA with
  `camel-jta` exists, but the docs warn of an inconsistency window if the JTA commit fails after
  the Kafka commit.
- **The real answer:** every consumer is idempotent. The event carries `eventId`; the status-board
  command carries `causationId = eventId`; the receiver (or a Camel idempotent consumer with a
  JDBC repository, module 05) drops what it has seen.

---

# In practice

The setting: an ESB made of Camel routes on Quarkus, Artemis queues and Kafka, on OpenShift,
deployed with GitOps (ArgoCD).

- **Two channels, two ways out.** The **partner channel** gives external partners REST and
  streaming APIs (behind an API gateway such as 3scale), built as ESB routes — partners never get a
  Kafka client to the core cluster. The **analytics channel** is Cluster Linking of curated topics
  from Confluent Platform on-prem to Confluent Cloud (Confluent course module 08). An ESB route that
  publishes a fact feeds both, so it must be a good Kafka producer.
- **ESB routes follow the same contract rules as any producer.** Schema registered from CI before
  deploy; `auto.register.schemas=false` in the platform's producer defaults; **one write owner per
  subject** (only `partner-order-status` writes `orders.order-status.updated.v1`); key =
  business entity; `acks=all`. A Camel route is not a reason for an exception. A legacy route
  that cannot use the Confluent serializer writes a raw topic, and a platform validator promotes it.
- **The blocked-partition trade-off is a design question, not a config detail.** A status change
  cannot wait behind a broken partner webhook. So: **one consumer group per destination** (status
  board route and partner webhook route read the same topic independently), and for unreliable
  destinations a **retry topic / DLT** after a few attempts instead of `breakOnFirstError=true`. Keep
  `breakOnFirstError` for flows where order is a safety property and the failure is short.
- Header names: ask the team that owns the platform whether they run upstream Camel (4.21+,
  `CamelKafka*`) or the Red Hat build (`kafka.*` until they upgrade). A Camel upgrade can silently
  drop keys in YAML routes that use string header names — a good question about their upgrade
  testing.

# Check yourself

1. *How do you make sure all events of one order stay in order?* Key = orderId via
   `KafkaConstants.KEY`; consume without `.threads()`/`seda`; order holds per partition.
2. *Your status-board consumer hits a 503 from a partner. What happens?* With
   `breakOnFirstError=true`: retries, seek back, retry forever, the partition is blocked and side
   effects repeat. With `false`: the record is skipped and lost. Better: separate groups per
   destination + DLT.
3. *Is the default Camel Kafka producer fire-and-forget?* No. Async routing: the route waits for
   the ack via callback without blocking a thread. `synchronous=true` blocks the thread.
4. *Who registers schemas in production?* CI, against the registry, with a compatibility check.
   Producers run with `auto.register.schemas=false`.
5. *Exactly-once with Camel?* At-least-once end to end; transactions only cover Kafka→Kafka;
   design idempotent consumers on `eventId`.
6. *Why did `${header.kafka.KEY}` print nothing?* Dotted name needs brackets, and since Camel 4.21
   the header is `CamelKafkaKey`.
7. *Camel or Quarkus Messaging for a new service?* Camel if it bridges protocols; Quarkus Messaging
   or Kafka Streams if it is Kafka-in, Kafka-out logic.

# Sources

- Camel Kafka component (4.x, options, headers, manual commit, batching, transactions): https://camel.apache.org/components/latest/kafka-component.html
- Camel 4.21 upgrade guide, "camel-kafka - potential breaking change" (header rename): https://camel.apache.org/manual/camel-4x-upgrade-guide-4_21.html
- Camel 4.22 upgrade guide (groupId UUID fix, batch key, batch headers): https://camel.apache.org/manual/camel-4x-upgrade-guide-4_22.html
- `KafkaConstants`, `KafkaProducer`, `KafkaHeaderFilterStrategy` source at tag `camel-4.22.1`: https://github.com/apache/camel/tree/camel-4.22.1/components/camel-kafka
- Camel Simple language: https://camel.apache.org/components/latest/languages/simple-language.html
- Quarkus — Kafka, Schema Registry and Avro (codegen path, Confluent serde setup): https://quarkus.io/guides/kafka-schema-registry-avro
- Confluent serializer configuration (`auto.register.schemas`, `use.latest.version`, subject strategies): https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html
- Example project: https://github.com/apache/camel-quarkus-examples/tree/main/kafka
