---
title: "05 — Reliability: errors, retries, duplicates, transactions"
subtitle: Error handlers, onException, dead letter channels, who retries (Camel, broker, Kafka, sender), idempotent consumers, transactions and dual writes. Grounded in what the lab showed on 2026-11-08.
---

# What this module is for

This is the most important module of the course. An integration design is judged on what happens
when things fail, not on the happy path.

- Classify every failure as **poison** (retry never helps) or **transient** (retry may help), and
  route each to the right place.
- Say for any flow **who retries** — Camel in memory, the broker, Kafka re-poll, or the sender —
  and what is lost on a crash in each case.
- Explain why the ESB is **at-least-once**, and where duplicates are removed.
- Spot a **dual write** in a route and name the fix.

---

# 1. What the lab showed (esb-lab on the lab host, 2026-11-08)

| Experiment | What happened | Lesson |
|---|---|---|
| Broker redelivery (`transacted=true`, `noErrorHandler`, Postgres down) | 3 deliveries ~12–14 s apart, then `DLQ.esb.tracking.scan.in` **and** a copy in the default `DLQ` queue | The broker can own retries and keeps the message through a crash; check what else is bound to the DLA |
| Poison XML on `esb.tracking.scan.in` | Straight to `esb.tracking.scan.dlq`, original XML, **no retries** (`onException(...).maximumRedeliveries(0)`) | Classify early; do not burn retries on poison |
| Same valid scan sent twice | One Kafka record; one row in `camel_messageprocessed` | Idempotent consumer with a business key works across restarts |
| **Postgres stopped**, valid scan sent | 4 attempts in ~6.5 s (`CannotCreateTransactionException`), then the **good message went to the DLQ** | A short outage turns good messages into "dead" ones. The DLQ now needs a replay path, or the retry window must be longer than a typical outage |
| Partner webhook returns 503 (Kafka consumer, `breakOnFirstError=true`) | 5 attempts with back-off (1 s, 2 s, 4 s, 8 s), then "Will seek consumer to offset …", repeat forever. **The next event on the same partition was blocked** until the partner recovered. Lag went to 0 within seconds after recovery | Blocking retry keeps order and loses nothing, but one broken destination stops the whole partition |
| Same 503 run | **4 extra status-board commands** on `esb.status-board.cmd` — each re-poll re-ran the whole route, including the JMS send before the HTTP call | Dual write: two side effects, no shared transaction. Receivers must be idempotent (`causationId`) |
| Invalid partner JSON | First a **500** (REST binding does not marshal error bodies), after the fix a 400 with a reason | Test error responses, not just the happy path |

---

# 2. Camel error handling in five lines

```java
errorHandler(deadLetterChannel("jms:queue:esb.tracking.scan.dlq")   // what happens when retries are exhausted
        .useOriginalMessage()                                         // park the message as received
        .maximumRedeliveries(3).redeliveryDelay(500)
        .useExponentialBackOff().backOffMultiplier(2));
onException(InvalidMessageException.class, JacksonException.class)   // poison: override the policy
        .maximumRedeliveries(0);
```

| Piece | Meaning |
|---|---|
| **DefaultErrorHandler** | Retries (0 by default), then the exception goes **back to the consumer**. For JMS that means rollback if transacted; for Kafka, `breakOnFirstError` decides; for HTTP, a 500 |
| **DeadLetterChannel** | Retries, then sends the exchange to an endpoint and marks it **handled** — the consumer sees success, commits / acks |
| **NoErrorHandler** | No retries in Camel at all. Use it when the broker should do the retrying |
| **TransactionErrorHandler** | Used automatically in `.transacted()` routes; retries inside the transaction, then rolls back |
| `onException(X.class)` | Picks a policy by exception type (closest match in the class hierarchy). Scope: the RouteBuilder (or a single route) |
| `.handled(true)` | Stop the exception here; build a response (e.g. HTTP 400). Without it the exception continues to the error handler / DLC |
| `.continued(true)` | Ignore the exception and carry on with the next step. Rarely right |
| `.retryWhile(predicate)` | Retry by condition (e.g. HTTP status 503 or 429 only) |
| `useOriginalMessage()` | DLC gets the body as it entered the route, not the half-transformed version |
| `onWhen(...)`, `onRedelivery(...)`, `onPrepareFailure(...)` | Conditions, hooks before each retry, enrich the DLQ message with the reason |

**Redelivery happens at the failed step, not from the start of the route.** That is why the JMS
send in the status-board route did not repeat during Camel's own 5 attempts — but did repeat on
each Kafka re-poll, which starts the route again.

**Redelivery blocks the consumer thread** by default (the delay is a sleep). With
`asyncDelayedRedelivery()` the thread is freed, but for Kafka and JMS the consumer still waits for
the exchange to finish before moving on.

---

# 3. Who retries? The four places

| Retrier | How | Survives a pod crash? | Ordering | Cost |
|---|---|---|---|---|
| **Camel, in memory** (error handler) | Redelivery policy in the route | **No** — in-flight retries are lost; JMS/Kafka redeliver from the last ack/commit | Kept per consumer thread | Simple; delays hold a thread |
| **Broker (Artemis)** | `transacted=true` on the JMS endpoint + exception reaches the consumer → session rollback → broker redelivers after `redeliveryDelay`, up to `maxDeliveryAttempts`, then `DLQ.<queue>` (set in `broker.properties`) | **Yes** — the message never left the broker | Redelivered message may go to another consumer; per-queue order is lost unless message groups | Needs transacted sessions; broker settings per address |
| **Kafka re-poll** | `breakOnFirstError=true` → seek back to the failed offset | **Yes** — offset not committed | **Kept** per partition | Blocks the partition (observed) |
| **The sender** | Return 503/429 to the HTTP caller | n/a | Caller decides | Pushes the problem to the partner — fine at the edge, with `Retry-After` |

Design rule for the ESB: **one retrier per hop, chosen on purpose.** Camel retries *plus* broker
retries multiply (3 × 3 = 9 attempts) and make timing hard to reason about.

Broker redelivery, observed: with Camel's error handler off (`noErrorHandler()`) and the endpoint
`transacted=true`, Postgres down → three listener failures, then Artemis moved the message to
`DLQ.esb.tracking.scan.in` exactly as `broker.properties` says (Jolokia confirmed
`maxDeliveryAttempts: 3`, `redeliveryDelay: 1000`, `deadLetterAddress: DLQ`, prefix `DLQ.`). The
default `DLQ` queue from `broker.xml` got a **copy** as well — two queues on one address. Clean that
up in a real broker.

**The crash window (found in review).** Camel JMS consumes through Spring's
`DefaultMessageListenerContainer`. With the default `AUTO_ACKNOWLEDGE` it acks the message
**before** the route runs, so a pod killed mid-route loses that message — the DLC only covers
exceptions, not crashes. The reference bridge therefore uses `transacted=true` (ack on completion;
a crash means redelivery, which the idempotent consumer absorbs) plus `cacheLevelName=CACHE_CONSUMER`
(transacted falls back to `CACHE_NONE`, which opened a new consumer per receive — the console showed
0 consumers between messages).

---

# 4. Poison vs transient — classify by exception

| Class | Examples | Handling |
|---|---|---|
| **Poison** (data) | XML/JSON parse error, schema/validation failure, unknown enum, Avro deserialization error | No retry → DLQ/DLT with reason. Alert the *owner of the data*, not ops |
| **Transient** (environment) | DB down, broker failover, HTTP 503/429, timeout, `NotEnoughReplicas` | Retry with back-off; long enough to ride out a normal failover. Then DLQ with a replay plan |
| **Unknown** | NPE in your code, 500 from a partner | Treat as transient a few times, then DLQ; fix the code |

The Postgres experiment is the warning: **a DLQ full of good messages is a normal day after an
outage.** Plan for replay from day one — a small route or CLI that moves messages from the DLQ back
to the input after the cause is fixed (Artemis console can move messages; for Kafka a replay
consumer from the DLT).

---

# 5. Kafka consumer failures: three patterns

| Pattern | How in Camel | Keeps order | Blocks others | Use for |
|---|---|---|---|---|
| **Blocking retry** (lab default) | `breakOnFirstError=true` + error handler | Yes | **Yes**, the whole partition | Flows where order beats availability (state machines, ledgers) |
| **Dead-letter topic** | `deadLetterChannel("kafka:<topic>.<consumer>.dlt?...")`, commit and move on | No (failed record skipped) | No | Most notification flows |
| **Retry topics** | DLC to `...retry-1m`, a second route consumes it with a delay, then `...dlt` | No | No | Partner APIs that recover in minutes |
| **Split consumers per destination** | One consumer group per side effect (status board, partner webhook) | Per destination | Only that destination | Independent destinations — **the fix for the lab's coupling** |

DLT sketch for the status-board route (Avro body needs the Avro serializer again) [V]:

```java
errorHandler(deadLetterChannel("kafka:orders.order-status.updated.v1.status-board.dlt"
            + "?valueSerializer=io.confluent.kafka.serializers.KafkaAvroSerializer"
            + "&schemaRegistryURL={{esb.schema-registry-url}}")
        .useOriginalMessage()
        .maximumRedeliveries(4).redeliveryDelay(1000).useExponentialBackOff()
        .onPrepareFailure(e -> e.getMessage().setHeader("dlt.reason",
                e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class).getMessage())));
// and remove breakOnFirstError=true from the consumer URI
```

---

# 6. Idempotent consumer

```java
.idempotentConsumer(simple("${body.dedupKey}"))       // itemTag|location|scannedAt
    .idempotentRepository("itemScanIdempotentRepo")   // JdbcMessageIdRepository on Postgres
    .skipDuplicate(true)
```

| Choice | Options | Advice |
|---|---|---|
| **Key** | Transport message ID (JMSMessageID, Kafka topic+partition+offset) vs **business key** | Business key. A resend by the legacy system gets a new JMSMessageID — the lab sends the same XML twice and the business key catches it |
| **Store** | Memory (`MemoryIdempotentRepository`), JDBC, Infinispan, Redis, Kafka (`KafkaIdempotentRepository`) | Shared and persistent once there are 2+ pods or restarts. Memory only for tests |
| **Eager** (default `true`) | Key added *before* processing; removed if processing fails (`removeOnFailure=true`) | Default blocks a concurrent duplicate while the first is in flight. A crash between add and finish can leave a key for a message that was never published [V] |
| **Retention** | Table grows forever | Add a clean-up job (delete rows older than the replay window) |
| **Scope** | `processorName` per route | Lets several routes share one table |

Idempotency in the receiver beats exactly-once in the pipe. Every command the ESB emits carries a
`causationId` (the event ID that caused it) so the receiver can de-duplicate too.

---

# 7. Transactions — what each one covers

| Mechanism | Covers | Does not cover |
|---|---|---|
| JMS local transaction (`transacted=true`) | Consume + send on the **same** broker connection, atomically | Kafka, DB, HTTP |
| JDBC local transaction | The DB | The broker |
| **JTA/XA** (Narayana; `camel-quarkus-jta`, `quarkus-pooled-jms` with `transaction=xa`) | Several XA resources: IBM MQ + Artemis + DB. The upstream `message-bridge` example moves IBM MQ → Artemis this way | **Kafka is not an XA resource.** HTTP is not either |
| Kafka transactions (producer `transactional.id`) | Atomic writes to several Kafka topics + consumer offsets (read-process-write inside Kafka) | Anything outside Kafka |
| **Outbox** | DB change + event, via a table polled or captured by CDC (Debezium) | — the usual answer for "DB and Kafka together" |

XA costs: a transaction log (object store) that needs a **persistent volume** on OpenShift,
recovery after a crash, slower throughput, and operational knowledge few teams have. Use it only
where losing or duplicating a message is unacceptable and both ends support it (MQ → Artemis
migration bridges). Everywhere else: at-least-once + idempotent receivers.

**Dual writes in the lab:** `StatusBoardRoute` sends to JMS, then calls HTTP. No transaction spans
both. Options, best first: (1) split into two consumer groups — each route has one side effect;
(2) make both receivers idempotent on `causationId`; (3) put only the JMS send in the route and let
a separate consumer of the command queue call the partner.

---

# In practice

The setting: an ESB made of Camel routes on Quarkus, Artemis queues and Kafka, on OpenShift,
deployed with GitOps (ArgoCD).

- Ask the team that owns the platform: **"What is the ESB's delivery guarantee per flow, and who
  owns the DLQs?"** If nobody owns them, they fill up after every outage and nobody replays.
  Owner = the team that owns the data.
- For flows that reach OT or safety-related systems: no commands from IT into OT (IEC 62443
  conduits), and the retry policy must be written down — a status change replayed 30 minutes late
  can be worse than none. Add a **time-to-live**: commands older than N minutes are dropped and
  logged (JMS `timeToLive`, or check `issuedAt` in the receiver).
- Organisations that run critical or regulated operations fall under NIS2 incident reporting
  (24 h early warning). That needs evidence of message loss or delay: DLQ depth and consumer lag
  are the two metrics to alert on (module 07).
- Run the blocked-partition experiment with the team that builds the routes. It shows in a few
  minutes why one broken destination must not share a consumer group with a status change, and it
  makes the design choice (split groups, DLT, or blocking retry) concrete.

# Check yourself

1. *Difference between DefaultErrorHandler and DeadLetterChannel?* After retries, default gives the
   exception back to the consumer (rollback / no commit / HTTP 500); DLC parks the message and
   reports success.
2. *A route retries 3 times and the JMS endpoint is transacted with broker max-delivery-attempts 3.
   How many attempts?* Up to 3 × (1 + 3) = 12 deliveries of work — pick one retrier.
3. *Why did good messages land in the DLQ in the lab?* The retry window (~6 s) was shorter than the
   outage. Longer back-off, broker redelivery, or replay tooling.
4. *How do you get exactly-once from Artemis to Kafka?* You do not. At-least-once with an idempotent
   consumer or idempotent receivers. XA cannot include Kafka.
5. *Your consumer calls two systems. One fails. What happens on retry?* The other gets the call
   again (observed: 4 duplicate commands). Split consumers, or idempotent receivers.

# Sources

- Camel error handling: https://camel.apache.org/manual/error-handler.html · exception clause: https://camel.apache.org/manual/exception-clause.html
- Dead Letter Channel: https://camel.apache.org/components/latest/eips/dead-letter-channel.html
- Idempotent consumer: https://camel.apache.org/components/latest/eips/idempotentConsumer-eip.html
- Kafka component (`breakOnFirstError`): https://camel.apache.org/components/latest/kafka-component.html
- Artemis undelivered messages (redelivery, DLA): https://activemq.apache.org/components/artemis/documentation/latest/undelivered-messages.html
- Camel Quarkus example `message-bridge` (IBM MQ → Artemis, XA): https://github.com/apache/camel-quarkus-examples/tree/main/message-bridge
- Transactional outbox: https://microservices.io/patterns/data/transactional-outbox.html
