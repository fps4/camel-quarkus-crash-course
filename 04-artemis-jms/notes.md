---
title: 04 — Artemis and JMS with Camel, and the IBM MQ bridge
subtitle: "Queue vs log, the Artemis address model if you know IBM MQ, Camel JMS on Quarkus, the queue-to-Kafka bridge inside the ESB, XA from IBM MQ to Artemis. Artemis 2.57 / Camel 4.22. Written 2026-11-08."
---

# What this module is for

- Say in two sentences when a flow belongs on a queue (Artemis) and when on a log (Kafka), and
  defend it two follow-up questions deep. This is the "why does the ESB have both?" question.
- Read an Artemis configuration with IBM MQ eyes: address, queue, routing type, address settings.
- Know the Camel JMS options that decide behaviour in production: pooling, concurrency,
  acknowledgement, transactions, request-reply.
- Walk through the bridge from a queue into Kafka inside the ESB (queue → log) and the reverse
  (log → queue) in `esb-lab`, and know where each one can lose or duplicate a message.
- Plan an IBM MQ → Artemis move, including what XA buys and what it costs.

---

# 1. Queue semantics vs log semantics

A queue **hands work to one taker and forgets it**. A log **records a fact and keeps it** for
everyone. Most design mistakes come from using one where the other is needed.

| Property | Queue (Artemis, IBM MQ) | Log (Kafka) |
|---|---|---|
| After consumption | Message is **deleted** on ack | Record **stays** until retention (time/size) or compaction |
| Several consumers on one destination | **Competing consumers**: each message to one of them | **Consumer groups**: each group gets everything; inside a group, one consumer per partition |
| Scale-out limit | Add consumers freely | Max active consumers in a group = number of partitions |
| Progress tracking | **Per-message ack** by the broker | **Offset** per partition, committed by the consumer |
| Failure of one message | Broker **redelivers that message** (delay, max attempts) → DLQ | No per-message redelivery. Consumer retries in place (blocks the partition) or writes to a retry/DLT topic itself |
| Replay | No. Once acked, it is gone | Yes. Reset the offset, or start a new group |
| Filtering | **Selectors** on the broker (`JMSType = 'SCAN'`), also queue filters | None on the broker. Consumer reads everything and filters |
| Priority | JMS priority 0–9 | None. Use separate topics |
| Per-message expiry | **TTL** (`JMSExpiration`) → expiry address | None per record. Retention is per topic |
| Scheduled delivery | Yes (`deliveryDelay`, Artemis `_AMQ_SCHED_DELIVERY`) | None. Build it in the app |
| Ordering | FIFO per queue with **one** consumer. With many: use **message groups** (`JMSXGroupID`) | Per partition. Same key → same partition → in order |
| Slow consumer | Messages pile up in the broker: memory, then **paging** to disk, or producers blocked | Lag grows. The log is on disk anyway; nothing changes for the producer |
| Request-reply | Native: `JMSReplyTo`, `JMSCorrelationID`, temporary queues | Possible (reply topic + correlation id) but awkward and slow to set up per request |
| Transactions | Local JMS transactions; **XA** with other resources | Kafka transactions = exactly-once **inside Kafka** only. Not an XA resource |
| Typical size of the backlog | Small. A queue that grows is an incident | Large by design. Days or weeks of history |

**Which one to use.**

| Use Artemis (queue) when | Use Kafka (log) when |
|---|---|
| It is a **command** or a **task**: "print this item label", "show this on status board ZONE-D7", "send this file to a partner" | It is a **fact**: "item 0074123456 scanned at SCANNER-3", "order SO-1001234 is ready" |
| Exactly one system must do it, and you need per-message retry and a DLQ | Several systems read it now, and more will later (data & analytics teams, cloud copy, new apps) |
| The other side is a legacy **JMS client** or speaks AMQP/MQTT/STOMP/OpenWire | Consumers need **replay**: a new consumer, a bug fix, a rebuild of a read model |
| Synchronous **request-reply** over messaging | You want a **history** of state changes per key |
| Per-message TTL, priority or scheduled delivery matters | Throughput and fan-out matter more than per-message handling |

Rule of thumb: **command → queue, event → log.** The bridge from a queue into Kafka inside the ESB
is where a queue message becomes a log fact.

---

# 2. Artemis if you know IBM MQ

## 2.1 Address, queue, routing type

In IBM MQ the queue is the thing you send to. In Artemis you send to an **address**, and the
address routes to zero or more **queues**. The **routing type** decides how:

```
 producer ──► address "esb.tracking.scan.in"  (ANYCAST)  ──► queue "esb.tracking.scan.in"  ──► consumer 1 OR consumer 2
 producer ──► address "esb.lab.events"        (MULTICAST) ─┬► queue <sub-A>  ──► subscriber A
                                                           └► queue <sub-B>  ──► subscriber B
```

| Routing type | Behaviour | JMS | IBM MQ equivalent |
|---|---|---|---|
| **ANYCAST** | Each message to **one** queue on the address; consumers on that queue compete | `jms:queue:x` → address `x` + queue `x` | Local queue |
| **MULTICAST** | Each message to **every** queue on the address; one queue per subscription | `jms:topic:x` → address `x`, queue per subscriber | Topic object + subscriptions |

- Non-durable topic subscription: queue with a UUID name, deleted when the consumer leaves.
  Durable: queue named from `clientId` + subscription name; it keeps messages while the
  subscriber is away.
- **Fully qualified queue name** `address::queue` lets a client read one specific queue, e.g. a
  durable subscription queue.
- **Auto-create.** By default the broker creates addresses and queues when a client first uses
  them (`auto-create-addresses`, `auto-create-queues`, `auto-delete-*`). Handy in the lab — that is
  how `esb.tracking.scan.in` appeared. In production most teams turn auto-create off for named
  addresses and define them in config (or via the operator), so a typo does not create a silent
  new queue. Ask the team that runs the broker which way they chose.

## 2.2 Address settings: redelivery, dead letter, expiry, paging

Settings apply by **address match** with wildcards (`.` separates words, `#` = any words, `*` =
one word). This is what the lab broker uses (`00-lab-setup/artemis/etc-override/broker.properties`):

```properties
addressSettings."esb.#".maxDeliveryAttempts=3
addressSettings."esb.#".redeliveryDelay=1000
addressSettings."esb.#".deadLetterAddress=DLQ
addressSettings."esb.#".autoCreateDeadLetterResources=true
addressSettings."esb.#".deadLetterQueuePrefix=DLQ.
```

Meaning: when a consumer **rolls back** (transacted session) or the session closes without ack,
the broker redelivers after 1 s, at most 3 deliveries, then moves the message to queue
`DLQ.esb.tracking.scan.in` on address `DLQ`. `broker.properties` is the newer way to configure
Artemis (key=value on top of `broker.xml`); the XML names are the kebab-case versions
(`max-delivery-attempts`, `dead-letter-address`, …).

| Setting | What it does |
|---|---|
| `max-delivery-attempts`, `redelivery-delay`, `redelivery-delay-multiplier`, `max-redelivery-delay` | Broker-side retry with back-off |
| `dead-letter-address` (+ `auto-create-dead-letter-resources`) | Where poison messages go |
| `expiry-address`, `expiry-delay` | Where expired (TTL) messages go; default TTL if the producer sets none |
| `max-size-bytes`, `address-full-policy` | When the address is "full": **PAGE** (default, write to disk), **BLOCK** producers, **FAIL** (exception to producer), **DROP** (silent) |
| `global-max-size` (broker level) | Memory cap over all addresses; default 50% of `-Xmx` |
| `default-last-value-key` | **Last-value queue**: keep only the newest message per key — a queue-side cousin of a compacted topic. Useful for "current state of location ZONE-D7" |

**Paging** is Artemis' back-pressure: when an address exceeds its memory limit, new messages go
to page files on disk and are read back in order. It keeps the broker alive, but a paging broker
is slow and its disk must be sized. A queue that pages is a symptom: a consumer is down or too
slow. Alert on queue depth and paging, not on CPU.

## 2.3 One acceptor, many protocols

The lab's port 61616 accepts **Core, AMQP 1.0, OpenWire, MQTT and STOMP** on the same acceptor.
**Core** is Artemis' native protocol (the Artemis JMS client, what `esb-lab` uses). **AMQP 1.0** is
the standard wire protocol (Qpid JMS / `camel-amqp`, Quarkus Messaging, .NET and Python clients).
**OpenWire** lets old ActiveMQ 5 clients move without a code change. **MQTT** is for devices,
**STOMP** for simple text clients. That is one reason an organisation keeps Artemis next to Kafka:
legacy and device clients that will never speak Kafka.

## 2.4 High availability and OpenShift

| Policy | How it works | Watch out |
|---|---|---|
| **Shared store** | Primary and backup share one data directory (SAN/NFS); backup activates when the primary's file lock is released | Needs a reliable shared file system; naturally safe against split brain |
| **Replication** | Primary copies its journal over the network to the backup | Split brain risk. Use quorum voting or the pluggable lock manager (ZooKeeper, ≥3 nodes) |

The docs now say **primary/backup** (older docs: live/backup). Core clients learn the topology and
fail over (`ha=true`, `reconnectAttempts=-1` in the URL). In-flight, uncommitted work can still be
lost on failover; Artemis duplicate detection plus retries closes that gap.

**On OpenShift** the broker runs as a StatefulSet with a PersistentVolumeClaim per pod, managed by
an operator:

- **Red Hat AMQ Broker operator** — supported build. AMQ Broker 7.13 is an LTS release on
  OpenShift 4.16–4.20, console on HawtIO 4. [V: which Artemis version 7.13 is based on]
- **Upstream operator** — moved from ArtemisCloud to the **arkmq-org** GitHub organisation after
  Artemis became a top-level Apache project; it plans new `Broker` custom resources in group
  `arkmq.org`, replacing `ActiveMQArtemis` in `broker.amq.io`. [V: status of v3.0.0]
- The operator pattern on Kubernetes leans on **pod restart + persistent volume** and broker
  clustering more than on classic primary/backup pairs; on scale-down it can drain messages to
  another pod. [V: exact HA options in the current operator]

Artemis left ActiveMQ and is now its own Apache top-level project. Images moved from
`apache/activemq-artemis` (last 2.44) to **`apache/artemis`** (the lab runs 2.57.0, released
2026-09-09). Red Hat still sells it as **Red Hat AMQ Broker 7.x**.

## 2.5 IBM MQ ↔ Artemis mapping

| IBM MQ | Artemis | Note |
|---|---|---|
| Queue manager | Broker instance | |
| Local queue | ANYCAST address + queue of the same name | |
| Alias queue | Address + **divert** | Divert can also copy (non-exclusive) |
| Topic object + subscription (durable/non-durable) | MULTICAST address + subscription queue | Durable = `clientId.subscriptionName` |
| Remote queue def + transmission queue + sender/receiver channel | **Core bridge** or **AMQP broker connection** | Store-and-forward between brokers |
| MQ cluster (workload balancing) | **Cluster connection** + message load balancing / redistribution | |
| SVRCONN channel + listener | **Acceptor** (server side) | One acceptor, many protocols |
| CLNTCONN / CCDT | **Connector** / client URL with failover list | `(tcp://a:61616,tcp://b:61616)?ha=true` |
| `BOTHRESH` + `BOQNAME` (backout) | `max-delivery-attempts` + `dead-letter-address` | **Difference:** in IBM MQ the *client* (JMS classes, ACE) does the backout requeue; in Artemis the *broker* does it |
| Queue manager DLQ (`SYSTEM.DEAD.LETTER.QUEUE`) | `dead-letter-address` (per address match) | |
| MQMD `MsgId`, `CorrelId`, `ReplyToQ`, `Expiry`, `Priority`, `Persistence` | `JMSMessageID`, `JMSCorrelationID`, `JMSReplyTo`, `JMSExpiration`, `JMSPriority`, `JMSDeliveryMode` | Same JMS header model; Core adds `_AMQ_*` properties |
| MQMD `GroupId` + `MsgSeqNumber` | `JMSXGroupID` + `JMSXGroupSeq` | Artemis message groups |
| `MAXDEPTH` / queue full | `max-size-bytes` + `address-full-policy` (page/block/fail/drop) | Artemis pages to disk instead of refusing |
| Multi-instance queue manager (shared NFS) | Shared-store HA | |
| RDQM / Native HA | Replication HA | |
| `runmqsc` / MQSC | `broker.xml` / `broker.properties`, `bin/artemis` CLI, console, Jolokia | |
| CHLAUTH, OAM (`setmqaut`) | `security-settings` per address match (roles: send, consume, createAddress, …) + JAAS login | |
| Triggering | None. Consumers listen all the time; scale on queue depth with KEDA if needed [V] | |

---

# 3. Camel JMS on Quarkus

## 3.1 The wiring

```xml
<dependency><groupId>org.apache.camel.quarkus</groupId><artifactId>camel-quarkus-jms</artifactId></dependency>
<dependency><groupId>io.quarkiverse.artemis</groupId><artifactId>quarkus-artemis-jms</artifactId><version>3.14.3</version></dependency>
<dependency><groupId>io.quarkiverse.messaginghub</groupId><artifactId>quarkus-pooled-jms</artifactId></dependency>
```

```properties
quarkus.artemis.url=tcp://localhost:61616
quarkus.artemis.username=esb
quarkus.artemis.password=esb
quarkus.pooled-jms.max-connections=4
```

- `quarkus-artemis-jms` (Quarkiverse, not in the platform BOM) produces a `ConnectionFactory`
  bean from `quarkus.artemis.*`. Camel's `jms` component picks the single `ConnectionFactory`
  bean up by autowiring — no Java code.
- `quarkus-pooled-jms` wraps that factory in a **pool**. Why it matters: Camel JMS sends with
  Spring's `JmsTemplate`, which opens a connection, session and producer **per send** and closes
  them. The Camel docs: this "typically requires some caching in the JMS provider to avoid poor
  performance." Without a pool, every message to Artemis is a new TCP connection and login.
- Endpoints: `jms:queue:esb.tracking.scan.in`, `jms:topic:esb.lab.events`. `jms:x` without a prefix
  means queue.
- Camel 4 is on **Jakarta JMS 3** (`jakarta.jms.*`). Old `javax.jms` clients (IBM MQ classes for
  JMS before 9.3, ActiveMQ 5 client) need their Jakarta versions (`com.ibm.mq.jakarta.client`).

## 3.2 Options that matter

| Option | Default | What it decides |
|---|---|---|
| `concurrentConsumers` / `maxConcurrentConsumers` | 1 / – | Parallel listeners on the queue. More throughput, **less ordering** (see 3.4) |
| `transacted` | false | Local JMS transaction: commit on success, **rollback → broker redelivers** |
| `acknowledgementModeName` | `AUTO_ACKNOWLEDGE` | `AUTO_ACKNOWLEDGE`, `CLIENT_ACKNOWLEDGE`, `DUPS_OK_ACKNOWLEDGE`, `SESSION_TRANSACTED` |
| `cacheLevelName` | `CACHE_AUTO` | Listener caching. `CACHE_AUTO` becomes `CACHE_NONE` when `transacted=true` — Camel docs advise `CACHE_CONSUMER` for local (non-XA) transactions; no caching with XA |
| `exchangePattern` | InOnly | `InOut` = request-reply |
| `replyTo`, `replyToType` | – / `Temporary` | Fixed reply queue; `Temporary`, `Shared` (default when `replyTo` is set; slowest, uses a selector), `Exclusive` |
| `requestTimeout` | 20000 ms | How long the requester waits; header `CamelJmsRequestTimeout` overrides per message |
| `disableReplyTo` | false | Ignore an incoming `JMSReplyTo` (do not answer) |
| `jmsMessageType` | from body type | Force `Text`, `Bytes`, `Map`, `Object`, `Stream` |
| `selector` | – | JMS selector on the consumer |
| `timeToLive`, `deliveryDelay`, `priority` + `explicitQosEnabled` | – | Producer QoS |
| `subscriptionDurable`, `subscriptionShared`, `durableSubscriptionName`, `clientId` | false | Topic subscriptions. **Shared** (JMS 2) = several consumers share one subscription — the JMS version of a consumer group |
| `testConnectionOnStartup` | false | Fail the pod at start if the broker is unreachable |

## 3.3 Who retries — broker or Camel?

Camel JMS consumes through Spring's `DefaultMessageListenerContainer`. Its acknowledgement mode
decides what happens when the route fails. Spring's javadoc, plainly:

- `AUTO_ACKNOWLEDGE` (the default): the message is acknowledged **before** the listener runs. "No
  redelivery in case of an exception", and none if the JVM dies either.
- `CLIENT_ACKNOWLEDGE`: ack after success; best-effort redelivery.
- `transacted=true`: commit after success; "guaranteed redelivery" on exception or JVM crash.

| Setup | Route throws | Who retries | Where the poison message ends |
|---|---|---|---|
| Not transacted + Camel **dead letter channel** | Camel retries in memory, then sends to the DLC queue | **Camel** | `esb.tracking.scan.dlq` (Camel's choice) |
| Not transacted, no DLC | Camel logs the error; message already acked | Nobody | **Lost** |
| `transacted=true`, exception reaches the consumer | Session rolls back | **Broker**: `redelivery-delay`, `max-delivery-attempts` | `DLQ.esb.tracking.scan.in` (broker address settings) |
| `transacted=true` + Camel error handler that handles it (`esb-lab` today) | Camel retries, sends to DLC, then commits | Camel | DLC queue |

Broker redelivery survives a pod crash and spreads retries over other pods; Camel redelivery is
fast but lives in one pod's memory. Module 05 runs both side by side.

## 3.4 Ordering and message groups

One queue with one consumer is FIFO. `concurrentConsumers=2` (as in `esb-lab`, observed in the lab
as CONSUMER_COUNT 2) means two listeners take turns: scan 1 and scan 2 of the same item can be
processed in parallel and reach Kafka in the wrong order. Kafka keeps order per key only for what
the producer sends in order.

Fix: **message groups.** The producer sets `JMSXGroupID = itemTag`. Artemis then sends all messages
of one group to the same consumer while it lives; other groups spread over other consumers. This
is the queue version of "key → partition". Caveats: a dead consumer moves its groups (possible
overlap during the switch); a hot group limits throughput to one consumer; grouping across a
broker cluster needs a grouping handler and the docs call it "not recommended". The producer must
set the group — which the legacy scanning system may not do. Then the honest choice is one consumer
on that queue.

## 3.5 Request-reply

```java
// requester
from("timer:ping?period=5000")
    .setBody(constant("hi"))
    .to(ExchangePattern.InOut, "jms:queue:esb.lab.ping?requestTimeout=5000")
    .log("reply: ${body}");
// responder — Camel answers automatically when the message has JMSReplyTo
from("jms:queue:esb.lab.ping").transform(simple("pong ${body}"));
```

Camel creates a **temporary reply queue** per producer, sets `JMSReplyTo` and `JMSCorrelationID`,
and waits up to `requestTimeout`. With `replyTo=esb.lab.ping.reply&replyToType=Exclusive` it uses a
fixed queue that only this instance reads — needed when firewalls or security settings block
temporary queues, but each pod then needs its own reply queue name. Timeouts are the main design
issue: a late reply arrives to nobody.

## 3.6 Message mapping

Body type → JMS type: `String` → `TextMessage`, `byte[]` → `BytesMessage`, `Map` → `MapMessage`,
`Serializable` → `ObjectMessage` (avoid: Java serialisation). Camel headers become JMS properties,
with `.` and `-` encoded (`a.b` → `a_DOT_b`). Headers starting with `Camel` (including the Kafka
ones, `CamelKafka*` since Camel 4.21) are filtered by the JMS header filter strategy; any other
Kafka record header from the source would become a JMS property — remove what is not part of the
command contract.

## 3.7 Alternatives

| Option | What it is | When |
|---|---|---|
| `camel-quarkus-jms` + `quarkus-artemis-jms` | Core protocol, Artemis client (`esb-lab`, upstream examples) | Default for Artemis |
| `camel-quarkus-amqp` | Camel JMS on **Qpid JMS** over AMQP 1.0 | Broker-neutral wire protocol; Red Hat's AMQ Clients story |
| `camel-sjms2` | Camel's own JMS implementation, no Spring | Smaller, fewer features [V: status in Camel 4.22] |
| Quarkus Messaging AMQP (SmallRye Reactive Messaging) | `@Incoming`/`@Outgoing` channels, no Camel | Plain Quarkus services that only need a queue |

---

# 4. The bridges between Artemis and Kafka in `esb-lab`

## 4.1 Queue → log: `ScanBridgeRoute`

```java
errorHandler(deadLetterChannel("jms:queue:{{esb.queue.scan-dlq}}")
        .useOriginalMessage()                 // park the XML as received
        .maximumRedeliveries(3).redeliveryDelay(500).useExponentialBackOff() ...);
onException(InvalidMessageException.class, JacksonException.class)
        .maximumRedeliveries(0);              // a retry never fixes a poison message

// as run for the observations below; the reference app now also has
// &transacted=true&cacheLevelName=CACHE_CONSUMER (see point 1 further down)
from("jms:queue:{{esb.queue.scan-in}}?concurrentConsumers=2")
    .routeId("item-scan-bridge")
    .unmarshal().jacksonXml(ItemScan.class)                    // XML -> POJO
    .bean("orderStatusMapper", "validate")                     // 10-digit tag, required fields
    .setHeader(KafkaConstants.KEY, simple("${body.itemTag}"))  // key = item tag
    .idempotentConsumer(simple("${body.dedupKey}"))            // tag|location|time, Postgres
        .idempotentRepository("itemScanIdempotentRepo").skipDuplicate(true)
        .marshal().json()
        .to("kafka:{{esb.topic.item-scan}}?requestRequiredAcks=all&enableIdempotence=true")
    .end();
```

Observed in the lab (2026-11-08):

- Valid XML → one JSON record on `tracking.item-scan.recorded.v1`, key `0074123456`.
- Same XML again → dropped by the idempotent consumer. One row in Postgres table
  `camel_messageprocessed`: `processorname` `item-scan-bridge`, `messageid`
  `0074123456|SCANNER-3|2026-11-08T13:05:12Z`.
- Poison XML → straight to `esb.tracking.scan.dlq`, no retries.
- The broker's `DLQ.esb.tracking.scan.in` stayed **empty**: the JMS endpoint is not transacted, so
  Camel's DLC handled the failure and the broker never saw a rollback. That is the module 05
  contrast.
- Bug found: `${body.itemTag}` failed with `MethodNotFoundException` while `ItemScan` had public
  fields. Simple calls getters. `ItemScan` now has getters.

What a reviewer should say about it:

1. **Crash window** (found while writing this module; the reference app now has `transacted=true`).
   With the default `AUTO_ACKNOWLEDGE` the message is acked before the
   route runs. A pod killed between ack and Kafka send loses that scan. The DLC covers exceptions,
   not crashes. Fix: `transacted=true` (commit after the Kafka send) — then a crash means
   redelivery, and the idempotent consumer makes the redelivery harmless. Result: at-least-once
   into Kafka, de-duplicated by key.
2. **Ordering.** Two concurrent consumers, no message groups (§3.4).
3. **Idempotency store is shared** (Postgres), so it works across pods. An in-memory repository
   would not.
4. **Kafka producer** uses `acks=all` and idempotence: no duplicates from producer retries.
5. **The DLQ needs an owner** and a replay procedure, or it is a black hole.

## 4.2 Log → queue: `StatusBoardRoute`

Kafka `orders.order-status.updated.v1` (Avro) → filter `LOCATION_CHANGE`/`READY` → JSON
command on `jms:queue:esb.status-board.cmd` → POST to the partner webhook. Observed in the lab:
commands landed on `esb.status-board.cmd` (no consumer there; inspect them in the console).

Points: one Kafka fact becomes a **command** for exactly one system. The JMS send and the webhook
are not atomic — on a webhook failure the error handler retries the whole exchange, and the status
board may get the command twice. So the command carries `causationId = eventId` and the status
board de-duplicates. `breakOnFirstError=true` stops the Kafka consumer from committing past a
failed record.

---

# 5. IBM MQ → Artemis with XA (upstream `message-bridge`)

The upstream example
[camel-quarkus-examples/message-bridge](https://github.com/apache/camel-quarkus-examples/tree/main/message-bridge)
relays IBM MQ `DEV.QUEUE.1` → Artemis queue `in` inside one **XA transaction**:

- IBM MQ client `com.ibm.mq:com.ibm.mq.jakarta.client`, `MQXAConnectionFactory` built in a
  producer method (no Quarkus extension for IBM MQ); Artemis via `quarkus-artemis-jms`.
- Both factories wrapped by `quarkus-pooled-jms` with `quarkus.pooled-jms.transaction=xa`.
- Transaction manager **Narayana** (`camel-quarkus-jta`), Spring `JtaTransactionManager` handed
  to two JMS components `ibmmq` and `amq`; routes use `.transacted()`.
- Object store `quarkus.transaction-manager.object-store.directory` (`target/narayana` locally,
  `/storage/narayana` on a PVC on OpenShift), `enable-recovery=true`.
- Test keywords: `rollback` → the route throws after the Artemis send, both sides roll back, IBM MQ
  redelivers, the second attempt passes. `crash` → the JVM halts during commit; on restart,
  recovery finishes the commit.

**What XA buys:** the get from IBM MQ and the put to Artemis commit or roll back together. No loss,
no duplicate, without application code. The broker hand-off looks like one queue manager
moving a message to another.

**What XA costs:**

| Cost | Why |
|---|---|
| **State in the pod** | Narayana writes a transaction log. On OpenShift that needs a **persistent volume** (or a JDBC object store), and the pod must come back with the **same identity** to recover in-doubt transactions — StatefulSet thinking for an app that was stateless |
| **Recovery operations** | In-doubt transactions hold locks/messages on both brokers until recovery runs. Somebody must know how to read and resolve them |
| **Throughput** | Two-phase commit = extra round trips and forced log writes per message; no JMS caching under XA. Batch where possible |
| **Coupling** | Both brokers must be up for any message to move |
| **Kafka cannot join** | Kafka is **not an XA resource**. An Artemis → Kafka bridge is always at-least-once + idempotent consumer (or idempotent writes), never XA |

**When the simpler pattern is enough:** transacted consume on the source + idempotent target.
Consume from IBM MQ in a local transaction, send to Artemis, commit the IBM MQ session only after
the send succeeds. A crash between send and commit gives a **duplicate**, never a loss. If the
target de-duplicates (Artemis duplicate detection with `_AMQ_DUPL_ID` = IBM MQ message id, or the
consumer de-duplicates by business key), you get the same effective result with no transaction
log. Choose XA when duplicates are unacceptable and the target cannot de-duplicate — and accept the
PVC and the recovery runbook.

---

# In practice

The setting: an ESB made of Camel routes on Quarkus, Artemis queues and Kafka, on OpenShift,
deployed with GitOps (ArgoCD).

- **Kafka inside the ESB, next to Artemis.** Why both: a queue hands work to one taker, a log keeps
  a fact for many readers. Artemis carries what one system **hands to** another — commands, work
  distribution over competing consumers, request-reply, transactional hand-offs, and legacy
  JMS/AMQP/MQTT clients. Kafka carries the facts that many systems **read** — with retention,
  replay and a cloud copy for the data & analytics teams. The ESB's Camel routes are the adapters
  between them.
- **"Which flows stay on Artemis and which move to Kafka, and who decides?"** Default answer:
  commands, request-reply and transactional hand-offs stay; events with several consumers move —
  especially the ones the cloud and the data & analytics teams also need. Move one flow family at
  a time with a parallel run. The decision belongs to the architecture function with the domain
  owner of the data, not to the ESB team alone: the ESB team owns the pipe, the domain owns the
  meaning and the consumers. Ask who that is today.
- **Coming from IBM MQ.** If you know IBM MQ and IBM ACE/IIB, the patterns carry over: backout
  queues map to dead-letter addresses, MQMD to JMS headers, channels to acceptors (§2.5). For each
  queue-to-queue hand-off, choose transacted + idempotent or XA, and write down why. Where the ESB
  still bridges IBM MQ to Artemis, the `message-bridge` example is the reference design.
- **Operating Artemis on OpenShift.** Ask the team that owns the platform: Red Hat AMQ Broker
  operator or upstream? Persistent volumes on what storage class? HA by primary/backup, by
  clustering, or by pod restart + PVC? How do they alert on queue depth, paging and DLQ growth?
  Who replays a DLQ?
- **Ordering.** A good question to ask: "Do your producers set `JMSXGroupID`, or do you run single
  consumers where order matters?"

# Check yourself

1. *Why does the ESB need Artemis if it has Kafka?* Commands with per-message ack, redelivery,
   DLQ, TTL, priority, request-reply, XA, and clients that only speak JMS/AMQP/MQTT. Kafka gives
   retention, replay and fan-out. Different jobs.
2. *What is an Artemis address, and how does a JMS topic map onto it?* Address = where you send;
   queues bound to it = where you consume. Topic = MULTICAST address with one queue per subscription.
3. *A consumer fails on a message. Who retries?* Depends on the session: not transacted → Camel's
   error handler or nobody; transacted with rollback → the broker, by address settings, then DLQ.
4. *Why a JMS connection pool in Camel?* `JmsTemplate` opens and closes a connection per send.
5. *Two consumers on one queue — is order kept?* No. Message groups by business key, or one consumer.
6. *Is the `esb-lab` bridge at-least-once?* Only since it has `transacted=true`: with AUTO_ACK the
   message is acked before processing and a crash loses it. Transacted + the idempotent consumer
   gives at-least-once with de-duplication.
7. *Why not XA from Artemis to Kafka?* Kafka is not an XA resource. Use transacted consume +
   idempotent producer and consumer-side de-duplication.
8. *What does XA cost on OpenShift?* Transaction log on a PVC, stable pod identity for recovery,
   slower commits, in-doubt handling, both brokers coupled.
9. *IBM MQ backout vs Artemis DLA?* MQ: the client requeues to `BOQNAME` after `BOTHRESH`. Artemis:
   the broker counts deliveries and moves the message itself.

# Sources

- Artemis docs (2.57): address model https://artemis.apache.org/components/artemis/documentation/latest/address-model.html ·
  HA https://artemis.apache.org/components/artemis/documentation/latest/ha.html ·
  message grouping https://artemis.apache.org/components/artemis/documentation/latest/message-grouping.html ·
  paging https://artemis.apache.org/components/artemis/documentation/latest/paging.html
- Camel JMS component: https://camel.apache.org/components/latest/jms-component.html
- Spring `AbstractMessageListenerContainer` (ack modes): https://docs.spring.io/spring-framework/docs/current/javadoc-api/org/springframework/jms/listener/AbstractMessageListenerContainer.html
- Upstream example — message-bridge: https://github.com/apache/camel-quarkus-examples/tree/main/message-bridge
- Quarkiverse Artemis: https://docs.quarkiverse.io/quarkus-artemis/dev/index.html
- Red Hat AMQ Broker 7.13 release notes: https://docs.redhat.com/en/documentation/red_hat_amq_broker/7.13
- arkmq operator (formerly ArtemisCloud): https://github.com/arkmq-org/activemq-artemis-operator
