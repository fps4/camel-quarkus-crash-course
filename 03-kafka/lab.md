---
title: "03 — Lab: Kafka and Avro with Camel Quarkus"
subtitle: "Environment: your workstation with quarkus dev through the tunnel to the lab host (or the esb-lab container on the lab host). About 2 hours. Written 2026-11-08."
---

# Before you start

- 00-lab-setup done: on the lab host `dc-a`, `sr-a`, `artemis`, `postgres`, `wiremock`, `kafka-ui`
  are up; on your workstation `tunnel.sh` is running.
- **Run the app in one place only.** Either `quarkus dev` on your workstation (this lab assumes it)
  or the `esb-lab` container on the lab host — never both: they share the consumer group
  `esb-status-board`.
- Container variant: after each code change run, on the lab host,
  `cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile app up -d --build esb-lab`,
  read logs with `docker logs -f esb-lab`, and add port 8080 to the tunnel
  (`ssh -N -L 8080:localhost:8080 labhost`). Step 7 (the old jar) works only on your workstation.
- If you already ran `esb-lab`, the topic, the subject and the group exist. That is fine.

Shortcuts used below:

```bash
LAB=~/labs/camel-quarkus-crash-course
TOPIC=orders.order-status.updated.v1
SUBJ=$TOPIC-value
```

# Step 1 — project, dependencies, Avro code generation

Start from your module 02 project, or copy the reference app:

```bash
mkdir -p ~/labs && cp -r $LAB/esb-lab ~/labs/esb03 && cd ~/labs/esb03
```

The Kafka + Avro part of `pom.xml` (as in `esb-lab/pom.xml`):

```xml
<properties>
  <confluent.version>8.4.0</confluent.version>
</properties>

<repositories>
  <!-- io.confluent:kafka-avro-serializer is only published here -->
  <repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
    <snapshots><enabled>false</enabled></snapshots>
  </repository>
</repositories>

<!-- Kafka (Confluent Platform dc-a) + Avro with Confluent Schema Registry -->
<dependency><groupId>org.apache.camel.quarkus</groupId><artifactId>camel-quarkus-kafka</artifactId></dependency>
<dependency><groupId>io.quarkus</groupId><artifactId>quarkus-avro</artifactId></dependency>
<dependency><groupId>io.quarkus</groupId><artifactId>quarkus-confluent-registry-avro</artifactId></dependency>
<dependency><groupId>io.quarkus</groupId><artifactId>quarkus-rest-client</artifactId></dependency>
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-avro-serializer</artifactId>
  <version>${confluent.version}</version>
  <exclusions>
    <exclusion><groupId>jakarta.ws.rs</groupId><artifactId>jakarta.ws.rs-api</artifactId></exclusion>
  </exclusions>
</dependency>
<dependency><groupId>com.fasterxml.jackson.dataformat</groupId><artifactId>jackson-dataformat-csv</artifactId></dependency>
```

The `generate-code` goal of `quarkus-maven-plugin` must be enabled (it is in `esb-lab`). The schema
is `src/main/avro/order-status-updated.avsc`. Generate and look:

```bash
mvn -q compile
ls target/generated-sources/avsc/lab/esb/events/       # OrderStatus.java  OrderStatusUpdated.java
grep -n 'public java.lang.String getOrderId\|getClassSchema' target/generated-sources/avsc/lab/esb/events/OrderStatusUpdated.java
```

The generated class has getters and a builder — that is why `${body.orderId}` works in Simple.
Start the app and check the versions in the first log lines:

```bash
mvn quarkus:dev          # or: quarkus dev
# expect: Apache Camel 4.22.1 (esb-lab) ... started   (observed: ~2.6 s, Quarkus 3.40.1, JVM mode)
```

# Step 2 — the producer route: key = orderId, Avro value

Read `PartnerApiRoute.java`: REST in, `OrderStatusMapper.toEvent` builds an `OrderStatusUpdated`,
`setHeader(KafkaConstants.KEY, simple("${body.orderId}"))`, then `to("kafka:...")` with
`KafkaAvroSerializer`, `requestRequiredAcks=all`, `enableIdempotence=true`.

Add two things to see what the producer gives back. Append `+ "&recordMetadata=true"` to the Kafka
URI, and after the `.to(...)` add:

```java
.log("key=${header.CamelKafkaKey} meta=${header.CamelKafkaRecordMeta}")
.log("wrong forms: [${header.kafka.KEY}] [${header[kafka.KEY]}]")
```

Save (dev mode reloads on the next request) and send an event:

```bash
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' \
  -d @samples/order-status-location-change.json
# {"status":"accepted","eventId":"..."}   HTTP 202
```

Expected log: `key=SO-1001234 meta=[orders.order-status.updated.v1-0@<offset>]`, and the
two wrong forms empty (notes §0 and §6). Then the registry:

```bash
curl -s localhost:8081/subjects | jq                    # [..., "orders.order-status.updated.v1-value"]
curl -s localhost:8081/subjects/$SUBJ/versions/latest | jq '{version, id, name: (.schema|fromjson|.name)}'
curl -s localhost:8081/config | jq                      # {"compatibilityLevel":"BACKWARD"}
```

Only a `-value` subject exists: the key is a plain string (`StringSerializer`, the default).

In kafka-ui (http://localhost:8070) → dc-a → Topics → `orders.order-status.updated.v1` →
Messages: the key is the order id; the topic has **1 partition** (auto-created). Open the
**Headers** of the record. Do you see HTTP headers such as `User-Agent` or `Accept`? That is the
header leak from notes §5. [V] Fix it with `.removeHeaders("*")` right after the mapper (before
`setHeader(KafkaConstants.KEY, ...)`), send again, and compare.

# Step 3 — read the topic with the Avro console consumer

```bash
ssh labhost docker exec sr-a kafka-avro-console-consumer \
  --bootstrap-server dc-a:29092 --topic $TOPIC --from-beginning \
  --property schema.registry.url=http://sr-a:8081 \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --timeout-ms 10000
```

Without the `key.deserializer` line the tool tries to read the key as Avro and fails with
"Unknown magic byte". [V: on CP 8.4 (Kafka 4.x) `--property` may print a deprecation warning in
favour of `--formatter-property`]

# Step 4 — the consumer route and its group

Read `StatusBoardRoute.java`: `groupId=esb-status-board`, `autoOffsetReset=earliest`,
`KafkaAvroDeserializer` + `specificAvroReader=true`, `breakOnFirstError=true`, a filter on
`LOCATION_CHANGE`/`READY`, then a JMS command and the partner webhook. After step 2 you should have
seen `Status board + partner notified: 200` in the log.

```bash
ssh labhost docker exec dc-a kafka-consumer-groups --bootstrap-server dc-a:29092 \
  --describe --group esb-status-board
# GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID ...   -> LAG 0
```

Now send an event the filter drops:

```bash
curl -s -X DELETE localhost:8089/__admin/requests                    # clear WireMock's journal
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' \
  -d '{"orderId":"SO-1005501","status":"delayed","partner":"globex"}'
curl -s localhost:8089/__admin/requests | jq '.meta.total'             # 0 — no webhook call
```

Describe the group again: LAG is still 0. A filtered record is "done": the offset moves on.

# Step 5 — poison record, blocked partition

`samples/order-status-ready-fail.json` has `"location": "FAIL"`; WireMock answers 503 for it.
Send it, then **right after** a valid event for another order:

```bash
curl -s -X DELETE localhost:8089/__admin/requests
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' -d @samples/order-status-ready-fail.json
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' -d @samples/order-status-location-change.json
```

Both POSTs return 202 — the producer side is fine. Watch the dev-mode log for about a minute.
What we observed on 2026-11-08:

- 5 attempts per cycle (1 + 4 redeliveries; back-off 1 s, 2 s, 4 s, 8 s), WARN per retry, then ERROR.
- `KafkaRecordStreamingProcessor ... Will seek consumer to offset ...` — and the cycle starts again,
  without end (20 webhook calls in ~70 s).
- The SO-1001234 event behind it is **not** processed.

Check it:

```bash
curl -s localhost:8089/__admin/requests | jq '[.requests[] | select(.responseDefinition.status==503)] | length'
ssh labhost docker exec dc-a kafka-consumer-groups --bootstrap-server dc-a:29092 --describe --group esb-status-board
# LAG 2 (the FAIL record and the SO-1001234 record behind it)  [V: may show 1 if a commit raced]
```

Open the Artemis console (http://localhost:8161, esb/esb) → queue `esb.status-board.cmd`: the
SO-1001801 command is there **several times**, one per cycle. The JMS send happens before the webhook,
and every re-poll runs the whole route again.

Unblock it by removing the 503 stub (then the FAIL record matches the happy-path stub):

```bash
ID=$(curl -s localhost:8089/__admin/mappings | jq -r '.mappings[] | select(.response.status==503) | .id')
curl -s -X DELETE localhost:8089/__admin/mappings/$ID
```

Within one cycle both records go through, and LAG drops to 0. Put the stub back:

```bash
curl -s -X POST localhost:8089/__admin/mappings/reset      # reloads the mappings from files
```

**Think about it:** the status board waited for a partner. How would you split
`StatusBoardRoute` so that cannot happen? (Two routes, two `groupId`s; the partner route uses a
dead-letter topic after N attempts instead of `breakOnFirstError`. Notes §4 and "In practice".)

# Step 6 — exercise: the key decides the order

The lab topic has one partition, so keys cannot show anything there. Make a 3-partition topic:

```bash
ssh labhost docker exec dc-a kafka-topics --bootstrap-server dc-a:29092 \
  --create --topic lab03.keys --partitions 3 --replication-factor 1
```

Add `src/main/java/lab/esb/routes/KeyLabRoute.java` (plain strings, no Avro):

```java
package lab.esb.routes;

import java.util.concurrent.atomic.AtomicLong;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

@ApplicationScoped
public class KeyLabRoute extends RouteBuilder {
    private static final String[] ORDERS = {"SO-1001234", "SO-1005501", "SO-1001001"};
    private final AtomicLong seq = new AtomicLong();

    @Override
    public void configure() {
        from("timer:keys?period=500&repeatCount=12").routeId("lab03-produce")
            .process(e -> {
                long n = seq.incrementAndGet();
                String order = ORDERS[(int) (n % 3)];
                e.getIn().setBody(order + " seq=" + n);
                e.getIn().setHeader(KafkaConstants.KEY, order);           // A: key = orderId
                // e.getIn().setHeader(KafkaConstants.KEY, "ALL");        // B: constant key
                // e.getIn().setHeader(KafkaConstants.KEY, java.util.UUID.randomUUID().toString()); // C: per-event key
            })
            .to("kafka:lab03.keys?recordMetadata=true")
            .log("sent key=${header.CamelKafkaKey} -> ${header.CamelKafkaRecordMeta}");

        from("kafka:lab03.keys?groupId=lab03-keys&autoOffsetReset=earliest&consumersCount=3")
            .routeId("lab03-consume")
            .log("${threadName} p=${header.CamelKafkaPartition} off=${header.CamelKafkaOffset} ${body}");
    }
}
```

Run A, then B, then C (edit, save, the timer starts again on reload). For each, answer:

- A — each order always lands on the same partition; per-order `seq` is in order; three threads
  share the work.
- B — everything on **one** partition: global order, but one thread does all the work and two sit
  idle. On a real topic: a hot partition and no scaling. This is the "constant key" mistake.
- C — one order's events spread over all partitions and threads: `seq` for one order can be
  processed out of order. For a location change this means the status board can show the
  **older** location.

Remove `KeyLabRoute.java` when done.

# Step 7 — exercise: schema evolution under BACKWARD

**7a. Keep the old build.** Stop dev mode (`q`), then:

```bash
mvn -q package -DskipTests && rm -rf /tmp/esb-v1 && cp -r target/quarkus-app /tmp/esb-v1
```

**7b. Compatible change.** Add an optional field to the `.avsc` (after `location`):

```json
{ "name": "channel", "type": ["null", "string"], "default": null, "doc": "Added in v2" },
```

Start dev mode with the status-board route switched off, so only the producer runs, and send one
event:

```bash
ESB_STATUS_BOARD_AUTO_STARTUP=false mvn quarkus:dev
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' -d @samples/order-status-location-change.json
curl -s localhost:8081/subjects/$SUBJ/versions | jq        # [1,2]
```

Stop dev mode and run the **old** build (v1 class, no `channel` field). It consumes the v2 event:

```bash
java -jar /tmp/esb-v1/quarkus-run.jar        # prod profile; defaults point at localhost via the tunnel
# expect: Status board + partner notified: 200     (Ctrl-C after)
```

The deserializer reads with the writer schema (v2, by ID from the registry) and resolves to the
reader schema (v1 class); the unknown field is dropped.

**7c. Breaking change.** Add a required field (no default) and set it in `OrderStatusMapper.toEvent`
(`.setRegion("EU")`, otherwise the Avro builder fails first):

```json
{ "name": "region", "type": "string" },
```

Start dev mode, POST again. Expected: the serializer's register call is rejected with HTTP 409
"Schema being registered is incompatible with an earlier schema" [V exact text], and the partner
gets **503** from `onException(Exception.class)`. Question for the design: a contract error is not
transient — should the partner really be told "retry later"? Revert the `.avsc` and the mapper.

# Step 8 — optional: manual commit

In `StatusBoardRoute` add `&allowManualCommit=true&autoCommitEnable=false` to the URI, and after
the filter's `.end()` (so filtered records are committed too):

```java
// import org.apache.camel.component.kafka.consumer.KafkaManualCommit
.process(e -> e.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class).commit())
```

Repeat step 5. The poison record is still re-polled, but now nothing is committed before the
webhook succeeded. Then remove `breakOnFirstError`: the record is skipped and lost.

# Teardown

```bash
ssh labhost docker exec dc-a kafka-topics --bootstrap-server dc-a:29092 --delete --topic lab03.keys
ssh labhost docker exec dc-a kafka-consumer-groups --bootstrap-server dc-a:29092 --delete --group lab03-keys
curl -s -X POST localhost:8089/__admin/mappings/reset
# Only if step 7 left a v2 or v3 in the registry (lab only — never delete versions in a governed registry):
curl -s -X DELETE localhost:8081/subjects/$SUBJ/versions/2
curl -s -X DELETE "localhost:8081/subjects/$SUBJ/versions/2?permanent=true"
rm -rf /tmp/esb-v1
```

Purge `esb.status-board.cmd` in the Artemis console (Queues → Operations → `removeAllMessages`) if
the duplicates bother you. Keep `orders.order-status.updated.v1` and the group
`esb-status-board` — later modules use them.

# [V] items

- Step 2: platform-http request headers showing up as Kafka record headers; `${header.CamelKafkaKey}`
  printing the key after `.to(kafka)` (the 4.22.1 source reads the header and does not remove it).
- Step 3: `--property` vs `--formatter-property` and `--timeout-ms` on CP 8.4.
- Step 5: exact LAG while blocked; WireMock journal field `responseDefinition.status`.
- Step 7: `ESB_STATUS_BOARD_AUTO_STARTUP=false` overriding the placeholder in dev mode; the 409 text.
- Step 8: `KafkaManualCommit` header present on filtered records too.
