---
title: "04 — Lab: Artemis, Camel JMS, the queue-to-Kafka bridge, IBM MQ → Artemis"
subtitle: "Environment: your workstation + tunnel to the lab host. About 2.5 hours (plus 45 minutes for the optional IBM MQ step). Written 2026-11-08."
---

# Before you start

- 00-lab-setup done: on the lab host `dc-a`, `sr-a`, `artemis`, `postgres`, `kafka-ui` up;
  `tunnel.sh` running on your workstation.
- Your working Quarkus project from labs 02–03 (here called `~/labs/esb`). If you skipped them,
  copy `esb-lab` to `~/labs/esb` and delete the classes under `routes/` — you will rebuild them.
- **Do not run the reference app container (`--profile app`) at the same time.** It consumes the
  same queues, and each side would see only part of the messages.

Two shortcuts for this lab (zsh on your workstation):

```bash
art() { ssh labhost docker exec artemis bin/artemis "$@" --user esb --password esb --url tcp://localhost:61616; }
S=~/labs/camel-quarkus-crash-course/esb-lab/samples
```

`ssh` joins the arguments into one remote command string. An argument with `<`, `>` or spaces must
carry its own single quotes, as in step 2.

# Step 1 — the Artemis console

Open http://localhost:8161 (through the tunnel), log in `esb` / `esb`, go to the Artemis view.

1. **Addresses** tab: note the routing type column (ANYCAST / MULTICAST). On a fresh broker you see
   only the built-ins (`DLQ`, `ExpiryQueue`, `activemq.notifications`). Queues of the app appear
   when a client first uses them (auto-create).
2. **Queues** tab: columns for message count, consumer count, messages added, acknowledged.
3. Create a queue by hand: action "Create queue" (or "Create address" first), name
   `esb.lab.console`, routing type ANYCAST, durable. [V: exact menu names in the HawtIO 4 console]
4. On the queue: "Send message", body `hello from the console`, add a header `source=console`.
5. "Browse messages": see the message, its headers, `durable`, `priority 4`, `expiration 0`.

**Check yourself:** where is the "queue manager"? (The broker itself.) Where is the "channel"?
(The acceptor on 61616 — all protocols. See the broker's `etc/broker.xml`:
`ssh labhost docker exec artemis grep -A2 '<acceptor ' etc/broker.xml`.)

# Step 2 — the Artemis CLI

The CLI is inside the container. These were run in the lab on 2026-11-08:

```bash
art queue stat                            # NAME, CONSUMER_COUNT, MESSAGE_COUNT, MESSAGES_ADDED, DELIVERING, MESSAGES_ACKED
art producer --destination queue://esb.lab.cli --message-count 3 --message "'hello cli'"
art queue stat                            # esb.lab.cli: MESSAGE_COUNT 3, no consumers
art queue purge --name esb.lab.cli
art queue stat                            # MESSAGE_COUNT 0
```

Also useful (not run in the lab) [V]:

```bash
art browser  --destination queue://esb.lab.console --message-count 5   # read without removing
art consumer --destination queue://esb.lab.console --message-count 1   # read and ack
art address show                                                          # addresses and routing types
```

Compare with Kafka: `kafka-console-consumer` never removes anything. Here `consumer` empties the
queue and `browser` does not.

# Step 3 — build the bridge from a queue into Kafka (queue → log)

## 3a. Dependencies and config

Add to `pom.xml` (same as `esb-lab`):

```xml
<properties>
  <quarkiverse-artemis.version>3.14.3</quarkiverse-artemis.version>   <!-- not in the platform BOM -->
</properties>
...
<dependency><groupId>org.apache.camel.quarkus</groupId><artifactId>camel-quarkus-jms</artifactId></dependency>
<dependency>
  <groupId>io.quarkiverse.artemis</groupId>
  <artifactId>quarkus-artemis-jms</artifactId>
  <version>${quarkiverse-artemis.version}</version>
</dependency>
<dependency><groupId>io.quarkiverse.messaginghub</groupId><artifactId>quarkus-pooled-jms</artifactId></dependency>
<!-- if not there yet from lab 02: -->
<dependency><groupId>org.apache.camel.quarkus</groupId><artifactId>camel-quarkus-jacksonxml</artifactId></dependency>
<dependency><groupId>org.apache.camel.quarkus</groupId><artifactId>camel-quarkus-jackson</artifactId></dependency>
```

Add to `application.properties`:

```properties
esb.topic.item-scan=tracking.item-scan.recorded.v1
esb.queue.scan-in=esb.tracking.scan.in
esb.queue.scan-dlq=esb.tracking.scan.dlq

%dev,prod.quarkus.artemis.url=${ARTEMIS_URL:tcp://localhost:61616}
%dev,prod.quarkus.artemis.username=esb
%dev,prod.quarkus.artemis.password=esb
quarkus.pooled-jms.max-connections=4
```

No Java code for the connection factory: `quarkus-artemis-jms` creates it, `quarkus-pooled-jms`
pools it, and Camel's `jms` component finds it by autowiring. In `%test` the URL is unset, so Dev
Services start an Artemis container for tests (module 06).

Create the topic (dc-a may auto-create it, but be explicit):

```bash
ssh labhost 'docker exec dc-a kafka-topics --bootstrap-server dc-a:29092 --create --if-not-exists \
  --topic tracking.item-scan.recorded.v1 --partitions 3 --replication-factor 1'
```

## 3b. The model

Copy `esb-lab/src/main/java/lab/esb/model/ItemScan.java` into your project. Note the **getters**:
the first version in the lab had public fields, and `${body.itemTag}` failed with
`MethodNotFoundException` — Simple calls `getItemTag()`, it cannot read a field.

## 3c. Version 1 — the happy path

`src/main/java/lab/esb/routes/ScanBridgeRoute.java`:

```java
package lab.esb.routes;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import lab.esb.model.ItemScan;

@ApplicationScoped
public class ScanBridgeRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("jms:queue:{{esb.queue.scan-in}}")
            .routeId("item-scan-bridge")
            .unmarshal().jacksonXml(ItemScan.class)
            .setHeader(KafkaConstants.KEY, simple("${body.itemTag}"))
            .marshal().json()
            .log("Item ${header.CamelKafkaKey} -> {{esb.topic.item-scan}}")
            .to("kafka:{{esb.topic.item-scan}}?requestRequiredAcks=all&enableIdempotence=true");
    }
}
```

```bash
cd ~/labs/esb && mvn quarkus:dev
# second terminal
art producer --destination queue://esb.tracking.scan.in --message-count 1 --message "'$(cat $S/item-scan.xml)'"
```

Expected: the log line `Item 0074123456 -> tracking.item-scan.recorded.v1`. Check the record:

- kafka-ui http://localhost:8070 → dc-a → Topics → `tracking.item-scan.recorded.v1` → Messages:
  key `0074123456`, JSON value with `itemTag`, `orderId`, `location`, `scannedAt`.
- or: `ssh labhost 'docker exec dc-a kafka-console-consumer --bootstrap-server dc-a:29092 --topic tracking.item-scan.recorded.v1 --from-beginning --property print.key=true --timeout-ms 5000'`

## 3d. Version 2 — poison messages

Send the poison sample to version 1 first:

```bash
art producer --destination queue://esb.tracking.scan.in --message-count 1 --message "'$(cat $S/item-scan-poison.xml)'"
```

Watch the log: an exception, and the message is **gone** — not on the queue, not anywhere. The
endpoint uses `AUTO_ACKNOWLEDGE`, so the message was acked before the route ran. (`art queue stat`:
MESSAGES_ACKED went up.)

Now add a dead letter channel at the top of `configure()`:

```java
errorHandler(deadLetterChannel("jms:queue:{{esb.queue.scan-dlq}}")
        .useOriginalMessage()
        .maximumRedeliveries(3).redeliveryDelay(500).useExponentialBackOff()
        .retryAttemptedLogLevel(org.apache.camel.LoggingLevel.WARN));
onException(com.fasterxml.jackson.core.JacksonException.class)
        .maximumRedeliveries(0);          // retrying bad XML never helps
```

Save (dev mode reloads), send the poison sample again. Expected, as observed in the lab: no
retries, the original XML lands on `esb.tracking.scan.dlq` (`art queue stat`, or browse it in the
console). The broker's `DLQ.esb.tracking.scan.in` from `broker.properties` stays empty or absent:
Camel handled the failure, the broker never saw a rollback.

**The reference route** (`esb-lab/.../ScanBridgeRoute.java`) adds `validate` (10-digit item tag)
and the idempotent consumer on Postgres. In the lab run, the same XML sent twice gave one Kafka
record and one row in `camel_messageprocessed` (`messageid`
`0074123456|SCANNER-3|2026-11-08T13:05:12Z`). Module 05 builds that part; copy it now if you want.

# Step 4 — consumers and ordering

## 4a. See the consumers

Change the endpoint to `jms:queue:{{esb.queue.scan-in}}?concurrentConsumers=2`, save, run
`art queue stat`: `esb.tracking.scan.in` shows CONSUMER_COUNT 2 (observed in the lab). Console →
Consumers tab shows two consumers on one session pool.

## 4b. Break the order

Add a lab-only route class `OrderingLabRoute.java`:

```java
package lab.esb.routes;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class OrderingLabRoute extends RouteBuilder {
    @Override
    public void configure() {
        // 3 items x 10 scans each, sent in order: item 50 scan 1, item 51 scan 1, item 52 scan 1, item 50 scan 2, ...
        from("timer:scans?repeatCount=1&delay=3000")
            .loop(30)
                .process(e -> {
                    int i = e.getProperty(Exchange.LOOP_INDEX, Integer.class);
                    e.getMessage().setHeader("itemTag", "00741234" + (50 + i % 3));
                    e.getMessage().setBody("scan " + (i / 3 + 1));
                })
                // .setHeader("JMSXGroupID", header("itemTag"))     // step 4c
                .to("jms:queue:esb.lab.order")
            .end();

        from("jms:queue:esb.lab.order?concurrentConsumers=2")
            .routeId("order-consumer")
            .delay(simple("${random(50,400)}"))                     // uneven processing time
            .log("${threadName} item=${header.itemTag} ${body}");
    }
}
```

Save, wait for the run, and read the log per item (e.g. filter on `item=0074123450`). You will see
`scan 3` before `scan 2` for some item: Artemis hands messages to both consumers in turn, and the
slower one finishes later. In the bridge, that is the order in which records reach Kafka — the
Kafka key keeps order only for what arrives in order.

## 4c. Fix it with message groups

Uncomment the `JMSXGroupID` line, save, and read the log again. Each item now sticks to one consumer
thread, and its scans come out `1, 2, 3, …`. Different items still run in parallel. This is the
queue version of "key → partition".

**Discuss:** the group ID is set by the **producer**. A legacy scanning system that does not set
it cannot be fixed on the consumer side — then the choice is one consumer for that queue (order)
or several (throughput). Which would you pick for item scans? What happens to a group when its
consumer pod dies?

Remove `concurrentConsumers=2` from the bridge again, or keep it and write down why.

# Step 5 — request-reply over a queue

Add `PingLabRoute.java`:

```java
@ApplicationScoped
public class PingLabRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("timer:ping?period=5000&delay=2000")
            .setBody(constant("hi"))
            .to(org.apache.camel.ExchangePattern.InOut, "jms:queue:esb.lab.ping?requestTimeout=5000")
            .log("reply: ${body}");

        from("jms:queue:esb.lab.ping")
            .routeId("ping-responder")
            .transform(simple("pong ${body}"));
    }
}
```

Expected: `reply: pong hi` every 5 s. In the console → Queues: next to `esb.lab.ping` there is a
queue with a UUID-like name — the **temporary reply queue** Camel created for this producer. Look at
a request message: `JMSReplyTo` points to it, `JMSCorrelationID` is set.

Variations:

1. Fixed reply queue: change the URI to
   `jms:queue:esb.lab.ping?requestTimeout=5000&replyTo=esb.lab.ping.reply&replyToType=Exclusive`.
   The temporary queue disappears; `esb.lab.ping.reply` appears. With two pods, each needs its
   own reply queue name — that is why `Exclusive` is per instance.
2. Timeout: stop the responder (`.autoStartup(false)` on `ping-responder`, save). After 5 s the
   requester fails with `ExchangeTimedOutException`. The request still sits on `esb.lab.ping`.
   Start the responder again: it answers old requests, and nobody is waiting for those replies.
   That late-reply problem is the main design issue with request-reply over messaging.

# Step 6 — topic vs queue

Add `TopicLabRoute.java`:

```java
@ApplicationScoped
public class TopicLabRoute extends RouteBuilder {
    @Override
    public void configure() {
        from("timer:events?period=3000&delay=2000")
            .setBody(simple("SO-1001234 READY ${date:now:HH:mm:ss}"))
            .to("jms:topic:esb.lab.events");

        from("jms:topic:esb.lab.events").routeId("subscriber-a").log("A got ${body}");
        from("jms:topic:esb.lab.events").routeId("subscriber-b").log("B got ${body}");
    }
}
```

Both A and B log every event. Console → Addresses: `esb.lab.events` is **MULTICAST** with two
queues with generated names — one per subscription.

Make B durable and shared (JMS 2):

```java
from("jms:topic:esb.lab.events?subscriptionDurable=true&subscriptionShared=true&subscriptionName=sub-b")
```

Stop dev mode (`q`). Send three events while the app is down:

```bash
art producer --destination topic://esb.lab.events --message-count 3 --message "'sent while down'"
art queue stat          # the queue for sub-b holds 3 messages; non-durable A has no queue now
```

Start dev mode again: B logs the three messages, A never sees them. [V: queue name for a shared
durable subscription without clientId]

**Compare with Kafka:**

| | Artemis durable shared subscription | Kafka consumer group |
|---|---|---|
| Every subscription/group gets every message | yes | yes |
| Several consumers share the load inside one | yes (shared) | yes (one per partition) |
| Survives the consumer being away | yes, messages wait in the subscription queue | yes, records wait in the log |
| A **new** subscriber sees the past | **no** — only from creation | **yes** — `earliest` reads the retained log |
| Replay after a bug fix | no | yes, reset offsets |

The last two rows are the reason order status facts go to Kafka.

# Step 7 (optional) — IBM MQ → Artemis with XA

Start IBM MQ on the lab host (the tunnel already forwards 1414 and 9443):

```bash
ssh labhost 'cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile ibmmq up -d ibm-mq'
ssh labhost docker logs ibm-mq 2>&1 | grep -i "started" | tail -3
```

Get the upstream example and point it at the lab:

```bash
git clone --depth 1 https://github.com/apache/camel-quarkus-examples.git ~/labs/cq-examples
cd ~/labs/cq-examples/message-bridge
grep -nE 'ibm.mq|artemis|pooled-jms|transaction-manager' src/main/resources/application.properties
```

IBM MQ defaults already match the lab (`localhost:1414`, `QM1`, `DEV.APP.SVRCONN`, `app` /
`passw0rd`, `DEV.QUEUE.1`). Artemis does not: the example sets
`quarkus.artemis.devservices.enabled=true`, so in dev mode it would start its own Artemis container.
Switch that off and set the lab broker for `%dev` (on macOS; on Linux use `sed -i` without `''`):

```bash
sed -i '' 's/^quarkus.artemis.devservices.enabled=true/quarkus.artemis.devservices.enabled=false/' src/main/resources/application.properties
cat >> src/main/resources/application.properties <<'EOF'
%dev.quarkus.artemis.url=tcp://localhost:61616
%dev.quarkus.artemis.username=esb
%dev.quarkus.artemis.password=esb
EOF
```

Stop your own `quarkus dev` first (both use port 8080). Then:

```bash
mvn clean compile quarkus:dev
# second terminal
curl -s -X POST -H "Content-Type: text/plain" localhost:8080/message -d 'Hello from IBM MQ'
```

Expected log lines: `Sending message to IBMMQ`, `Sending message from IBMMQ to ActiveMQ`,
`ActiveMQ received: Hello from IBM MQ`. In the Artemis console, queue `in` shows messages added and
acknowledged (the example's own route consumes it).

Try the failure paths:

1. `curl ... -d 'please rollback'` → the route throws **after** the Artemis send. Log:
   `Redelivering message after rollback to ActiveMQ`, then one `ActiveMQ received`. The Artemis
   send was rolled back with the IBM MQ get; IBM MQ redelivered. One message on Artemis, not two.
2. `curl ... -d 'crash'` → the JVM halts during the commit. Restart **without** `clean`
   (`mvn compile quarkus:dev`): Narayana reads `target/narayana` and recovery completes the commit.
   With `clean` you delete the transaction log — on OpenShift that is the PVC.
3. Look at IBM MQ while the app is stopped:
   `ssh labhost 'echo "DISPLAY QSTATUS(DEV.QUEUE.1) CURDEPTH IPPROCS" | docker exec -i ibm-mq runmqsc QM1'`.
   Console: https://localhost:9443/ibmmq/console (`admin` / `passw0rd`).

**Think about:** what would change if the target were Kafka instead of Artemis? (No XA: Kafka is not
an XA resource. You would commit the IBM MQ get after the Kafka ack and de-duplicate downstream.)

# Teardown

```bash
# stop quarkus dev in each project (q or Ctrl-C), then:
for q in esb.lab.order esb.lab.ping esb.lab.ping.reply esb.lab.cli esb.lab.console esb.tracking.scan.dlq; do art queue purge --name $q; done
art queue stat
ssh labhost 'cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile ibmmq stop ibm-mq'
```

Delete the lab route classes (`OrderingLabRoute`, `PingLabRoute`, `TopicLabRoute`) or set
`autoStartup(false)` before module 05. Keep the Kafka topic and the bridge — module 05 builds on
them. A full Artemis reset: `docker compose rm -sf artemis && docker volume rm camel-lab_artemis-data
&& docker compose up -d artemis` on the lab host.

# [V] items

- Console menu names in Artemis 2.57 (HawtIO 4): "Create queue", "Send message", "Browse messages".
- CLI commands not run in the lab: `browser`, `consumer`, `address show`.
- Queue naming for a JMS 2 shared durable subscription without `clientId`, and whether Camel needs
  `clientId` for `subscriptionShared=true` on Artemis.
- Step 4b reorders reliably only if both consumers get messages; the Artemis client prefetches
  (`consumerWindowSize`, 1 MB by default). If one consumer takes everything, add
  `?consumerWindowSize=0` to `quarkus.artemis.url` and repeat.
- Step 7 was not run end to end on the lab host: the example's `%dev` override, and whether the IBM MQ
  developer image accepts `app` / `passw0rd` on `DEV.APP.SVRCONN` from your workstation through the tunnel
  without extra CHLAUTH changes.
- `crash` in dev mode: whether Quarkus dev mode restarts cleanly after the app JVM halts.
