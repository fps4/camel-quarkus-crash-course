---
title: esb-lab — reference solution
subtitle: A small enterprise service bus in Camel Quarkus — partner REST → Kafka (Avro), Artemis queue → Kafka bridge, Kafka → Artemis command + partner webhook. Built and run on a Linux Docker host on 2026-11-08.
---

# What is in it

| Route | From | To | Patterns |
|---|---|---|---|
| `partner-order-status` (`routes/PartnerApiRoute.java`) | `POST /api/v1/order-status` (REST DSL, platform-http) | Kafka `orders.order-status.updated.v1`, Avro via sr-a, key = orderId | Messaging gateway, message translator, canonical model; 202 / 400 / 503 |
| `item-scan-bridge` (`routes/ScanBridgeRoute.java`) | Artemis `esb.tracking.scan.in` (XML) | Kafka `tracking.item-scan.recorded.v1` (JSON), key = itemTag | Queue → log (Kafka inside the ESB, next to Artemis), idempotent consumer on Postgres, dead letter channel |
| `status-board-commands` (`routes/StatusBoardRoute.java`) | Kafka `orders.order-status.updated.v1` (Avro) | Artemis `esb.status-board.cmd` + partner webhook (WireMock) | Log → command, filter, blocking retry (`breakOnFirstError`) |

Logic lives in `support/OrderStatusMapper.java` (plain Java, unit-tested). Config, topic and
queue names are in `src/main/resources/application.properties`. The Avro schema is
`src/main/avro/order-status-updated.avsc` (classes generated at build by `quarkus-avro`).

Versions: Quarkus platform 3.40.1 → Camel Quarkus 3.40.0 → Apache Camel 4.22.1 (reported at
start), Java 21, Confluent serializer 8.4.0, quarkus-artemis-jms 3.14.3.

# Run

- In a container on the lab host: `cd ../00-lab-setup && docker compose --profile app up -d --build esb-lab`
- On your workstation: `../00-lab-setup/tunnel.sh` in one terminal, `mvn quarkus:dev` here in another.
- Tests (need Docker for Dev Services): `mvn test`. Without a JDK on the machine:
  `docker run --rm --network host -v /var/run/docker.sock:/var/run/docker.sock -v esb-m2:/root/.m2 -v "$PWD":/src -w /src maven:3.9.16-eclipse-temurin-21 mvn -B test`

Sample payloads are in `samples/`.

# What was verified on the lab host (2026-11-08)

Lab host: Ubuntu 24.04, x86_64, 32 GB, Docker.

| Check | Result |
|---|---|
| Image build (multi-stage, Maven in Docker) | BUILD SUCCESS, image 619 MB |
| Start | Camel 4.22.1 context in 0.8 s, Quarkus JVM 2.6 s, 339 MiB RSS idle |
| `/q/health` | context, routes, consumers, Artemis, DB all UP |
| Valid partner POST | 202 + eventId; subject `orders.order-status.updated.v1-value` auto-registered in sr-a; status-board command on Artemis; webhook called (200) |
| Invalid partner POST | First **500** (bug: REST binding skips error bodies) → fixed with explicit `marshal().json()` → 400 with reason |
| Item scan XML | One JSON record on Kafka, key 0074123456 |
| Same scan again | Dropped (one row in `camel_messageprocessed`) |
| Poison XML | To `esb.tracking.scan.dlq` immediately, original body, no retries |
| Crash window (review finding) | Default `AUTO_ACKNOWLEDGE` acks before the route runs → a crash could lose a scan. Bridge changed to `transacted=true` and re-tested (see below) |
| First run of the bridge | **Bug:** Simple `${body.itemTag}` cannot read public fields → `MethodNotFoundException`; fixed with getters |
| Log lines with `${header.kafka.KEY}` | Empty — two reasons: since Camel 4.21 the header is `CamelKafkaKey` (was `kafka.KEY`; Red Hat's 4.18 build still uses the old name), and Simple reads a dotted name as OGNL unless written `${header[a.b]}`. Record keys were fine because the code uses `KafkaConstants.KEY` |
| Postgres stopped | 4 attempts in ~6.5 s, then the good scan went to the DLQ; replayed with `05-reliability/replay-dlq.sh` after Postgres came back |
| Partner webhook 503 | 5 attempts per cycle, seek back, endless loop; next event on the partition blocked; 4 duplicate status-board commands; drained to lag 0 when the partner recovered |
| Artemis stopped | live 200, ready 503 (Artemis DOWN), JMS listener reconnected on recovery |
| `broker.properties` applied | Jolokia: `maxDeliveryAttempts 3`, `redeliveryDelay 1000`, `deadLetterAddress DLQ`, prefix `DLQ.` for `esb.*` |
| Broker redelivery variant (`noErrorHandler`, Postgres down) | 3 deliveries, then `DLQ.esb.tracking.scan.in` + a copy in default `DLQ` |
| `transacted=true` side effect | Spring fell back to `CACHE_NONE` (0 consumers between messages) → `cacheLevelName=CACHE_CONSUMER`, 2 consumers stay attached |
| Unit + Quarkus tests | 6/6 green on the lab host (`mvn test` in a Maven container, Dev Services: kafka-native 4.2.0, postgres 18, arkmq Artemis 2.53.0) |
| `mvn package` manifests | `target/kubernetes/kubernetes.yml`: Service, Deployment with `/q/health/live`, `ready`, `started` probes, and a ServiceMonitor (Prometheus Operator) because of the Prometheus registry |

# Tests

| Test | Type | What it proves |
|---|---|---|
| `support/OrderStatusMapperTest` | Plain JUnit 5 | Mapping and validation rules, in milliseconds |
| `routes/PartnerApiRouteTest` | `@QuarkusTest`, Dev Services Kafka, mock Schema Registry (`mock://esb-test`) | 202 + an Avro event on the topic; 400 with a reason |
| `routes/ScanBridgeRouteTest` | `@QuarkusTest`, Dev Services Artemis + Kafka + Postgres | Bridged once, duplicate dropped, poison parked unchanged |

The status-board route is switched off in tests (`%test.esb.status-board.auto-startup=false`)
because its webhook does not exist there.

# Known limits (deliberate, for the labs)

- `status-board-commands` couples two side effects and blocks its partition on a partner failure.
  Module 05 experiment 6 is about fixing that.
- `auto.register.schemas` is left at the serializer default (true). A governed setup registers
  schemas from CI.
- Credentials are in `application.properties` for the lab only (`esb/esb`).
