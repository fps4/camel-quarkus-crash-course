---
title: "06 — Lab: green build — unit tests, @QuarkusTest with Dev Services, an isolated route test"
subtitle: Environment your workstation (Docker Desktop or another local Docker for Dev Services) or the lab host (Maven in a container). About 60 minutes. The reference tests passed 6/6 on the lab host on 2026-11-08.
---

# Before you start

- Docker must be reachable from where tests run: Docker Desktop (or another local Docker) on your
  workstation, or the lab host.
- Dev Services pull images on first run: kafka-native, postgres, the arkmq Artemis image, ryuk.
  Expect 1–2 minutes the first time.
- The tunnel can stay up: the test HTTP port is random (`quarkus.http.test-port=0`), so it does not
  clash with the tunnelled 8081.

# Step 1 — run the reference tests

On your workstation, in your copy of `esb-lab` (or the reference):

```bash
mvn test
```

Without a JDK, on the lab host (this is how they were run on 2026-11-08):

```bash
cd ~/labs/camel-quarkus-crash-course/esb-lab
docker run --rm --network host -v /var/run/docker.sock:/var/run/docker.sock \
  -v esb-m2:/root/.m2 -v "$PWD":/src -w /src maven:3.9.16-eclipse-temurin-21 mvn -B test
```

Expected:

```
Tests run: 1  -- in lab.esb.routes.ScanBridgeRouteTest         (~13 s)
Tests run: 2  -- in lab.esb.routes.PartnerApiRouteTest         (~2 s, app already up)
Tests run: 3  -- in lab.esb.support.OrderStatusMapperTest      (~0.2 s)
Tests run: 6, Failures: 0, Errors: 0   BUILD SUCCESS
```

In the log, find the Dev Services lines (which images, which ports) and
`Apache Camel 4.22.1 (esb-lab) started`.

# Step 2 — reproduce the three failures (10 minutes, worth it)

In `src/main/resources/application.properties`, one at a time:

1. Comment out `%test.quarkus.artemis.devservices.enabled=true` → `connectionFactory must be
   specified`. Restore.
2. Comment out `quarkus.apicurio-registry.devservices.enabled=false` → an extra Apicurio container
   in the log. Harmless here, but slow. Restore.
3. Comment out `quarkus.http.test-port=0` with the tunnel running (or on the lab host) → `Port already bound:
   8081`. Restore.

Explain each in one sentence. These are typical first-day problems for a team adopting Dev Services.

# Step 3 — write a test that fails for the right reason

Add to `ScanBridgeRouteTest` a case for **ordering**: three scans of the same item at different
locations, check that they arrive on Kafka in send order. With `concurrentConsumers=2` on the JMS
endpoint, does it pass every time? Run it 5 times (`mvn test -Dtest=ScanBridgeRouteTest
-Dsurefire.rerunFailingTestsCount=0` in a loop). If it is flaky, that is the finding: two consumers
on one queue do not keep order. Fix: message groups (`JMSXGroupID` = itemTag) on the producer side,
or one consumer. (Module 04 has the background.)

# Step 4 — isolated route test with AdviceWith

Create `src/test/java/lab/esb/routes/StatusBoardRouteIsolatedTest.java` from notes §4. Helper for
the event:

```java
static lab.esb.events.OrderStatusUpdated sampleEvent(String status) {
    return lab.esb.events.OrderStatusUpdated.newBuilder()
        .setEventId("e-1").setOrderId("SO-1001234")
        .setStatus(lab.esb.events.OrderStatus.valueOf(status)).setLocation("ZONE-D7")
        .setSource("ACME").setOccurredAt(java.time.Instant.now()).build();
}
```

Tests to write:

1. `READY` → one message on `mock:status-board`, HTTP step called.
2. `DELAYED` → zero messages (filtered).
3. HTTP step throws → the mock still sees **one** message (redelivery retries the failed step, not
   the route), and the exception surfaces after 5 attempts. Tip: shorten the redelivery delay in the
   test profile, or the test waits ~15 s.

Note: `CamelQuarkusTestSupport` subclasses run in their own Quarkus application per test class —
slower than plain `@QuarkusTest`. Keep the number small. [V: check the camel-quarkus-junit docs for
your version; this test is not in the reference project]

# Step 5 — smoke test the image with the prod profile

On the lab host, after `docker compose --profile app up -d --build esb-lab`:

```bash
curl -sf localhost:8080/q/health/ready && echo READY
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/api/v1/order-status \
  -H 'Content-Type: application/json' -d @esb-lab/samples/order-status-location-change.json   # 202
```

That is the minimum a pipeline should run against a freshly built image before it is promoted.

# Teardown

Dev Services containers stop with the test JVM (ryuk removes leftovers). On the lab host:
`docker volume rm esb-m2` frees the Maven cache (~1 GB) when you are done with the course.

# [V] items

- `CamelQuarkusTestSupport` + `isUseAdviceWith()` exact API in camel-quarkus-junit 3.40.
- Step 3 flakiness depends on timing; it may pass by luck on a quiet machine.
