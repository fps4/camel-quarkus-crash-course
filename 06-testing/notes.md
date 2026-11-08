---
title: 06 — Testing Camel Quarkus routes
subtitle: The test pyramid for integration code — plain JUnit for mapping, @QuarkusTest with Dev Services for routes, AdviceWith and mocks for isolation, mock Schema Registry. The three failures we hit on the way to green (2026-11-08).
---

# What this module is for

- Choose the right test level for each part of a route, and keep the slow ones few.
- Run the whole app against real brokers in throw-away containers (Dev Services) and know what
  that needs (Docker, ports, config).
- Answer the reviewer question "how do you test an integration?" with something better than
  "we test in the acceptance environment".

---

# 1. Three levels

| Level | Tool | Speed | Tests | esb-lab |
|---|---|---|---|---|
| **Logic** | Plain JUnit 5 | ms | Mapping, validation, routing decisions in beans | `OrderStatusMapperTest` (3 tests, 0.2 s) |
| **Route with real brokers** | `@QuarkusTest` + Dev Services (Kafka, Artemis, Postgres containers) | seconds; containers start once per test run | The wiring: endpoints, serializers, error handling, idempotency, HTTP contract | `PartnerApiRouteTest`, `ScanBridgeRouteTest` (13 s for the bridge test incl. warm-up) |
| **Route in isolation** | `CamelQuarkusTestSupport` + `AdviceWith` + `mock:` endpoints | fast | One route, external endpoints replaced, failures injected | Lab exercise |
| **Contract** | Schema Registry compatibility check in CI; OpenAPI/AsyncAPI diff | ms | That a change does not break consumers | Confluent course module 03 |
| End-to-end | The lab on the lab host, or an acceptance environment | minutes | Everything together, incl. networks and config | Modules 03–05 labs |

The cheapest test that catches a class of bug wins. Mapping bugs → JUnit. Wrong serializer, wrong
header name, wrong error handler → `@QuarkusTest`. Network/ACL/DNS → only end-to-end.

---

# 2. `@QuarkusTest` and Dev Services

```java
@QuarkusTest
class ScanBridgeRouteTest {
    @Inject ProducerTemplate producer;     // send into any endpoint
    @Inject ConsumerTemplate consumer;     // poll from any endpoint, with timeout

    @Test
    void scanIsBridgedOnceAndPoisonIsParked() {
        producer.sendBody("jms:queue:esb.tracking.scan.in", SCAN);
        producer.sendBody("jms:queue:esb.tracking.scan.in", SCAN);            // duplicate
        assertNotNull(consumer.receiveBody(TOPIC, 20_000, String.class));    // one record
        assertNull(consumer.receiveBody(TOPIC, 5_000, String.class));        // and only one
        ...
    }
}
```

- The **whole app** starts once per test run (shared across `@QuarkusTest` classes with the same
  profile). Routes run for real.
- **Dev Services** start a container per missing server and inject its URL. Observed in the lab:
  `apache/kafka-native:4.2.0` (Kafka), `postgres:18`, `quay.io/arkmq-org/activemq-artemis-broker:artemis.2.53.0`
  (Artemis), plus Testcontainers' `ryuk` to clean up.
- **Schema Registry:** `%test.esb.schema-registry-url=mock://esb-test` — the Confluent serializer
  then uses an in-memory registry shared by everything in the JVM with the same scope name. No
  container needed. Producer and the test's consumer both use `mock://esb-test`.
- **Switch off what you do not test:** `%test.esb.status-board.auto-startup=false` keeps the
  status-board route stopped (its webhook does not exist in tests). Route `autoStartup` from config
  is also an operations lever.

---

# 3. The three failures on the way to green (2026-11-08)

| Symptom | Cause | Fix |
|---|---|---|
| `FailedToStartRouteException ... connectionFactory must be specified` | **Artemis Dev Services are not on by default** in the Quarkiverse extension. No URL in `%test` → no `ConnectionFactory` bean | `%test.quarkus.artemis.devservices.enabled=true` |
| An Apicurio Registry container started (unasked) | `quarkus-confluent-registry-avro` brings Dev Services for Apicurio | `quarkus.apicurio-registry.devservices.enabled=false` (we use `mock://`) |
| `QuarkusBindException: Port already bound: 8081` | Tests listen on **8081** by default — Schema Registry's port, on the lab host and on your workstation through the tunnel | `quarkus.http.test-port=0` (random); RestAssured picks it up |

Each one is the kind of thing that makes a team give up on integration tests. Fix once, in
`application.properties`, and write it down.

---

# 4. Route in isolation: AdviceWith and mocks

When the brokers are not the point, replace endpoints and inject failures:

```java
@QuarkusTest
class StatusBoardRouteIsolatedTest extends CamelQuarkusTestSupport {

    @Override
    public boolean isUseAdviceWith() { return true; }          // routes wait until advised

    @Test
    void partnerFailureIsRetriedThenSurfaced() throws Exception {
        AdviceWith.adviceWith(context, "status-board-commands", a -> {
            a.replaceFromWith("direct:in");                                 // no Kafka
            a.weaveByToUri("jms:*").replace().to("mock:status-board");      // no Artemis
            a.weaveByToUri("http*").replace().throwException(new java.io.IOException("503"));
        });
        context.getRouteController().startRoute("status-board-commands");   // autoStartup=false in %test

        MockEndpoint board = getMockEndpoint("mock:status-board");
        board.expectedMessageCount(1);                      // JMS step runs once; retries happen at the HTTP step
        template.sendBody("direct:in", sampleEvent("READY"));
        board.assertIsSatisfied();
    }
}
```

Points worth being able to explain: `weaveByToUri`, `replaceFromWith`, `mockEndpoints("kafka:*")`,
`NotifyBuilder` (wait until N exchanges are done), and **why redelivery retries the failed step,
not the route** — the mock sees one message, not five. [V: `CamelQuarkusTestSupport` API details —
check the camel-quarkus-junit docs for your version; the reference project does not include this
test]

---

# 5. What to test in an integration route — checklist

- Happy path with a realistic payload (copy a real one, scrub personal data).
- Each **error class**: poison → DLQ without retries; transient → retries then DLQ; HTTP 400 vs 503.
- **Duplicates**: same message twice → one effect.
- **Ordering** where it matters: same key → same partition; message groups on queues.
- **Contract**: schema compatible (registry check in CI), HTTP response body for errors.
- **Config**: the app starts with the prod profile and env vars only (a smoke test of the image).

---

# In practice

For an organisation that runs its ESB as Camel routes on Quarkus, with Artemis and Kafka, on
OpenShift with GitOps (ArgoCD):

- A good question for the platform team: **"Do route changes run integration tests against real
  Artemis and Kafka in CI, or only in a shared test environment?"** Shared environments are where
  flaky tests and blocked teams come from. Dev Services (or Testcontainers) make every PR
  self-contained.
- Contract tests for partner-facing APIs and event schemas belong in the same pipeline: a breaking
  schema change should fail the build, not the consumer at 06:00.

# Check yourself

1. *How do you test a route that reads from Kafka and writes to Artemis?* `@QuarkusTest` with Dev
   Services for both, `ProducerTemplate` in, `ConsumerTemplate` out with a timeout. Or AdviceWith
   to replace the endpoints when the brokers are not the point.
2. *How do you test Avro without a Schema Registry?* `mock://<scope>` URL on serializer and
   deserializer.
3. *How do you prove a duplicate is dropped?* Send twice, receive once, then assert a second receive
   with a short timeout returns null.
4. *Your test suite needs Docker. CI runs on OpenShift without Docker — now what?* Testcontainers
   Cloud, a remote Docker host, Podman with a socket, or run these tests in a pipeline stage on a VM
   runner. Or shift some tests to AdviceWith + mocks. [V for your CI]

# Sources

- Quarkus testing guide: https://quarkus.io/guides/getting-started-testing
- Dev Services: https://quarkus.io/guides/dev-services · Artemis Dev Services: https://docs.quarkiverse.io/quarkus-artemis/dev/index.html
- Camel Quarkus testing: https://camel.apache.org/camel-quarkus/latest/user-guide/testing.html
- AdviceWith: https://camel.apache.org/manual/advice-with.html
- Confluent mock schema registry (`mock://`): https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html
