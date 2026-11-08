---
title: "02 — Lab: a Camel Quarkus project with a partner REST endpoint"
subtitle: Environment your workstation (quarkus dev). About 75 minutes. Written 2026-11-08; the finished code is esb-lab/ (PartnerApiRoute, OrderStatusMapper).
---

# Before you start

- Module 00 done (JDK 21, Maven, Quarkus CLI on your workstation). The tunnel is not needed until
  module 03.
- Work outside the course repo: `mkdir -p ~/labs && cd ~/labs`.

# Step 1 — create the project

```bash
quarkus create app lab.esb:esb-routes:1.0.0-SNAPSHOT \
  --extension=camel-quarkus-rest,camel-quarkus-platform-http,camel-quarkus-jackson,camel-quarkus-direct,camel-quarkus-log,camel-quarkus-bean \
  --no-code
cd esb-routes
grep -E 'quarkus.platform.(version|artifact-id)|<artifactId>(quarkus-camel-bom|camel-quarkus)' pom.xml
```

Without the Quarkus CLI: https://code.quarkus.io → pick the same extensions → download.

Check `pom.xml`: two BOMs (`quarkus-bom` and `quarkus-camel-bom`) at the same platform version.
Compare with `esb-lab/pom.xml` in this course repo — that one pins **3.40.1**. If the CLI generated a
newer version, keep it; note the Camel version it brings at start-up.

# Step 2 — the partner contract and the mapper (plain Java)

Copy from the reference project (`~/labs/camel-quarkus-crash-course/esb-lab/src/main/java/lab/esb/`)
into `src/main/java/lab/esb/...`:

- `model/OrderStatusRequest.java` — the partner's JSON (a Java record)
- `support/InvalidMessageException.java`

For now, write a simpler mapper that returns a `Map` (module 03 replaces it with the Avro event):

```java
package lab.esb.support;

import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import lab.esb.model.OrderStatusRequest;

@ApplicationScoped
@Named("orderStatusMapper")
public class OrderStatusMapper {
    public Map<String, Object> toEvent(OrderStatusRequest in) {
        if (in == null || in.orderId() == null || !in.orderId().matches("[A-Z]{2,4}-\\d{4,10}")) {
            throw new InvalidMessageException("orderId must look like SO-1001234");
        }
        return Map.of(
            "eventId", UUID.randomUUID().toString(),
            "orderId", in.orderId(),
            "status", in.status().trim().toUpperCase(Locale.ROOT).replace(' ', '_'),
            "source", in.partner().toUpperCase(Locale.ROOT));
    }
}
```

# Step 3 — the route

`src/main/java/lab/esb/routes/PartnerApiRoute.java`:

```java
package lab.esb.routes;

import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import lab.esb.model.OrderStatusRequest;
import lab.esb.support.InvalidMessageException;

@ApplicationScoped
public class PartnerApiRoute extends RouteBuilder {
    @Override
    public void configure() {
        onException(InvalidMessageException.class)
            .handled(true)
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(400))
            .setBody(e -> Map.of("error", e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class).getMessage()));

        restConfiguration().bindingMode(RestBindingMode.json);

        rest("/api/v1")
            .post("/order-status")
                .consumes("application/json").produces("application/json")
                .type(OrderStatusRequest.class)
                .to("direct:publish-order-status");

        from("direct:publish-order-status")
            .routeId("partner-order-status")
            .bean("orderStatusMapper", "toEvent")
            .log("would publish: ${body}")
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
            .setBody(e -> Map.of("status", "accepted"));
    }
}
```

# Step 4 — dev mode

```bash
quarkus dev            # or ./mvnw quarkus:dev
```

In a second terminal:

```bash
curl -s -i -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' \
  -d '{"orderId":"SO-1001234","status":"location change","location":"ZONE-D7","partner":"acme"}'
curl -s -i -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' \
  -d '{"orderId":"so-12","status":"ready","partner":"acme"}'
```

**Expected trap:** the second call returns **500**, not 400. The log says
`No type converter available to convert from type: java.util.ImmutableCollections.Map1 to the
required type: java.io.InputStream`.

Why: the REST binding does **not** marshal error responses (option `skipBindingOnErrorCode`,
default `true`). Your `Map` reaches the HTTP layer unmarshalled. We hit exactly this in the
reference app on 2026-11-08. Fix it in the `onException` block:

```java
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .setBody(e -> Map.of("error", e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class).getMessage()))
            .marshal().json();
```

Save. Dev mode reloads on the next request. Call again: `400 {"error":"orderId must look like SO-1001234"}`.

# Step 5 — Dev UI, health, config

```bash
open http://localhost:8080/q/dev-ui          # Extensions -> Camel: routes, exchanges, timings
curl -s localhost:8080/q/health | jq          # needs camel-quarkus-microprofile-health:
quarkus ext add camel-quarkus-microprofile-health    # then call /q/health again (dev mode restarts)
```

(`open` is macOS; on Linux use `xdg-open` or a browser.)

Move the path into config to see profiles at work:

```properties
# src/main/resources/application.properties
esb.api.base=/api/v1
%dev.quarkus.log.category."lab.esb".level=DEBUG
```

```java
rest("{{esb.api.base}}")
```

Then override without a rebuild: stop dev mode, `./mvnw package -DskipTests`, and run
`ESB_API_BASE=/api/v2 java -jar target/quarkus-app/quarkus-run.jar`. The route now listens on
`/api/v2/order-status`. Env var name rule: dots and dashes → `_`, upper case.

# Step 6 — unit test the mapper (no Quarkus)

`src/test/java/lab/esb/support/OrderStatusMapperTest.java` with plain JUnit 5 — see the reference
project's version. The reference mapper returns the Avro event, so assert on the `Map` keys here
instead. Run `./mvnw test -Dtest=OrderStatusMapperTest`. Milliseconds, no containers. That is the
payoff of keeping mapping in a bean.

# Step 7 (optional) — native

```bash
./mvnw package -Dnative -Dquarkus.native.container-build=true   # builds inside a Mandrel container
./target/esb-routes-1.0.0-SNAPSHOT-runner                       # Linux binary: run it on a Linux host, not on macOS
```

On a macOS workstation the container build produces a Linux binary for the workstation's CPU
architecture. Run it on a Linux host with the same architecture, or skip this step and read the
start time and RSS numbers in module 07 instead. Expect several minutes and 6+ GB of RAM for the
build. [V]

# Done when

- `POST` valid → 202, invalid → 400 with a JSON reason.
- You can explain why it was 500 first.
- You changed a config value through an env var without rebuilding.

# [V] items

- `quarkus create app ... --no-code` flag name on your CLI version (`quarkus create app --help`).
- Native build memory and time on your machine, and the CPU architecture of the binary.
