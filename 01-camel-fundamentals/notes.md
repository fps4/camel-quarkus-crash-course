---
title: 01 — Camel fundamentals
subtitle: Routes, exchange, components, EIPs, DSLs — mapped to IBM ACE. Camel 4.22 (LTS). Written 2026-11-08.
---

# What this module is for

- Read any Camel route (Java or YAML) and say what it does, where it can fail, and what each
  step costs.
- Translate between IBM ACE vocabulary and Camel vocabulary without hesitating.
- Know what changed in Camel 3 → 4 and in Red Hat's product line (Fuse → Red Hat build of Apache
  Camel; Camel K end of life), because most teams that run Camel today have lived through it.

---

# 1. The model in one picture

```
CamelContext  (one per app: registry, type converters, components, routes)
 └── Route  "item-scan-bridge"
       from(Endpoint)  ── consumer ──►  Exchange ──► Processor ──► Processor ──► to(Endpoint) ── producer
       jms:queue:esb.tracking.scan.in               unmarshal     idempotent     kafka:tracking.item-scan...
```

| Concept | What it is | ACE equivalent |
|---|---|---|
| **CamelContext** | The runtime container. Holds routes, components, registry (beans), type converters | Integration server |
| **Route** | A flow from one consumer endpoint through processors | Message flow |
| **Endpoint / URI** | `scheme:path?options`, e.g. `kafka:topic?groupId=g1`. The scheme picks the **component** | Input/output node with its properties |
| **Component** | A factory for endpoints: `kafka`, `jms`, `platform-http`, `file`, `sql`, ~300 of them | Node type / connector |
| **Consumer** | The `from` side. Event-driven (JMS listener, HTTP server) or polling (file, SQL, timer) | Input node |
| **Producer** | The `to` side. Sends the exchange to a system | Output / request node |
| **Exchange** | The unit of work: message + properties + exception + pattern | Message assembly (`InputRoot`, `Environment`, `LocalEnvironment`, `ExceptionList`) |
| **Message** | Body + headers (+ attachments). Headers travel to the transport (JMS properties, Kafka headers, HTTP headers) | `InputRoot.Properties` / `MQMD` / `HTTPHeader` + body |
| **Exchange properties** | Scratch data that never leaves the route | `Environment` / `LocalEnvironment` |
| **Variables** (Camel 4.4+) | Named values, route- or global-scoped, alternative to properties | `Environment.Variables` |
| **Processor** | Any step that changes the exchange. A bean method, a lambda, an EIP | Compute / Mapping node |
| **Message Exchange Pattern** | `InOnly` (fire and forget) or `InOut` (request-reply) | One-way vs request-response flow |

**Body type conversion.** Camel converts the body on demand (`bodyAs(String.class)`, bean
parameter binding, `convertBodyTo`). Hundreds of built-in converters: `byte[]` ↔ `String` ↔
`InputStream` ↔ DOM, etc. Convenient, but a large stream read twice is empty the second time —
enable stream caching (on by default in Camel 4) or convert once.

---

# 2. The EIPs that matter most

Camel implements the Hohpe/Woolf patterns as DSL words. The ones that matter for an enterprise service bus:

| EIP | DSL | Example use | Watch out |
|---|---|---|---|
| Message Translator | `transform()`, `bean()`, `marshal()/unmarshal()` | Partner JSON → canonical Avro event | Keep mapping in plain Java (testable without Camel) |
| Content-Based Router | `choice().when(...).otherwise()` | Route legacy messages by type (scan vs status report) | Every branch needs an `otherwise` or messages vanish silently |
| Message Filter | `filter(predicate)` | Only `LOCATION_CHANGE`/`READY` reach the status board | Filtered messages are "done" — on Kafka the offset still advances |
| Splitter | `split(body().tokenize("\n"))`, `.streaming()` | One batch file → one message per line | Without `streaming()` the whole file is in memory |
| Aggregator | `aggregate(correlationExpr, strategy).completionSize/Timeout` | All items of an order → one summary message | State lives in memory unless you give it a repository (JDBC, Infinispan) |
| Recipient List / Multicast | `recipientList(header("to"))`, `multicast().parallelProcessing()` | Fan out one event to several partners | Partial failure: one recipient fails, others already got it |
| Wire Tap | `wireTap("seda:audit")` | Audit copy without slowing the main flow | Fire-and-forget: lost on crash |
| Idempotent Consumer | `idempotentConsumer(expr).idempotentRepository(repo)` | Drop duplicate scans (module 05) | In-memory repo forgets on restart and is per pod |
| Dead Letter Channel | `errorHandler(deadLetterChannel("jms:queue:dlq"))` | Park poison messages (module 05) | Without `useOriginalMessage()` you park the half-transformed body |
| Claim Check | `claimCheck(...)` | Keep big payloads out of the message | Store must be shared across pods |
| Saga | `saga()` + LRA coordinator | Compensations across services | Needs a coordinator; rarely worth it |
| Throttler / Circuit Breaker | `throttle(n)`, `circuitBreaker()` (Resilience4j / MicroProfile Fault Tolerance) | Protect a fragile partner API | Throttle in one pod ≠ throttle across pods |
| Request-Reply | `to(ExchangePattern.InOut, "jms:queue:x")` | Synchronous call over a queue (legacy) | Temporary reply queues; timeouts |

---

# 3. Languages and data formats

- **Simple** — Camel's own expression language: `${body.orderId}`, `${header.CamelKafkaKey}`, `${header[some.dotted.name]}` (brackets for names with dots),
  `${exchangeProperty.eventId}`, `${date:now:yyyy-MM-dd}`, `${body.status} in 'A,B'`. Fine for
  routing decisions and small templates. Not for mapping logic.
- **JSONPath / XPath / jq / JSONata** — query JSON or XML without unmarshalling.
- **Data formats** — `marshal()` = object → wire format, `unmarshal()` = wire → object:
  `json()` (Jackson), `jacksonXml()`, `avro()`, `protobuf()`, `csv()`, `bindy` (fixed-length,
  CSV — useful for legacy fixed-length text formats), `jaxb()`.
- **Bean binding** — `bean("mapper", "toEvent")` calls a method; Camel converts the body to the
  parameter type. The cleanest way to keep logic in plain, unit-testable Java.

---

# 4. DSLs: same route, three syntaxes

Java DSL (what `esb-lab` uses):

```java
from("jms:queue:esb.tracking.scan.in")
    .routeId("item-scan-bridge")
    .unmarshal().jacksonXml(ItemScan.class)
    .setHeader(KafkaConstants.KEY, simple("${body.itemTag}"))   // "CamelKafkaKey" since Camel 4.21
    .marshal().json()
    .to("kafka:tracking.item-scan.recorded.v1");
```

YAML DSL (what Kaoto draws and Camel JBang runs):

```yaml
- route:
    id: item-scan-bridge
    from:
      uri: jms:queue:esb.tracking.scan.in
      steps:
        - unmarshal:
            jacksonXml:
              unmarshalType: lab.esb.model.ItemScan
        - setHeader:
            name: CamelKafkaKey       # was kafka.KEY before Camel 4.21
            simple: ${body.itemTag}
        - marshal:
            json: {}
        - to: kafka:tracking.item-scan.recorded.v1
```

XML DSL (Spring/Blueprint era, still common in Fuse code you may meet). Same model — routes can
be moved between DSLs, and `camel transform` converts between them.

**Which one?** Java when developers own the routes and want refactoring and tests. YAML when
routes are configuration-like, drawn in Kaoto, or managed by a platform team. Mixed is normal:
YAML routes calling Java beans.

---

# 5. Runtimes: where a route runs

| Runtime | When | Notes |
|---|---|---|
| **Camel Quarkus** | Red Hat's main target; containers on OpenShift | Fast start, low memory, native option; build-time wiring (module 02) |
| Camel Spring Boot | Teams already on Spring | Also supported by Red Hat |
| Camel Main | Plain Java, no framework | What Camel JBang runs underneath |
| Camel JBang | Prototyping, one-file routes, `camel export` to a real project | Not a production runtime |
| ~~Camel K~~ | Was: operator builds and runs routes on Kubernetes | **Red Hat build of Camel K reached end of life on 2025-06-30**; Red Hat points to Camel Quarkus. Upstream Camel K still exists [V] |
| ~~Fuse 7 (Karaf/OSGi, EAP, Spring Boot 2)~~ | Was: Red Hat's Camel 2 product | End of life; migrations to Camel 4 on Quarkus are a typical project |

---

# 6. Changed since the Camel you may have seen

| Area | Camel 2.x (Fuse 7, *Camel in Action* 2nd ed.) | Camel 4.x (now) |
|---|---|---|
| Java | 8 | 17+ (Camel 4.x); Camel Quarkus 3.40 builds on Java 17/21 |
| APIs | `javax.*` | `jakarta.*` (JMS 3, Servlet 6) |
| Runtime | Karaf/OSGi, Spring XML | Quarkus, Spring Boot, Main |
| Exchange | `getIn()` / `getOut()` | `getMessage()`; `getOut()` deprecated |
| Tooling | Fuse Tooling (Eclipse), Hawtio | Camel JBang, Kaoto, VS Code debugger, Hawtio 4, `camel-cli` |
| Config | `CamelContext` XML | `application.properties` (`camel.component.kafka.brokers=...`) |
| Kafka component | old consumer model | `breakOnFirstError` seeks back; batching consumer; Kafka 4 clients |
| New | — | Variables, Kamelets, YAML DSL, `platform-http` on Vert.x, LangChain4j components |
| Red Hat product | Fuse 7 | Red Hat build of Apache Camel (Quarkus + Spring Boot); release cadence follows upstream LTS |

---

# In practice

For an organisation that runs its ESB as Camel routes on Quarkus, with Artemis queues and Kafka, on
OpenShift with GitOps (ArgoCD):

- The whole of this course applies, plus Artemis (module 04). Questions an architect asks the team
  that owns the platform: **Java DSL or YAML? One app per route group or per domain? Who owns a
  route — the platform team or the domain team?** The ownership answer tells you whether the bus is
  a central ESB (IBM-style, a bottleneck) or a platform with self-service.
- If you come from IBM ACE/IIB: the patterns are the same; the runtime moved from integration
  servers to containers. The mapping table in the course README is the short version.
- Camel K's end of life matters if anybody still runs it — a migration to Camel Quarkus is real
  work, and an architect should know it.

# Check yourself

1. *What is an Exchange, and how is a header different from an exchange property?* Exchange = unit
   of work. Headers go to the transport (JMS property, Kafka header, HTTP header); properties stay
   inside the route.
2. *Why would you put mapping in a bean rather than in Simple expressions in the route?*
   Testable with JUnit alone, type-safe, refactorable, reviewable. Route = wiring, bean = logic.
3. *A filter drops a Kafka record. Is the offset committed?* Yes — for Camel the exchange completed
   successfully. If dropping is wrong, that is data loss you designed in.
4. *Java DSL or YAML?* Depends who owns the routes; see §4. Mixed is fine.
5. *What happened to Camel K and Fuse?* Both end of life in Red Hat's line; Camel 4 on Quarkus (or
   Spring Boot) is the target.

# Sources

- Camel EIP index: https://camel.apache.org/components/latest/eips/enterprise-integration-patterns.html
- Camel JBang: https://camel.apache.org/manual/camel-jbang.html
- YAML DSL: https://camel.apache.org/components/latest/others/yaml-dsl.html
- Camel releases (LTS list): https://camel.apache.org/releases/
- Camel 3→4 migration guide: https://camel.apache.org/manual/camel-4-migration-guide.html
- Red Hat build of Camel K end of life notice: https://docs.redhat.com/documentation/red_hat_build_of_apache_camel_k/1.10.1/html/getting_started_with_camel_k/index
