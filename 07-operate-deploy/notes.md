---
title: 07 — Operate and deploy
subtitle: Health, metrics, tracing, logs, container images, Kubernetes/OpenShift manifests, scaling and leader election, GitOps with ArgoCD, Red Hat support. Lab observations from 2026-11-08.
---

# What this module is for

- Say what a route on the integration bus exposes for operations and what an on-call engineer looks at first.
- Explain how a Camel Quarkus app becomes a pod on OpenShift, how it scales, and which consumers
  must **not** run in every replica.
- Know the support boundary: what Red Hat supports, what is community.

---

# 1. Health

Extensions add checks automatically (`camel-quarkus-microprofile-health`, Artemis, Agroal):

| Endpoint | Meaning for Kubernetes | What esb-lab reports |
|---|---|---|
| `/q/health/live` | Liveness: "restart me if this fails" | Camel context state |
| `/q/health/ready` | Readiness: "do not send me traffic / count me as available" | context, routes, consumers, route controller, **Artemis JMS**, **Database** |
| `/q/health/started` | Startup probe | — |

**Observed with Artemis stopped:** `live` stayed **200**, `ready` went to **503** with
`Artemis JMS health check: DOWN` (the call took ~4 s — set the probe timeout above that, or the
probe fails by timeout rather than by status). The JMS listener logged `Could not refresh JMS
Connection` every few seconds and reconnected by itself when Artemis came back; `ready` returned
to 200, both consumers re-attached.

Rule: **dependencies in readiness, never in liveness.** If liveness checks Artemis, a broker
restart makes OpenShift restart every bus pod at once — a restart storm that makes the outage
longer.

---

# 2. Metrics

`camel-quarkus-micrometer` + `quarkus-micrometer-registry-prometheus` → `/q/metrics`. Seen in the
lab:

```
camel_exchanges_succeeded_total{routeId="item-scan-bridge"} 6
camel_exchanges_failed_total{routeId="status-board-commands"} 3
camel_exchanges_inflight, camel_exchanges_external_redeliveries_total,
camel_exchanges_failures_handled_total, camel_route_policy_seconds (per-route latency)
```

What to alert on for an integration bus, in order:

| Signal | Source | Why |
|---|---|---|
| **DLQ / DLT depth > 0, growing** | Artemis queue metrics, Kafka topic offsets | Messages that need a human; after an outage, good messages too (module 05) |
| **Consumer lag** (Kafka) / **queue depth** (Artemis) | Broker side — Confluent metrics, Artemis Prometheus plugin, kafka-lag-exporter | A blocked partition (module 05) shows here, not in app metrics |
| `camel_exchanges_failed_total` rate | App | Something is failing now |
| `camel_exchanges_external_redeliveries_total` | App | Broker is redelivering (transacted routes) |
| Route latency p99 (`camel_route_policy_seconds`) | App | Partner slowness before it becomes failure |
| Readiness DOWN per pod | OpenShift | A dependency is gone |

---

# 3. Tracing and logs

- **OpenTelemetry:** `camel-quarkus-opentelemetry2` (Camel 4.x) creates a span per route/endpoint
  and propagates the W3C `traceparent` in Kafka headers, JMS properties and HTTP headers, so one
  trace follows a partner POST → Kafka → status-board command → webhook. Export with OTLP to the
  platform's collector (Jaeger/Tempo/Dynatrace). [V: exact extension name for your Camel Quarkus
  version — the platform BOM lists both `camel-quarkus-opentelemetry` and `camel-quarkus-opentelemetry2`]
- **Logs:** JSON logs (`quarkus-logging-json`) so the platform can index them; put the exchange ID
  and business keys (orderId, itemTag) in MDC (`camel.main.use-mdc-logging=true`,
  `camel.main.use-breadcrumb=true`). Never log full payloads that carry personal data (GDPR) —
  item tags are fine, customer names and contact details are not.

---

# 4. From code to pod

| Way to build the image | How | When |
|---|---|---|
| **Dockerfile, multi-stage** (this lab) | `maven` build stage → JRE runtime stage | Any CI, no Quarkus plugins needed. Lab image: 619 MB on Temurin JRE (Ubuntu base) |
| **Jib** (`quarkus-container-image-jib`) | `mvn package -Dquarkus.container-image.build=true` — no Docker daemon | Common in CI; reproducible layers |
| **OpenShift S2I / BuildConfig** (`quarkus-openshift`) | Build inside the cluster | Red Hat-centric shops |
| **Native** (`-Dnative`) | Mandrel builder image → ~50–100 MB image, ms start [V] | Scale-to-zero, many small routes; costs build time and native caveats |

Red Hat base images for production: `registry.access.redhat.com/ubi9/openjdk-21-runtime` (JVM) or
`ubi9/ubi-minimal` / `quarkus-micro-image` (native). UBI images are what a Red Hat subscription
supports.

**Observed resource use (JVM mode, idle after the lab):** esb-lab **339 MiB**, start **2.6 s**
(Camel context 0.8 s). For comparison on the same host: Artemis 458 MiB, dc-a 1.2 GiB,
sr-a 378 MiB.

`quarkus-kubernetes` (or `quarkus-openshift`) writes `target/kubernetes/kubernetes.yml` at build
time: Deployment + Service, with liveness/readiness/startup probes already pointing at
`/q/health/*`, ports, labels and env from `quarkus.kubernetes.*` config. Observed in the lab build:
`Service`, `Deployment` (probes on `/q/health/live`, `/ready`, `/started`, port 8080, image
`esb-lab:dev`) and a `ServiceMonitor` for the Prometheus Operator — added automatically because the
Prometheus registry extension is present. Teams usually take this
as a starting point and keep the real manifests in a GitOps repo (Helm or Kustomize).

---

# 5. Scaling — which consumers can run in every replica

| Consumer | N replicas? | Note |
|---|---|---|
| **Kafka** (`kafka:`) | Yes, up to the **partition count** | Extra pods sit idle. Scale with KEDA on consumer lag |
| **JMS queue** (`jms:queue:`) | Yes, freely | Competing consumers. Per-key ordering needs message groups (`JMSXGroupID`) |
| **JMS topic** (`jms:topic:`) | **Careful** | Every replica gets every message → N× processing. Use a shared durable subscription (JMS 2) or an Artemis multicast address with one anycast queue |
| **HTTP** (`platform-http`) | Yes | Behind the Service / Route |
| **Polling: `timer`, `file`, `ftp`, `sql`, `scheduler`** | **No** — each replica polls | Wrap with `master:` (Camel Master + Kubernetes Lease) so only the leader runs it: `from("master:esb-sftp:sftp:...")`. Upstream example: `cluster-leader-election` |

Scaling signals: KEDA has scalers for **Kafka lag** and **Artemis queue length**; CPU-based HPA is
usually wrong for I/O-bound routes.

---

# 6. GitOps on OpenShift with ArgoCD

```
 app repo (routes, pom)  ──CI──►  image registry  (tag = git sha)
                                         │
 gitops repo (manifests per env) ◄── PR bumps image tag
        │  test / acc / prod overlays (Kustomize) or values files (Helm)
        ▼
     ArgoCD  ──sync──►  OpenShift namespaces: Deployment, Service, ConfigMap, Secret (sealed/external), KEDA ScaledObject
```

- Same image everywhere; environments differ only in ConfigMaps/Secrets (`KAFKA_BOOTSTRAP`,
  `ARTEMIS_URL`, credentials). That is why esb-lab reads `${ENV:default}`.
- Secrets from a vault (External Secrets Operator / HashiCorp Vault / Azure Key Vault), not in Git.
- Kafka topics, ACLs and schemas as code next to the app (CFK `KafkaTopic` resources, or Terraform
  for Confluent) — reviewed in the same PR as the route that uses them. Artemis addresses likewise
  through the operator's `ActiveMQArtemisAddress` [V].

---

# 7. Support and lifecycle

| Item | Status (2026-11-08) |
|---|---|
| Upstream Camel | LTS every ~6 months (4.14, 4.18, 4.22 are LTS lines); each LTS supported ~1 year by the community [V] |
| Camel Quarkus | 3.40.0 LTS (with Quarkus 3.40); also 3.33.x and 3.27.x LTS lines still patched |
| Red Hat build of Apache Camel | 4.18 line current; supports Quarkus and Spring Boot runtimes; extension-by-extension support levels [V] |
| Red Hat build of Camel K | **End of life 2025-06-30**; migrate to Camel Quarkus |
| Fuse 7 | End of life; Camel 2 → Camel 4 migration needed |
| Quarkus 4 | In beta (4.0.0.Beta3 on Maven Central, Nov 2026) — expect a major upgrade in 2027 [V] |

---

# In practice

For an organisation that runs its ESB as Camel routes on Quarkus, with Artemis and Kafka, on
OpenShift with GitOps (ArgoCD):

- Questions an architect asks the platform team: **"How many bus apps, one route per app or
  grouped by domain? Who operates them — the platform team, the domain teams, or an outsourcing
  partner? Are consumer lag and DLQ depth on a shared dashboard?"**
- Polling consumers (SFTP from external partners, DB polls of legacy systems) are where duplicate
  processing appears after someone scales a Deployment to 2. Ask how they handle leader election.
- Organisations that run critical or regulated operations must show evidence after an incident
  (NIS2, ISO 27001): tracing and DLQ metrics are part of the incident story.

# Check yourself

1. *Should liveness check Kafka or Artemis?* No — readiness. Observed: Artemis down → ready 503,
   live 200, auto-reconnect on recovery.
2. *How far can you scale a Kafka-consuming route?* To the partition count of the topic, per
   consumer group.
3. *A route polls an SFTP folder. Ops scale it to 3 pods. What happens?* Three pods pick the same
   files (or race on locks). Use `master:` with a Kubernetes lease.
4. *JVM or native for bus routes?* JVM by default (339 MiB, 2.6 s in the lab is fine for
   long-running consumers); native where scale-to-zero or density matters, after testing the
   extensions used.
5. *What does Red Hat support?* Red Hat build of Apache Camel on Quarkus/Spring Boot with its listed
   extensions; Camel K and Fuse are end of life.

# Sources

- Camel Quarkus health: https://camel.apache.org/camel-quarkus/latest/reference/extensions/microprofile-health.html
- Camel Quarkus micrometer: https://camel.apache.org/camel-quarkus/latest/reference/extensions/micrometer.html
- Quarkus Kubernetes extension: https://quarkus.io/guides/deploying-to-kubernetes · OpenShift: https://quarkus.io/guides/deploying-to-openshift
- Camel Master component: https://camel.apache.org/components/latest/master-component.html · example: https://github.com/apache/camel-quarkus-examples/tree/main/cluster-leader-election
- KEDA scalers (Kafka, Artemis): https://keda.sh/docs/latest/scalers/
- Camel releases and LTS: https://camel.apache.org/releases/
