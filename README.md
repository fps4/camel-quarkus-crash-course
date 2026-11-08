---
title: Camel / Quarkus crash course
subtitle: Hands-on course on Apache Camel 4 on Quarkus, with Apache Artemis for queues and Kafka (Confluent Platform) for events. Built and tested 2026-11-08.
---

# What this is

A short, hands-on course on **Apache Camel 4 routes on Quarkus**, with **Apache Artemis** (Red Hat
AMQ Broker) for queues next to **Kafka** for events. You build a small enterprise service bus (ESB)
step by step and break it on purpose: poison messages, duplicates, slow partners, a blocked
partition.

**For whom:** architects and engineers who design or review integration platforms and want to
learn how Camel on Quarkus really behaves. You do not need to be a Camel developer. Java helps; you
can read the code without writing much of it.

**The goal** is not to become a Camel developer. It is to explain, two follow-up questions deep, how
routes on Quarkus work, how they meet Kafka and Artemis, where they fail, and what that means for
the design. **[V]** in the text = check before you state it as fact (versions, support dates,
vendor claims).

This course is the companion of the
[Confluent crash course](https://github.com/fps4/confluent-crash-course). The lab uses that course's
Kafka cluster (`dc-a`) and Schema Registry (`sr-a`), so you clone both repos (see below).

# If you know IBM ACE/IIB and MQ

Most concepts map one to one:

| IBM | Red Hat / Apache |
|---|---|
| IIB / App Connect Enterprise message flows | Camel routes |
| Nodes (Compute, Mapping, Filter, Route, HTTPInput, MQInput) | Components + EIPs (`bean`, `transform`, `filter`, `choice`, `platform-http`, `jms`) |
| ESQL / Java compute node | Java DSL + Simple language / a bean |
| Integration server / BAR file | Quarkus app / container image |
| IBM MQ | AMQ Artemis (Red Hat AMQ Broker = Apache Artemis) |
| API Connect | 3scale (Red Hat) — out of scope here |
| Integration node on VMs | Pods on OpenShift, deployed by ArgoCD |

# Is there a good course online? (checked 2026-11-08)

Short answer: **no single current course covers Camel 4 + Quarkus + Kafka + Artemis.** The best
material is free and scattered. Use these alongside the labs, not instead of them:

| Resource | What it is good for | Verdict |
|---|---|---|
| [Camel Quarkus examples](https://github.com/apache/camel-quarkus-examples) (GitHub, on Quarkus 3.40.1) | Working projects: `kafka`, `message-bridge` (IBM MQ → Artemis with XA), `jms-jpa`, `jpa-idempotent-repository`, `saga`, `observability`, `openapi-contract-first`, `health` | **Best hands-on source.** Read one per module |
| [Camel docs: EIPs](https://camel.apache.org/components/latest/eips/enterprise-integration-patterns.html) + [Camel Quarkus user guide](https://camel.apache.org/camel-quarkus/latest/user-guide/index.html) | Reference for every pattern and extension | Reference, not a course |
| [Camel JBang](https://camel.apache.org/manual/camel-jbang.html) | Run a route from one file in seconds; `camel export` to a Quarkus project | Use in module 01 |
| [Kaoto](https://kaoto.io) (VS Code) | Visual designer for Camel routes (YAML) — the low-code face Red Hat shows customers | Worth 30 minutes |
| *Camel in Action*, 2nd ed. (Ibsen, Anstey; Manning 2018) | Still the best explanation of concepts: exchange, error handling, transactions, testing | Concepts hold; the API is Camel 2.x, so the code is dated. No 3rd edition found |
| *Enterprise Integration Patterns* (Hohpe, Woolf) — [enterpriseintegrationpatterns.com](https://www.enterpriseintegrationpatterns.com) | The pattern language Camel implements | If you know it, the site is enough |
| Red Hat **AD221** "Cloud-native Integration with Red Hat Fuse and Apache Camel" (3–5 days, ~€2.5–3.5k) | Instructor-led | **Skip.** Still built on Fuse 7.10 (end of life). A Red Hat Jira ticket asks for a Camel 4 version; no answer found |
| [Quarkus guides](https://quarkus.io/guides/) — Kafka, Schema Registry Avro, Dev Services, Kubernetes | Quarkus side of the stack | Reference |
| [Camel videos page](https://camel.apache.org/categories/Videos) (Claus Ibsen and others) | Camel JBang, debugging, Quarkus Insights sessions | Good for evenings |

So: **use the labs below**, built on the same Kafka cluster as the Confluent course, and dip into
the upstream examples where a module points to them.

# The lab

One small enterprise service bus, `esb-lab/`, grown across the modules. The finished project is in
the repo as the answer key; each lab tells you to build it yourself first.

```
 external partner ──HTTP POST──►  [partner-order-status]  ──Avro──►  orders.order-status.updated.v1
                                  REST DSL, validate, map                        │ (Kafka dc-a + sr-a)
                                                                                 ▼
                                                                 [status-board-commands]
                                                                  filter LOCATION_CHANGE/READY
                                                                    ├──► jms: esb.status-board.cmd   (Artemis)
                                                                    └──► POST partner webhook        (WireMock)

 legacy system ──XML──► jms: esb.tracking.scan.in ──► [item-scan-bridge] ──JSON──► tracking.item-scan.recorded.v1
                        (Artemis)                      unmarshal, validate,
                                                       idempotent (Postgres)
                                                       poison ─► esb.tracking.scan.dlq
```

| Module | Topic | Lab result |
|---|---|---|
| [00 Lab setup](00-lab-setup/README.md) | Services on the lab host, SSH tunnel, tool chain | Everything answers on localhost |
| [01 Camel fundamentals](01-camel-fundamentals/notes.md) · [lab](01-camel-fundamentals/lab.md) | Routes, exchange, components, EIPs, DSLs; Camel JBang; Kaoto | A YAML route run with JBang, exported to Quarkus |
| [02 Camel on Quarkus](02-camel-on-quarkus/notes.md) · [lab](02-camel-on-quarkus/lab.md) | Build-time boot, extensions, config profiles, dev mode, Dev Services, REST DSL | `POST /api/v1/order-status` answering 202 |
| [03 Kafka](03-kafka/notes.md) · [lab](03-kafka/lab.md) | camel-kafka producer/consumer, keys, acks, Avro + Confluent SR, offsets, `breakOnFirstError` | Avro events on dc-a, a consumer route |
| [04 Artemis and JMS](04-artemis-jms/notes.md) · [lab](04-artemis-jms/lab.md) | Queue vs log, addresses/anycast/multicast, Camel JMS, pooling, request-reply; IBM MQ bridge | The queue-to-log bridge: Artemis queue → Kafka topic |
| [05 Reliability](05-reliability/notes.md) · [lab](05-reliability/lab.md) | Error handler, `onException`, DLC, redelivery (Camel vs broker), idempotent consumer, transactions, dual writes | Poison messages parked, duplicates dropped |
| [06 Testing](06-testing/notes.md) · [lab](06-testing/lab.md) | `@QuarkusTest`, Dev Services, AdviceWith, mock endpoints, mock Schema Registry | Green build with route tests |
| [07 Operate and deploy](07-operate-deploy/notes.md) · [lab](07-operate-deploy/lab.md) | Health, metrics, tracing, container image, Kubernetes manifests, OpenShift + ArgoCD, Red Hat support, Camel K end of life | App in a container on the lab host, manifests generated |

Suggested order: 00 → 01 → 02 → 03 → 04 → 05 → 06 → 07. About two evenings for 00–03 and two for
04–07. Modules 04 and 05 are the core: that is where most design questions about an integration
bus live.

# Get the code

The lab needs both repos, cloned side by side, because this course starts `dc-a` + `sr-a` from the
Confluent course and the relative paths (`../../confluent-crash-course/00-lab-setup`) expect that
layout:

```bash
mkdir -p ~/labs && cd ~/labs
git clone https://github.com/fps4/confluent-crash-course.git
git clone https://github.com/fps4/camel-quarkus-crash-course.git
```

Clone them on the Docker host that runs the lab (any Linux Docker host) and on your workstation if
you develop there. [00 Lab setup](00-lab-setup/README.md) has the details.

# Status (2026-11-08)

- **Tested on a Linux Docker host** (Ubuntu 24.04, x86_64, 32 GB): both compose files (first real
  run of the Confluent `dc-a` + `sr-a` too), the reference app image, every flow in modules 02–05
  and 07, broker redelivery, DLQ replay, and the test suite (6/6 green). Results and the bugs found
  on the way are in [esb-lab/README.md](esb-lab/README.md).
- **Not run:** module 01 (JBang on the workstation), the module 04 IBM MQ bridge, module 06 step 4
  (AdviceWith test), module 07 native build. Their [V] items say what to check.
- **Watch out — Camel 4.21 renamed the Kafka headers** (`kafka.KEY` → `CamelKafkaKey`). Upstream
  4.22 (this lab) uses the new names; Red Hat's 4.18 build still uses the old ones. Module 03 §0.
- Modules 03 and 04 were drafted by sub-agents from the verified lab facts and checked against the
  Camel 4.22.1 source and docs.

# License

MIT — see [LICENSE](LICENSE).
