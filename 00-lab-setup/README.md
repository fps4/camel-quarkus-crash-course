---
title: 00 — Lab setup (Camel / Quarkus)
subtitle: Supporting services on the lab host next to the Confluent lab cluster, an SSH tunnel for your workstation, and the tool chain. Written 2026-11-08.
---

# The shape of the lab

```
 your workstation                              the lab host (any Linux Docker host; tested on
 ────────────────                              Ubuntu 24.04, x86_64, 32 GB) — SSH alias labhost
                                               ─────────────────────────────────────────────
 IDE + JDK 21 + Maven + JBang                  network crash-course_default
 quarkus dev  (esb-lab)  ── ssh tunnel ──►       dc-a  Kafka (CP 8.4)  :9092  REST :8090
 camel run    (JBang)       localhost:*          sr-a  Schema Registry :8081
                                                 artemis               :61616  console :8161
                                                 postgres              :5432
                                                 wiremock (partner)    :8089
                                                 kafka-ui              :8070
                                                 ibm-mq  (profile)     :1414   console :9443
                                                 esb-lab (profile app) :8080
```

- **Kafka is not duplicated.** dc-a and sr-a come from the
  [Confluent crash course](https://github.com/fps4/confluent-crash-course) compose
  (`../../confluent-crash-course/00-lab-setup`). This compose joins its network
  `crash-course_default`, so Camel routes reach Kafka as `dc-a:29092`, like any other client in
  that lab. dc-b is not needed here; start it only for the Cluster Linking modules.
- **Develop on your workstation, run the brokers on the lab host.** `tunnel.sh` forwards every lab
  port, so on your workstation everything is `localhost`. That also solves the Kafka
  advertised-listener problem: dc-a advertises `localhost:9092`, which through the tunnel is the
  right address.
- **Containers only on the lab host.** The app image builds with a multi-stage Dockerfile (Maven
  inside Docker), so the lab host needs no JDK.
- **One machine is fine too.** If your workstation is itself a Linux Docker host, run everything
  there and skip the tunnel.

Versions pinned 2026-11-08:

| Piece | Version | Note |
|---|---|---|
| Quarkus platform | **3.40.1** | `io.quarkus.platform:quarkus-bom` + `quarkus-camel-bom` |
| Camel Quarkus | **3.40.0** (LTS) | released 2026-09-28 |
| Apache Camel | **4.22.x** (LTS) | 4.22.2 released 2026-10-22; 4.23.0 is non-LTS |
| Red Hat build of Apache Camel | 4.18 (supported build; lags upstream) [V] | what a Red Hat subscription supports |
| Apache Artemis | **2.57.0**, image `apache/artemis` | Artemis left ActiveMQ and became a top-level Apache project; old image `apache/activemq-artemis` stopped at 2.44 |
| quarkus-artemis-jms (Quarkiverse) | 3.14.3 | not in the platform BOM; version from the upstream message-bridge example |
| Confluent `kafka-avro-serializer` | 8.4.0 | matches CP 8.4 on dc-a |
| IBM MQ developer image | 9.4.5.0-r1 | x86_64 only — fine on an x86_64 lab host |
| PostgreSQL / WireMock / kafka-ui | 17.11 / 3.13.2 / kafbat v1.5.0 | |

# One-time setup

## On the lab host — get the files there

The compose files use bind mounts (`./work`, `./artemis/etc-override`, `./wiremock`), and bind
mounts resolve **on the Docker host**. So run compose on the lab host itself, from clones of the two
course repos — not from your workstation with `docker --context labhost`, which would mount empty
folders.

```bash
ssh labhost
mkdir -p ~/labs && cd ~/labs
git clone https://github.com/fps4/confluent-crash-course.git      # or: git -C confluent-crash-course pull
git clone https://github.com/fps4/camel-quarkus-crash-course.git  # or: git -C camel-quarkus-crash-course pull
```

The two repos must sit side by side: the paths below, and the comments in `docker-compose.yml`,
assume `~/labs/confluent-crash-course` and `~/labs/camel-quarkus-crash-course`.

## On your workstation — tool chain

You need JDK 21, Maven and JBang. On macOS:

```bash
brew install openjdk@21 maven jbang
brew install quarkusio/tap/quarkus           # Quarkus CLI (optional; mvn quarkus:dev works too)
sudo ln -sfn "$(brew --prefix)/opt/openjdk@21/libexec/openjdk.jdk" /Library/Java/JavaVirtualMachines/openjdk-21.jdk
java -version                                # 21.x
jbang app install camel@apache/camel         # Camel JBang CLI: `camel --version`
```

On Linux, use your package manager or [SDKMAN!](https://sdkman.io) for JDK 21 and Maven, then
install JBang and the Camel CLI the same way.

Also clone the Camel course on your workstation (`~/labs/camel-quarkus-crash-course`), for
`tunnel.sh` and the reference code.

Add the lab host to `~/.ssh/config` as `labhost`, so `ssh labhost` works without a password
prompt. `tunnel.sh` uses that alias by default (`DS=otherhost ./tunnel.sh` to override).

IDE: IntelliJ IDEA or VS Code with the **Extension Pack for Apache Camel** (includes **Kaoto**, the
visual route designer, and the Camel debugger). Red Hat's VS Code tooling is the same set.

# Start, check, stop

On the lab host:

```bash
cd ~/labs/confluent-crash-course/00-lab-setup && docker compose up -d dc-a sr-a   # Kafka + Schema Registry (Confluent course)
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose up -d        # artemis, postgres, wiremock, kafka-ui
docker compose ps
```

On your workstation (separate terminal, leave it running):

```bash
~/labs/camel-quarkus-crash-course/00-lab-setup/tunnel.sh
```

Smoke test from your workstation:

```bash
curl -s localhost:8090/kafka/v3/clusters | jq -r '.data[0].cluster_id'   # Q0PvpgzYSwirapvmRY0NFg
curl -s localhost:8081/subjects                                           # []
curl -s -o /dev/null -w '%{http_code}\n' localhost:8161/console/          # 302 (login page)
curl -s -X POST localhost:8089/partner/webhook -H 'Content-Type: application/json' -d '{"orderId":"SO-1","location":"ZONE-D7"}'
open http://localhost:8070      # kafka-ui: topics, messages, SR subjects on dc-a
open http://localhost:8161      # Artemis console, user esb / esb
```

The cluster id is the one from the test run; yours will differ. `open` is macOS; on Linux use
`xdg-open` or paste the URL in a browser.

Stop and reset (on the lab host):

```bash
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile app --profile ibmmq stop     # keep data
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile app --profile ibmmq down -v  # wipe Artemis, Postgres, MQ
cd ~/labs/confluent-crash-course/00-lab-setup && docker compose stop dc-a sr-a
```

# The reference app in a container

`esb-lab` is the finished solution of modules 02–07. To run it on the lab host instead of on your
workstation:

```bash
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile app up -d --build esb-lab    # first build downloads ~250 MB of Maven deps
docker logs -f esb-lab
```

It uses the Docker service names (`dc-a:29092`, `sr-a:8081`, `artemis:61616`, …) through
environment variables; see `esb-lab/src/main/resources/application.properties`. Do not run it on
the lab host and in `quarkus dev` on your workstation at the same time: both would consume the same
queues and consumer group, and you would see only half the messages on each side.

# Artemis settings

`artemis/etc-override/broker.properties` is copied into the broker's `etc/` **only when the
instance is first created**. After a change: `docker compose rm -sf artemis && docker volume rm
camel-lab_artemis-data && docker compose up -d artemis`.

It sets, for every address under `esb.`: max 3 delivery attempts, 1 s redelivery delay, then an
auto-created dead-letter queue `DLQ.<queue>`. Verified 2026-11-08 through Jolokia and in module 05
experiment 4. Note: the default `DLQ` queue from `broker.xml` sits on the same address and also gets
a copy of every dead letter. This matters only when a consumer uses a **transacted**
session and rolls back. Module 05 shows the difference between broker redelivery and Camel
redelivery.

# WireMock — the partner webhook

`wiremock/mappings/partner-webhook.json`:

| Request body `location` contains | Response |
|---|---|
| `FAIL` | 503 — practise retries and back-off |
| `SLOW` | 200 after 8 s — practise timeouts |
| anything else | 200 `{"received":true}` |

See what WireMock received: `curl -s localhost:8089/__admin/requests | jq '.requests[].request.body'`.

# IBM MQ (optional, module 04)

```bash
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile ibmmq up -d ibm-mq
```

Queue manager `QM1`, channel `DEV.APP.SVRCONN`, user `app` / `passw0rd`, queues `DEV.QUEUE.1..3`.
Console: https://localhost:9443/ibmmq/console (user `admin` / `passw0rd`, self-signed certificate).
Licence: IBM MQ Advanced for Developers, non-production.

# Status

Tested on the lab host (Ubuntu 24.04, x86_64, 32 GB) on 2026-11-08: dc-a + sr-a start healthy;
Artemis 2.57.0 starts and copies `broker.properties`; WireMock returns 200 / 503 as mapped; kafka-ui
and the Artemis console answer. See the [course README](../README.md) for what was tested end to
end in the app.
