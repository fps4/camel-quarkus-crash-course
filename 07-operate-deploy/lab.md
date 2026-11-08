---
title: "07 — Lab: run it like production — container, probes, metrics, manifests, scaling"
subtitle: Environment the lab host (esb-lab container) + a browser on your workstation through the tunnel. About 60–90 minutes. Written 2026-11-08.
---

# Before you start

Services and dc-a/sr-a up on the lab host. Stop `quarkus dev` on your workstation if it runs —
this lab uses the container. Add `8080` to the tunnel if you want to call the app from your
workstation (it is not in `tunnel.sh` because dev mode uses that port on the workstation);
otherwise run the `curl`s on the lab host.

# Step 1 — build and run the container

On the lab host:

```bash
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile app up -d --build esb-lab
docker logs esb-lab 2>&1 | grep -E "Routes startup|Started |started in|Installed features"
docker stats --no-stream esb-lab
docker images esb-lab
```

Expected (observed): `Apache Camel 4.22.1 (esb-lab) started in ~0.8 s`, Quarkus `started in
~2.6 s`, ~340 MiB memory, image ~620 MB. Read the `Dockerfile`: why two stages, why `USER 1001`
(OpenShift runs containers with an arbitrary non-root UID), why `-XX:MaxRAMPercentage=75`
(the JVM sizes its heap from the container memory limit).

# Step 2 — probes under failure

```bash
curl -s localhost:8080/q/health | jq -c '[.status, (.checks[] | {name,status})]'
docker stop artemis
sleep 8
curl -s -o /dev/null -w 'live %{http_code}\n'  localhost:8080/q/health/live
time curl -s -o /dev/null -w 'ready %{http_code}\n' localhost:8080/q/health/ready
docker logs esb-lab --since 30s 2>&1 | grep -E "JMS|refresh" | cut -c1-160
docker start artemis; sleep 12
curl -s -o /dev/null -w 'ready %{http_code}\n' localhost:8080/q/health/ready
```

Expected (observed): live 200; ready **503** with `Artemis JMS health check: DOWN`, and the ready
call takes ~4 s; the JMS listener logs `Could not refresh JMS Connection` and reconnects by itself;
ready back to 200.

Now write down the probe settings you would put in the Deployment (timeout > 4 s for readiness;
liveness without dependencies) and why.

Repeat with `docker stop postgres` and with `docker stop dc-a` (on the Confluent compose). Which
checks go DOWN for Kafka? [V — the Kafka consumer health check behaviour was not tested]

# Step 3 — metrics

```bash
curl -s localhost:8080/q/metrics | grep -E '^camel_exchanges_(succeeded|failed)_total'
curl -s localhost:8080/q/metrics | grep -E '^camel_route_policy_seconds_(count|max)'
```

Send a few order-status POSTs and item scans (module 05 helpers), call again, and watch the
counters move. Then answer: which metric shows the **blocked partition** from module 05?
(None in the app — `camel_exchanges_failed_total` grows, but the clear signal is consumer lag on
the broker side: `kafka-consumer-groups --describe --group esb-status-board`.)

Optional: run Prometheus + Grafana on the lab host and scrape `esb-lab:8080/q/metrics` on the
`crash-course_default` network. Not needed for this course.

# Step 4 — generated Kubernetes manifests

The project has `quarkus-kubernetes`, so `mvn package` writes manifests. Build them without
installing Maven, in a throw-away container on the lab host:

```bash
cd ~/labs/camel-quarkus-crash-course/esb-lab
docker run --rm -v esb-m2:/root/.m2 -v "$PWD":/src -w /src maven:3.9.16-eclipse-temurin-21 \
  mvn -B -q package -DskipTests
sed -n '1,200p' target/kubernetes/kubernetes.yml
```

Find: the Deployment, the probes (`/q/health/live`, `/q/health/ready`, `/q/health/started`), the
container port, the image name (`esb-lab:dev` from `quarkus.container-image.image`). Then add to
`application.properties` and rebuild:

```properties
quarkus.kubernetes.replicas=2
quarkus.kubernetes.resources.requests.memory=384Mi
quarkus.kubernetes.resources.limits.memory=512Mi
quarkus.kubernetes.readiness-probe.timeout=5s
quarkus.kubernetes.env.vars.kafka-bootstrap=dc-a:29092
quarkus.kubernetes.env.secrets=esb-lab-credentials
```

Compare the new YAML. In an ArgoCD setup this file is a starting point; the real manifests live in
the GitOps repo with per-environment overlays.

Optional: swap `quarkus-kubernetes` for `quarkus-openshift` and look at the extra `Route` and
`ImageStream`/`BuildConfig` objects.

# Step 5 — two replicas: what is safe? (thought experiment + small test)

On the lab host, start a second copy of the app with a different name and port:

```bash
docker run -d --name esb-lab-2 --network crash-course_default -p 8085:8080 \
  -e KAFKA_BOOTSTRAP=dc-a:29092 -e SR_URL=http://sr-a:8081 -e ARTEMIS_URL=tcp://artemis:61616 \
  -e PG_URL=jdbc:postgresql://postgres:5432/esb -e PARTNER_WEBHOOK_URL=http://wiremock:8080/partner/webhook \
  esb-lab:dev
sleep 10
docker exec artemis bin/artemis queue stat --user esb --password esb --url tcp://localhost:61616 | grep scan.in   # 4 consumers now
docker exec dc-a kafka-consumer-groups --bootstrap-server dc-a:29092 --describe --group esb-status-board    # 1 partition -> one replica idle
```

Send 10 item scans with different item tags and see them spread across both apps
(`docker logs esb-lab` / `docker logs esb-lab-2`). Send the same scan to both — the shared Postgres
idempotent repository still lets only one through.

Then answer for each route: Kafka (bounded by partitions — `orders.order-status.updated.v1` has
1 partition, so the second consumer idles), JMS queue (competing consumers; ordering per item lost
without message groups), HTTP (fine). And if someone adds `from("timer:...")` or
`from("sftp:...")`? Both replicas would poll — the case for `master:`.

```bash
docker rm -f esb-lab-2
```

# Step 6 (optional) — native image

```bash
docker run --rm -v esb-m2:/root/.m2 -v "$PWD":/src -w /src \
  quay.io/quarkus/ubi9-quarkus-mandrel-builder-image:jdk-21 \
  ./mvnw -B package -Dnative -DskipTests            # or install mvn in the image; several minutes, 6+ GB RAM [V]
```

Native with Avro + the Confluent serializer + Artemis may need extra reflection config — Quarkus'
Confluent extension lists native support only for older serializer versions (6.2–7.3). Treat this
as an experiment, and note what breaks: that is exactly the "native caveats" answer. [V]

# Teardown

```bash
cd ~/labs/camel-quarkus-crash-course/00-lab-setup && docker compose --profile app stop esb-lab
docker rm -f esb-lab-2 2>/dev/null || true
```

# [V] items

- Kafka consumer health check behaviour when dc-a is down.
- `quarkus.kubernetes.*` property names for env vars from secrets — check the Quarkus Kubernetes
  guide for your version.
- Native build of this project (not attempted; the Mandrel image name and `mvnw` presence — the
  reference project has no Maven wrapper, so add one with `mvn wrapper:wrapper` first).
