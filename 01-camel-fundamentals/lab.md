---
title: "01 — Lab: Camel JBang, YAML routes, EIPs, export to Quarkus"
subtitle: Environment your workstation + SSH tunnel to the lab host. About 90 minutes. Written 2026-11-08.
---

# Before you start

- 00-lab-setup done: services up on the lab host, `tunnel.sh` running, `camel --version` works on
  your workstation.
- Work in a scratch folder **outside** the course repo: `mkdir -p ~/labs/camel01 && cd ~/labs/camel01`.

Camel JBang downloads the components a route needs on first run (into `~/.m2`). The first run of
each step is slow; later runs are fast.

# Step 1 — hello route, and what an exchange looks like

```bash
camel init hello.yaml       # writes a timer -> log route
cat hello.yaml
camel run hello.yaml        # Ctrl-C to stop
```

Replace the content with a route that shows the exchange parts:

```yaml
- route:
    id: hello
    from:
      uri: timer:tick
      parameters:
        period: 3000
      steps:
        - setBody:
            simple: '{"orderId":"SO-1001234","status":"READY","location":"ZONE-D7"}'
        - setHeader:
            name: source
            constant: jbang
        - setProperty:
            name: traceNote
            constant: "stays inside the route"
        - log: "body=${body} header.source=${header.source} property=${exchangeProperty.traceNote}"
```

Run in dev mode (reloads on save) and watch it live from a second terminal:

```bash
camel run hello.yaml --dev
camel get route             # second terminal: routes, state, throughput
camel get                   # everything running under JBang
```

**Check yourself:** where would `header.source` go if the next step were `to: kafka:...`? (Kafka
record header.) And `traceNote`? (Nowhere — properties never leave the route.)

# Step 2 — content-based router, filter, splitter

`router.yaml`:

```yaml
- route:
    id: order-events
    from:
      uri: timer:tick
      parameters:
        period: 5000
      steps:
        - setBody:
            constant: |
              SO-1001234,READY,ZONE-D7
              SO-1001001,DELAYED,
              SO-1005501,LOCATION_CHANGE,ZONE-B12
              BOGUS
        - split:
            tokenize: "\n"
            streaming: true
            steps:
              - filter:
                  simple: ${body} != ''
                  steps:
                    - choice:
                        when:
                          - simple: ${body} contains 'LOCATION_CHANGE' || ${body} contains 'READY'
                            steps:
                              - log: "BOARD -> ${body}"
                          - simple: ${body} regex '^[A-Z]{2,4}-\d{4,10},.*'
                            steps:
                              - log: "OTHER -> ${body}"
                        otherwise:
                          steps:
                            - log:
                                message: "REJECT -> ${body}"
                                loggingLevel: WARN
```

```bash
camel run router.yaml
```

Try: remove the `otherwise` and run again. `BOGUS` now disappears without a trace — the classic
silent-drop bug in content-based routers.

# Step 3 — talk to the lab Kafka (dc-a on the lab host, through the tunnel)

`application.properties` in the same folder (JBang reads it automatically):

```properties
camel.component.kafka.brokers=localhost:9092
```

`to-kafka.yaml`:

```yaml
- route:
    id: produce
    from:
      uri: timer:tick
      parameters:
        period: 2000
      steps:
        - setBody:
            simple: '{"orderId":"SO-${random(1000000,9999999)}","status":"READY","at":"${date:now:yyyy-MM-dd''T''HH:mm:ss}"}'
        - setHeader:
            name: CamelKafkaKey      # Camel 4.21+; it was kafka.KEY before (and still is on Red Hat's 4.18 build)
            simple: ${body}
        - to: kafka:lab01.order-status
        - log: "sent ${body}"
- route:
    id: consume
    from:
      uri: kafka:lab01.order-status
      parameters:
        groupId: lab01
        autoOffsetReset: earliest
      steps:
        - log: "got partition=${header.CamelKafkaPartition} offset=${header.CamelKafkaOffset} ${body}"
```

```bash
camel run to-kafka.yaml application.properties
open http://localhost:8070     # kafka-ui -> dc-a -> Topics -> lab01.order-status
```

Note: the key here is the whole body — a bad key. Change it to the order id only:
`simple: ${body}` → use `jsonpath: $.orderId` under `setHeader`. Why does it matter? Same
order → same partition → events for that order stay in order.

# Step 4 — Artemis from JBang

```properties
# add to application.properties
camel.beans.cf=#class:org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory("tcp://localhost:61616","esb","esb")
camel.component.jms.connection-factory=#cf
```

`to-artemis.yaml`:

```yaml
- route:
    id: put-scan
    from:
      uri: timer:tick
      parameters:
        period: 4000
        repeatCount: 5
      steps:
        - setBody:
            constant: "<ItemScan><itemTag>0074123456</itemTag><orderId>SO-1001234</orderId><location>SCANNER-3</location><scannedAt>2026-11-08T13:05:12Z</scannedAt></ItemScan>"
        - to: jms:queue:lab01.scan
- route:
    id: take-scan
    from:
      uri: jms:queue:lab01.scan
      steps:
        - log: "queue gave me: ${body}"
```

```bash
camel run to-artemis.yaml application.properties --dep=org.apache.activemq:artemis-jakarta-client:2.57.0
```

Open the Artemis console (http://localhost:8161, esb/esb) → Queues → `lab01.scan`: messages added,
acknowledged, consumer count. If the `#class:` bean syntax gives you trouble, skip this step —
module 04 does the same in Quarkus, where the connection factory is configured for you. [V]

Then stop the consumer route (comment it out), produce again, and look at the queue depth in the
console. Messages wait in the queue — unlike Kafka, where they wait in the log whether anyone reads
them or not, and are **removed** from the queue once acknowledged.

# Step 5 — Kaoto (visual designer)

In VS Code with the Camel extension pack: open `router.yaml` → "Open with Kaoto". You see the
same route as a diagram. Add a `log` step in the diagram, save, and look at the YAML diff.
This is what a low-code pitch to the business looks like — useful to know, and worth an opinion:
diagrams help reviews, YAML in Git stays the source of truth.

# Step 6 — export to a Quarkus project

```bash
camel export to-kafka.yaml application.properties \
  --runtime=quarkus --gav=lab.esb:camel01:1.0.0 --directory=camel01-quarkus
cd camel01-quarkus
cat pom.xml | grep -E 'quarkus.platform.version|camel-quarkus-'
./mvnw quarkus:dev            # or: mvn quarkus:dev
```

Look at what export produced: a normal Maven project, the YAML route under
`src/main/resources/camel/`, extensions picked from the components the route used. Module 02
starts from here.

# Teardown

```bash
camel stop                    # stop anything still running under JBang
# topic cleanup (runs on the lab host):
ssh labhost 'docker exec dc-a kafka-topics --bootstrap-server dc-a:29092 --delete --topic lab01.order-status'
```

# [V] items

- `camel export` flags (`--runtime=quarkus`, `--gav`, `--directory`) — check `camel export --help`
  on your version.
- JBang JMS with Artemis: the `#class:` bean syntax with constructor arguments, and whether the
  dependency is pulled automatically; `--dep` added to be safe.
- `jsonpath` in `setHeader` needs camel-jsonpath, which JBang downloads automatically.
