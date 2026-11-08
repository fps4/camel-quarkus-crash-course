---
title: "05 — Lab: break it on purpose — poison, outages, duplicates, blocked partitions, replay"
subtitle: Environment esb-lab (container on the lab host, or quarkus dev on your workstation) + Artemis, Postgres, WireMock, dc-a. About 90 minutes. Every expected result below was observed on 2026-11-08.
---

# Before you start

- Services up on the lab host, `tunnel.sh` running on your workstation.
- `esb-lab` running **once** — either the container (`docker compose --profile app up -d esb-lab`
  on the lab host) or `quarkus dev` on your workstation, not both.
- Shell helpers (your workstation, through the tunnel; or drop the `ssh labhost` part when on the
  lab host):

```bash
art()  { ssh labhost docker exec artemis bin/artemis "$@" --user esb --password esb --url tcp://localhost:61616; }
put()  { art producer --destination "queue://$1" --message-count 1 --message "$2" >/dev/null 2>&1; }
qstat(){ art queue stat 2>/dev/null | grep -E 'NAME|esb|DLQ'; }
applog(){ ssh labhost docker logs esb-lab --since "${1:-30s}" 2>&1 | grep -E "Item |Published|notified|Failed delivery|seek|ERROR" | cut -c1-220; }
cd ~/labs/camel-quarkus-crash-course
S=esb-lab/samples
```

(If you run `quarkus dev` on your workstation, read its console instead of `applog`.)

# Experiment 1 — poison vs good message

```bash
put esb.tracking.scan.in "$(cat $S/item-scan.xml)"
put esb.tracking.scan.in "$(cat $S/item-scan-poison.xml)"
sleep 3; applog; qstat
```

Expected: the good scan is bridged (`Item 0074123456 -> tracking.item-scan.recorded.v1`); the poison
message goes to `esb.tracking.scan.dlq` at once, **no "Failed delivery" retries** — because of
`onException(InvalidMessageException.class, JacksonException.class).maximumRedeliveries(0)` in
`ScanBridgeRoute`. In the Artemis console (http://localhost:8161 → Queues →
`esb.tracking.scan.dlq` → Browse) the body is the original broken XML (`useOriginalMessage()`).

**Change it:** comment out the `onException` line, rebuild/reload, send the poison again. Now you
see 4 attempts with back-off before it reaches the DLQ. Wasted time and log noise for a message that
could never succeed.

# Experiment 2 — duplicates

```bash
put esb.tracking.scan.in "$(cat $S/item-scan.xml)"   # the same scan again
sleep 3; applog
ssh labhost docker exec postgres psql -U esb -d esb -c "select * from camel_messageprocessed"
ssh labhost docker exec dc-a kafka-console-consumer --bootstrap-server dc-a:29092 \
  --topic tracking.item-scan.recorded.v1 --from-beginning --timeout-ms 5000 --formatter-property print.key=true
```

Expected: no new "Item" log line; one row per distinct scan in `camel_messageprocessed`; one Kafka
record per distinct scan.

**Think:** the key is `itemTag|location|scannedAt`. What if the legacy system re-sends with a new
`scannedAt` (its own clock at send time)? Then it is not a duplicate any more. The key must be chosen
with the source system's owner.

# Experiment 3 — a short outage turns good messages into dead ones

```bash
ssh labhost docker stop postgres
put esb.tracking.scan.in '<ItemScan><itemTag>0074555555</itemTag><orderId>SO-1001234</orderId><location>SCANNER-2</location><scannedAt>2026-11-08T13:10:00Z</scannedAt></ItemScan>'
sleep 12; applog; qstat
ssh labhost docker start postgres
```

Expected (observed): 4 attempts in about 6 seconds with `CannotCreateTransactionException: Could
not open JDBC Connection`, then the **valid** scan sits in `esb.tracking.scan.dlq`.

Replay it after Postgres is back:

```bash
05-reliability/replay-dlq.sh esb.tracking.scan.dlq esb.tracking.scan.in
sleep 5; applog; qstat
```

Expected (observed): `{"status":200,"moved":2}` — the good scan is now bridged, and the poison
message from experiment 1 is back in the DLQ. Replay is safe **because** the route is idempotent.

**Design question:** better retry policy? Options: longer exponential back-off (but it holds a
consumer thread), broker redelivery (experiment 4), or accept DLQ + automated replay.

# Experiment 4 — let the broker retry instead of Camel

The reference route already consumes with `transacted=true` (local JMS transaction: the message is
acked only when the route completes). Now take Camel's retries out, in your own copy:

```java
errorHandler(noErrorHandler());                                     // Camel does not retry
// keep the poison rule, but let it park the message itself:
onException(InvalidMessageException.class, JacksonException.class)
        .handled(true).useOriginalMessage().to("jms:queue:{{esb.queue.scan-dlq}}");
```

Repeat experiment 3 and watch for 60 s. Expected (observed 2026-11-08):

- `Execution of JMS message listener failed` three times, ~12–14 s apart — the gap is the JDBC
  connection timeout plus the broker's 1 s `redeliveryDelay`;
- after the third failure Artemis moves the message to **`DLQ.esb.tracking.scan.in`** (auto-created,
  routing type MULTICAST on address `DLQ`), as set in `broker.properties`;
- **surprise:** the default `DLQ` queue from `broker.xml` received a copy too. Address `DLQ` now
  has two queues, and a message sent to the address goes to both. In a real broker remove the
  default `DLQ` queue (or do not auto-create), or every dead letter exists twice.

Start Postgres again and replay from `DLQ.esb.tracking.scan.in` (console: Move, or the Jolokia
`moveMessages` call used by `replay-dlq.sh` with the MBean's `routing-type` set to `multicast`).

Compare: who holds the message during retries? (Broker — survives a pod crash.) What about
ordering with two concurrent consumers? (Lost on redelivery — another consumer may take the next
message first.)

# Experiment 5 — the blocked partition and the duplicate commands

```bash
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' -d @$S/order-status-ready-fail.json
sleep 2
curl -s -X POST localhost:8080/api/v1/order-status -H 'Content-Type: application/json' -d @$S/order-status-location-change.json
sleep 40; applog 45s; qstat
curl -s localhost:8089/__admin/requests | jq -r '.requests[].request.body | fromjson? | "\(.orderId) \(.location)"' | sort | uniq -c
```

WireMock answers 503 when `location` contains `FAIL`. Expected (observed):

- 5 attempts (`Failed delivery` at ~1 s, 2 s, 4 s, 8 s), then `Will seek consumer to offset …`, and
  the cycle repeats;
- `SO-1001234 ZONE-D7` (the location change sent second) **never reaches WireMock** — it waits behind
  the failing record on the same partition;
- `esb.status-board.cmd` gains one extra command per cycle — duplicates for the status board.

Unblock — the partner "recovers":

```bash
ID=$(curl -s localhost:8089/__admin/mappings | jq -r '.mappings[] | select(.name|test("FAIL")) | .id')
curl -s -X DELETE localhost:8089/__admin/mappings/$ID
sleep 20; applog 25s
ssh labhost docker exec dc-a kafka-consumer-groups --bootstrap-server dc-a:29092 --describe --group esb-status-board
curl -s -X POST localhost:8089/__admin/mappings/reset        # restore the FAIL/SLOW stubs
```

Expected: both events go through, `LAG 0`.

# Experiment 6 — fix the design (your change)

Pick one and implement it in your copy:

1. **Split consumers:** two routes on the same topic, `groupId=esb-status-board` (JMS only) and
   `groupId=esb-partner-webhook` (HTTP only). Repeat experiment 5: the status board keeps working while
   the partner is down; only the partner route blocks.
2. **Dead-letter topic:** replace `breakOnFirstError=true` with the DLC from notes §5. Repeat:
   the FAIL event ends in `orders.order-status.updated.v1.status-board.dlt` (look in kafka-ui),
   the SO-1001234 event is processed. You traded ordering for availability — say when that is OK.
3. **Timeout:** send `order-status-ready-slow.json` (WireMock waits 8 s; the route has
   `soTimeout=5000`). What happens, and is a timed-out call a failure or "unknown"? (Unknown — the
   partner may have processed it. Idempotent receiver again.)

# Teardown

```bash
art queue purge --name esb.tracking.scan.dlq
art queue purge --name esb.status-board.cmd
ssh labhost docker exec postgres psql -U esb -d esb -c "truncate camel_messageprocessed"
curl -s -X POST localhost:8089/__admin/mappings/reset; curl -s -X DELETE localhost:8089/__admin/requests
```

# [V] items

- `replay-dlq.sh` assumes an anycast DLQ; for the broker-created `DLQ.<queue>` (multicast) edit the
  MBean `routing-type` in the script.
- The DLT URI with the Avro serializer (experiment 6.2) is written from the docs, not run.
- The exact back-off timings depend on the redelivery policy; the lab used 1 s × 2ⁿ (Kafka route)
  and 0.5 s × 2ⁿ (JMS route).
