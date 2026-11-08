#!/usr/bin/env bash
# Move every message from an Artemis dead-letter queue back to its input queue (replay).
# Uses the broker's Jolokia endpoint (console port 8161). Run on your workstation through tunnel.sh, or on the lab host.
#
#   ./replay-dlq.sh esb.tracking.scan.dlq esb.tracking.scan.in            # move all
#   FILTER="JMSPriority > 4" ./replay-dlq.sh esb.tracking.scan.dlq esb.tracking.scan.in
#     (FILTER is a JMS selector on message properties, not on the body; empty = all)
#
# Tested against Artemis 2.57.0: returned {"status":200,"value":2} and the good message
# was bridged while the poison message went back to the DLQ.
set -euo pipefail
DLQ="${1:?dead-letter queue name}"
TARGET="${2:?target queue name}"
FILTER="${FILTER:-}"
HOST="${ARTEMIS_HTTP:-http://localhost:8161}"
CRED="${ARTEMIS_CRED:-esb:esb}"
BROKER="${ARTEMIS_BROKER:-0.0.0.0}"   # broker name as shown in the console / Jolokia search

MBEAN="org.apache.activemq.artemis:broker=\"${BROKER}\",component=addresses,address=\"${DLQ}\",subcomponent=queues,routing-type=\"anycast\",queue=\"${DLQ}\""
BODY=$(jq -n --arg m "$MBEAN" --arg f "$FILTER" --arg t "$TARGET" \
  '{type:"exec", mbean:$m, operation:"moveMessages(java.lang.String,java.lang.String)", arguments:[$f,$t]}')

curl -s -u "$CRED" -H "Origin: http://localhost" -H "Content-Type: application/json" \
  -X POST "${HOST}/console/jolokia/" -d "$BODY" | jq '{status, moved: .value, error}'
