#!/usr/bin/env bash
# Forward the lab ports from the lab Docker host to localhost on your workstation.
#
# Why a tunnel: dc-a advertises its host listener as localhost:9092. A Kafka client on your workstation that
# connects to <labhost>:9092 gets "localhost:9092" back in the metadata and then fails. Through this
# tunnel localhost:9092 on the Mac IS dc-a's host listener, so the advertised address works, and
# every other service looks local too. Quarkus dev mode on the Mac then needs no special config.
#
# Usage:  ./tunnel.sh            (foreground; Ctrl-C to stop)
#         DS=otherhost ./tunnel.sh
# If a local port is taken (e.g. a Postgres on the Mac uses 5432), stop that service or change the
# left-hand port here and the matching URL in esb-lab/src/main/resources/application.properties.

set -euo pipefail
DS="${DS:-labhost}"   # SSH host alias of your Docker host (set DS=... or add "labhost" to ~/.ssh/config)

PORTS=(
  9092   # Kafka dc-a (host listener)
  8090   # Confluent Server REST v3 (dc-a)
  8081   # Schema Registry sr-a
  61616  # Artemis (all protocols)
  8161   # Artemis console
  5432   # Postgres
  8089   # WireMock (partner webhook)
  8070   # kafka-ui
  1414   # IBM MQ (profile ibmmq)
  9443   # IBM MQ console (profile ibmmq)
)

ARGS=()
for p in "${PORTS[@]}"; do ARGS+=(-L "${p}:localhost:${p}"); done

echo "Tunnelling ${PORTS[*]} from ${DS} to localhost. Ctrl-C to stop."
exec ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 "${ARGS[@]}" "${DS}"
