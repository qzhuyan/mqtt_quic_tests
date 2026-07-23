#!/usr/bin/env bash
set -euo pipefail

EMQX_IMAGE="${EMQX_IMAGE:-emqx/emqx:5.10.4}"
EMQX_CONTAINER="${EMQX_CONTAINER:-mqtt-quic-tests-emqx}"
EMQX_QUIC_PORT="${EMQX_QUIC_PORT:-14567}"
EMQX_DASHBOARD_PORT="${EMQX_DASHBOARD_PORT:-18083}"
SCENARIOS="${SCENARIOS:-connect pubsub multistream multistream-pub-5x100 parallel-no-blocking}"

cleanup() {
    docker logs "$EMQX_CONTAINER" || true
    docker rm -f "$EMQX_CONTAINER" || true
}

trap cleanup EXIT

docker rm -f "$EMQX_CONTAINER" >/dev/null 2>&1 || true

docker run \
    --detach \
    --name "$EMQX_CONTAINER" \
    --publish "${EMQX_QUIC_PORT}:14567/udp" \
    --publish "${EMQX_DASHBOARD_PORT}:18083" \
    --env EMQX_NODE__COOKIE=mqtt_quic_tests \
    --env EMQX_LISTENERS__QUIC__DEFAULT__ENABLE=true \
    --env EMQX_LISTENERS__QUIC__DEFAULT__BIND=14567 \
    --env EMQX_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__CACERTFILE='${EMQX_ETC_DIR}/certs/cacert.pem' \
    --env EMQX_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__CERTFILE='${EMQX_ETC_DIR}/certs/cert.pem' \
    --env EMQX_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__KEYFILE='${EMQX_ETC_DIR}/certs/key.pem' \
    --env EMQX_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__VERIFY=verify_none \
    "$EMQX_IMAGE"

for _ in $(seq 1 60); do
    if curl --fail --silent --show-error "http://127.0.0.1:${EMQX_DASHBOARD_PORT}/status" >/dev/null; then
        break
    fi
    sleep 2
done

curl --fail --silent --show-error "http://127.0.0.1:${EMQX_DASHBOARD_PORT}/status" >/dev/null

cargo build --locked --bin mqtt_quic_test

for scenario in $SCENARIOS; do
    target/debug/mqtt_quic_test \
        --host 127.0.0.1 \
        --port "$EMQX_QUIC_PORT" \
        --server-name localhost \
        --insecure \
        --timeout-ms 30000 \
        --client-id "mqtt-quic-ci-${scenario}" \
        --topic "mqtt/quic/ci/${scenario}" \
        --scenario "$scenario"
done
