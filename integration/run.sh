#!/usr/bin/env bash

set -euo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly COMPOSE_FILE="$ROOT/integration/compose.yml"
readonly TLS_COMPOSE_FILE="$ROOT/integration/compose-tls.yml"
readonly PROJECT="fluss-go-integration"
readonly TLS_PROJECT="fluss-go-tls-integration"
readonly FLUSS_IMAGE="apache/fluss@sha256:ff461b45438033da4fd1c2556d3f978f3603bb3632fe075c2bd57388339a58cb"
readonly HAPROXY_IMAGE="haproxy@sha256:66e25cc9a8332635f4e897f7f4b1e5622c25f09f0ee23cddc6ce9bdb3a24772a"
readonly FLUSS_VERSION="1.0.0"
readonly FLUSS_COMMIT="5c07f88e50a8ff41b0ebc214a0458e5dba60be37"
readonly TLS_DIR="$(mktemp -d)"

export FLUSS_INTEGRATION=1
export FLUSS_VERSION
export FLUSS_COMMIT
export FLUSS_IMAGE
export FLUSS_COMPOSE_FILE="$COMPOSE_FILE"
export FLUSS_COMPOSE_PROJECT="$PROJECT"
export FLUSS_TLS_COMPOSE_FILE="$TLS_COMPOSE_FILE"
export FLUSS_TLS_COMPOSE_PROJECT="$TLS_PROJECT"
export FLUSS_TLS_DIR="$TLS_DIR"
export FLUSS_TLS_CA_FILE="$TLS_DIR/ca.crt"
export FLUSS_TLS_SERVER_NAME="fluss.test"
export FLUSS_RELIABILITY_PROFILE="${FLUSS_RELIABILITY_PROFILE:-smoke}"
export FLUSS_RELIABILITY_REPORT="${FLUSS_RELIABILITY_REPORT:-$ROOT/.task/reliability/${FLUSS_RELIABILITY_PROFILE}.json}"
export FLUSS_PLAIN_COORDINATOR_PORT="${FLUSS_PLAIN_COORDINATOR_PORT:-19123}"
export FLUSS_PLAIN_TABLET_0_PORT="${FLUSS_PLAIN_TABLET_0_PORT:-19124}"
export FLUSS_PLAIN_TABLET_1_PORT="${FLUSS_PLAIN_TABLET_1_PORT:-19125}"
export FLUSS_PLAIN_TABLET_2_PORT="${FLUSS_PLAIN_TABLET_2_PORT:-19126}"
export FLUSS_SASL_COORDINATOR_PORT="${FLUSS_SASL_COORDINATOR_PORT:-19223}"
export FLUSS_SASL_TABLET_PORT="${FLUSS_SASL_TABLET_PORT:-19224}"
export FLUSS_SASL_USERNAME="${FLUSS_SASL_USERNAME:-integration_admin}"
export FLUSS_SASL_PASSWORD="${FLUSS_SASL_PASSWORD:-$(openssl rand -hex 24)}"
export FLUSS_SASL_ACL_USERNAME="${FLUSS_SASL_ACL_USERNAME:-integration_acl_user}"
export FLUSS_SASL_ACL_PASSWORD="${FLUSS_SASL_ACL_PASSWORD:-$(openssl rand -hex 24)}"

compose() {
  docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" "$@"
}

tls_compose() {
  docker compose --project-name "$TLS_PROJECT" --file "$TLS_COMPOSE_FILE" "$@"
}

generate_tls_material() {
  openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
    -keyout "$TLS_DIR/ca.key" -out "$TLS_DIR/ca.crt" \
    -subj "/CN=fluss-go integration CA" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -sha256 -nodes \
    -keyout "$TLS_DIR/server.key" -out "$TLS_DIR/server.csr" \
    -subj "/CN=fluss.test" -addext "subjectAltName=DNS:fluss.test" >/dev/null 2>&1
  openssl x509 -req -sha256 -days 1 \
    -in "$TLS_DIR/server.csr" -CA "$TLS_DIR/ca.crt" -CAkey "$TLS_DIR/ca.key" \
    -CAcreateserial -copy_extensions copy -out "$TLS_DIR/server.crt" >/dev/null 2>&1
  cat "$TLS_DIR/server.crt" "$TLS_DIR/server.key" >"$TLS_DIR/server.pem"
  chmod 0711 "$TLS_DIR"
  chmod 0600 "$TLS_DIR"/*.key
  chmod 0644 "$TLS_DIR"/*.crt "$TLS_DIR/server.pem"
}

diagnostics() {
  compose ps || true
  compose logs --no-color --tail=200 2>&1 |
    sed \
      -e "s/${FLUSS_SASL_PASSWORD}/[REDACTED]/g" \
      -e "s/${FLUSS_SASL_ACL_PASSWORD}/[REDACTED]/g" ||
    true
  tls_compose ps || true
  tls_compose logs --no-color --tail=200 2>&1 |
    sed -e "s/${FLUSS_SASL_PASSWORD}/[REDACTED]/g" || true
}

cleanup() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    diagnostics
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  tls_compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$TLS_DIR"
  exit "$status"
}
trap cleanup EXIT

if [[ "${FLUSS_RELIABILITY_ONLY:-0}" != "1" ]]; then
  generate_tls_material
fi

docker run --rm --entrypoint test "$FLUSS_IMAGE" -f "/opt/fluss/lib/fluss-server-${FLUSS_VERSION}.jar"
compose up --detach --wait --wait-timeout 180

for service in plaintext-coordinator plaintext-tablet-0 plaintext-tablet-1 plaintext-tablet-2 sasl-coordinator sasl-tablet; do
  container="$(compose ps --quiet "$service")"
  actual_image="$(docker inspect --format '{{.Config.Image}}' "$container")"
  if [[ "$actual_image" != "$FLUSS_IMAGE" ]]; then
    printf 'unexpected image for %s: %s\n' "$service" "$actual_image" >&2
    exit 1
  fi
done

cd "$ROOT"
go test ./pkg/fmsg -run '^TestProtocolGoldenBytes$'
go test ./pkg/fgo -run '^Test(CompactedPrimaryKeyMatchesJavaFixture|RowsMatchJava091Fixtures|KVAndLogBatchesMatchJava091Fixtures|LogBatchEncodesV0AndV1Headers|ArrowLogBatchDecodesJava091Fixture|FlussBucketMatchesJava091)$'
go test -tags=integration -run '^TestReliabilityConfigValidation$' ./integration

if [[ "${FLUSS_RELIABILITY_ONLY:-0}" == "1" ]]; then
  selected_reliability="$(go test -tags=integration -list '^TestFluss100Reliability$' ./integration)"
  if ! grep -qx 'TestFluss100Reliability' <<<"$selected_reliability"; then
    printf 'no Fluss 1.0 reliability test was selected\n' >&2
    exit 1
  fi
  go test -tags=integration -count=1 -timeout=35m -v -run '^TestFluss100Reliability$' ./integration
  exit 0
fi

selected_tests="$(go test -tags=integration -list '^TestFluss100(Integration|Reliability)$' ./integration)"
for test_name in TestFluss100Integration TestFluss100Reliability; do
  if ! grep -qx "$test_name" <<<"$selected_tests"; then
    printf 'required Fluss 1.0 live test %s was not selected\n' "$test_name" >&2
    exit 1
  fi
done
go test -tags=integration -count=1 -timeout=7m -v -run '^TestFluss100(Integration|Reliability)$' ./integration

compose down --volumes --remove-orphans >/dev/null
tls_compose up --detach --wait --wait-timeout 180

for service in tls-coordinator tls-tablet-0 tls-tablet-1 tls-tablet-2 tls-sasl-coordinator tls-sasl-tablet; do
  container="$(tls_compose ps --quiet "$service")"
  actual_image="$(docker inspect --format '{{.Config.Image}}' "$container")"
  if [[ "$actual_image" != "$FLUSS_IMAGE" ]]; then
    printf 'unexpected image for %s: %s\n' "$service" "$actual_image" >&2
    exit 1
  fi
done
proxy_container="$(tls_compose ps --quiet tls-proxy)"
actual_proxy_image="$(docker inspect --format '{{.Config.Image}}' "$proxy_container")"
if [[ "$actual_proxy_image" != "$HAPROXY_IMAGE" ]]; then
  printf 'unexpected image for tls-proxy: %s\n' "$actual_proxy_image" >&2
  exit 1
fi

selected_tls_tests="$(go test -tags=integration -list '^TestFluss100TLSIntegration$' ./integration)"
if ! grep -qx 'TestFluss100TLSIntegration' <<<"$selected_tls_tests"; then
  printf 'no Fluss 1.0 TLS integration test was selected\n' >&2
  exit 1
fi
go test -tags=integration -count=1 -timeout=3m -v -run '^TestFluss100TLSIntegration$' ./integration
