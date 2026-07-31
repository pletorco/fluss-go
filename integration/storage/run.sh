#!/usr/bin/env bash

set -euo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly COMPOSE_FILE="$ROOT/integration/storage/compose.yml"
readonly PROJECT="fluss-go-storage-integration"
readonly MINIO_IMAGE="minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
readonly HADOOP_IMAGE="apache/hadoop@sha256:127774dadab40ce84df7ac668a7a8c99945688b3fe336f1388f4477ca33e1529"
readonly TARGET="${1:-all}"

export FLUSS_STORAGE_S3_PORT="${FLUSS_STORAGE_S3_PORT:-19000}"
export FLUSS_STORAGE_S3_ACCESS_KEY="${FLUSS_STORAGE_S3_ACCESS_KEY:-flussgotest}"
export FLUSS_STORAGE_S3_SECRET_KEY="${FLUSS_STORAGE_S3_SECRET_KEY:-$(od -An -N20 -tx1 /dev/urandom | tr -d ' \n')}"

compose() {
  docker compose --project-name "$PROJECT" --file "$COMPOSE_FILE" "$@"
}

diagnostics() {
  compose ps || true
  compose logs --no-color --tail=200 2>&1 |
    sed \
      -e "s/${FLUSS_STORAGE_S3_SECRET_KEY}/[REDACTED]/g" \
      -e "s/${FLUSS_STORAGE_S3_ACCESS_KEY}/[REDACTED]/g" || true
}

cleanup() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    diagnostics
  fi
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

case "$TARGET" in
  all) services=(minio hdfs-namenode hdfs-datanode) ;;
  s3) services=(minio) ;;
  hdfs) services=(hdfs-namenode hdfs-datanode) ;;
  *) printf 'usage: %s [all|s3|hdfs]\n' "$0" >&2; exit 2 ;;
esac

compose up --detach --wait --wait-timeout 180 "${services[@]}"

for service in "${services[@]}"; do
  container="$(compose ps --quiet "$service")"
  actual_image="$(docker inspect --format '{{.Config.Image}}' "$container")"
  case "$service" in
    minio) expected_image="$MINIO_IMAGE" ;;
    hdfs-*) expected_image="$HADOOP_IMAGE" ;;
  esac
  if [[ "$actual_image" != "$expected_image" ]]; then
    printf 'unexpected image for %s: %s\n' "$service" "$actual_image" >&2
    exit 1
  fi
done

if [[ "$TARGET" == all || "$TARGET" == s3 ]]; then
  export FLUSS_GO_S3_SERVICE=1
  export FLUSS_GO_S3_ENDPOINT="http://127.0.0.1:$FLUSS_STORAGE_S3_PORT"
  export AWS_REGION=us-east-1
  export AWS_ACCESS_KEY_ID="$FLUSS_STORAGE_S3_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$FLUSS_STORAGE_S3_SECRET_KEY"
  go -C "$ROOT/adapters/s3" test -tags=integration -count=1 -timeout=2m -v \
    -run '^TestS3AdapterService$' ./...
fi

if [[ "$TARGET" == all || "$TARGET" == hdfs ]]; then
  for _ in $(seq 1 60); do
    report="$(compose exec -T hdfs-namenode hdfs dfsadmin -report 2>&1 || true)"
    if grep -Fq 'Live datanodes (1)' <<<"$report"; then
      break
    fi
    sleep 2
  done
  report="$(compose exec -T hdfs-namenode hdfs dfsadmin -report 2>&1 || true)"
  if ! grep -Fq 'Live datanodes (1)' <<<"$report"; then
    printf 'HDFS datanode did not become live\n' >&2
    exit 1
  fi
  export FLUSS_HDFS_SERVICE=1
  export FLUSS_HDFS_COMPOSE_FILE="$COMPOSE_FILE"
  export FLUSS_HDFS_COMPOSE_PROJECT="$PROJECT"
  go -C "$ROOT/adapters/hdfs" test -tags=integration -count=1 -timeout=2m -v \
    -run '^TestHDFSAdapterService$' ./...
fi
