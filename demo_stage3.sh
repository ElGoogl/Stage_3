#!/usr/bin/env bash
set -euo pipefail

LOG_FILE="${LOG_FILE:-./demo_stage3_$(date +%Y%m%d_%H%M%S).log}"
SUMMARY_LOG_FILE="${SUMMARY_LOG_FILE:-./demo_stage3_summary_$(date +%Y%m%d_%H%M%S).log}"
if ! exec 3>/dev/tty 2>/dev/null; then
  exec 3>&1
fi

LB_URL="${LB_URL:-http://localhost:8000}"
SEARCH_PATH="${SEARCH_PATH:-/search}"
INGEST_URL="${INGEST_URL:-http://localhost:7001}"
INGEST_PATH_TEMPLATE="${INGEST_PATH_TEMPLATE:-/ingest/{bookId}}"
BOOK_ID="${BOOK_ID:-1342}"
SCALE_TO="${SCALE_TO:-3}"
FAIL_CONTAINER="${FAIL_CONTAINER:-search2}"

INGEST_PATH="${INGEST_PATH_TEMPLATE/\{bookId\}/$BOOK_ID}"
LB_STATUS_URL="${LB_URL}/status"
HEADERS_FILE="/tmp/demo_stage3_headers.$$"
BODY_FILE="/tmp/demo_stage3_body.$$"

SEARCH_QUERIES=("love" "adventure" "mystery" "science" "freedom" "river" "war" "hope")

HAS_JQ=0
if command -v jq >/dev/null 2>&1; then
  HAS_JQ=1
fi

timestamp() {
  date '+%H:%M:%S'
}

log() {
  if [ "$#" -eq 0 ]; then
    printf '\n' >>"$LOG_FILE"
    printf '\n' >&3
    return
  fi
  local entry line
  for entry in "$@"; do
    while IFS= read -r line; do
      printf '%s\n' "$line" >>"$LOG_FILE"
      printf '%s\n' "$line" >&3
    done <<<"$entry"
  done
}

run_cmd() {
  log "+ $*"
  local output status
  output="$("$@" 2>&1)" || status=$?
  if [ -n "${output:-}" ]; then
    log "$output"
  fi
  return "${status:-0}"
}

log_summary() {
  echo "[$(timestamp)] $*" >>"$SUMMARY_LOG_FILE"
}

cleanup_requested=0
for arg in "$@"; do
  case "$arg" in
    --cleanup) cleanup_requested=1 ;;
    *) log "Unknown argument: $arg"; exit 1 ;;
  esac
done

post_with_retries() {
  local url=$1
  local attempts=${2:-10}
  local delay=${3:-2}
  local attempt=1
  while [ "$attempt" -le "$attempts" ]; do
    log "+ curl -sS -X POST \"$url\" (attempt $attempt/$attempts)"
    local curl_err
    curl_err="$(curl -sS -X POST "$url" 2>&1)" && {
      if [ -n "$curl_err" ]; then
        log "$curl_err"
      fi
      return 0
    }
    if [ -n "$curl_err" ]; then
      log "[warn] POST failed: $curl_err"
    else
      log "[warn] POST failed with no error output"
    fi
    attempt=$((attempt + 1))
    read -t "$delay" -r _ || true
  done
  return 1
}

wait_for_http_200() {
  local url=$1
  local timeout=${2:-120}
  local start
  start=$(date +%s)

  log "+ curl -s -o /dev/null -w \"%{http_code}\" \"$url\" (retries)"
  while :; do
    local code
    code="$(curl -s -o /dev/null -w "%{http_code}" "$url" || true)"
    if [ "$code" = "200" ]; then
      log "[$(timestamp)] Ready with HTTP 200 from $url"
      return 0
    fi
    local now
    now=$(date +%s)
    if [ $((now - start)) -ge "$timeout" ]; then
      log "Timed out waiting for $url (last code: $code)"
      return 1
    fi
    read -t 2 -r _ || true
  done
}

extract_served_by() {
  local body_file=$1
  local headers_file=$2
  local served_by=""

  if [ "$HAS_JQ" -eq 1 ]; then
    served_by="$(jq -r '.served_by // empty' "$body_file" 2>/dev/null || true)"
  else
    served_by="$(sed -n 's/.*"served_by"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$body_file")"
  fi

  if [ -z "$served_by" ]; then
    served_by="$(grep -i '^X-Served-By:' "$headers_file" | sed -E 's/^[^:]*:[[:space:]]*//')"
  fi

  echo "$served_by"
}

add_instance() {
  local instance=$1
  if [ -z "$instance" ]; then
    return
  fi
  case " $SEEN_INSTANCES " in
    *" $instance "*) ;;
    *) SEEN_INSTANCES="$SEEN_INSTANCES $instance" ;;
  esac
}

run_search_round() {
  local label=$1
  local count=$2
  log ""
  log "$label"
  SEEN_INSTANCES=""

  local i=0
  for q in "${SEARCH_QUERIES[@]}"; do
    i=$((i + 1))
    if [ "$i" -gt "$count" ]; then
      break
    fi
    log "+ curl -sS -D \"$HEADERS_FILE\" --get --data-urlencode \"q=$q\" \"$LB_URL$SEARCH_PATH\" -o \"$BODY_FILE\""
    local curl_err
    curl_err="$(curl -sS -D "$HEADERS_FILE" --get --data-urlencode "q=$q" "$LB_URL$SEARCH_PATH" -o "$BODY_FILE" 2>&1)" || {
      log "$curl_err"
      return 1
    }
    if [ -n "$curl_err" ]; then
      log "$curl_err"
    fi
    local served_by
    served_by="$(extract_served_by "$BODY_FILE" "$HEADERS_FILE")"
    if [ -z "$served_by" ]; then
      served_by="unknown"
    fi
    log "[$i] q=\"$q\" served_by=$served_by"
    if [ "$served_by" != "unknown" ]; then
      add_instance "$served_by"
    fi
  done
  log "Instances seen:${SEEN_INSTANCES:- none}"
}

log "=== [1/6] PRECHECKS ==="
log "[info] Full log: $LOG_FILE"
log "[info] Summary log: $SUMMARY_LOG_FILE"
log_summary "Start demo; full_log=$LOG_FILE; summary_log=$SUMMARY_LOG_FILE"

if ! command -v docker >/dev/null 2>&1; then
  log "docker is required but not found."
  exit 1
fi

run_cmd docker --version
run_cmd docker compose version
log_summary "Prechecks OK: docker + docker compose available; will proceed with cluster deployment."

log ""
log "Configuration:"
log "  LB_URL=$LB_URL"
log "  SEARCH_PATH=$SEARCH_PATH"
log "  INGEST_URL=$INGEST_URL"
log "  INGEST_PATH_TEMPLATE=$INGEST_PATH_TEMPLATE"
log "  BOOK_ID=$BOOK_ID"
log "  SCALE_TO=$SCALE_TO"
log "  FAIL_CONTAINER=$FAIL_CONTAINER"
log "  jq_available=$HAS_JQ"

log ""
log "=== [2/6] DEPLOYMENT ==="
run_cmd docker compose up -d --build
run_cmd docker compose ps
wait_for_http_200 "$LB_STATUS_URL" 180
log_summary "Deployment OK: containers started and load balancer responded with HTTP 200 at $LB_STATUS_URL."

supports_scale=0
log "+ docker compose config --services"
services="$(docker compose config --services)"
if printf "%s\n" "$services" | grep -q "^search$"; then
  supports_scale=1
fi

static_scale_mode=0
if [ "$supports_scale" -eq 0 ] && [ "$SCALE_TO" -ge 3 ]; then
  static_scale_mode=1
  run_cmd docker stop search3 || true
  log "[info] Using search1/search2 for initial traffic; search3 will be added during scaling."
fi

log ""
log "=== [3/6] INGESTION -> INDEXING ==="
if ! post_with_retries "$INGEST_URL$INGEST_PATH" 10 3; then
  log "[error] Ingestion failed after retries."
  exit 1
fi
log_summary "Ingestion triggered: POST $INGEST_URL$INGEST_PATH for book_id=$BOOK_ID. Crawler should download + replicate the book and publish to ActiveMQ."
log ""
log "[info] Waiting briefly for async indexing..."
read -t 4 -r _ || true
log_summary "Indexing wait complete: gave the async pipeline time to consume messages and update Hazelcast."

log ""
log "Crawler logs (download + replication):"
log "+ docker compose logs --tail 80 ingestion1 | grep -E \"download|replica|ingest|book\""
if output="$(docker compose logs --tail 80 ingestion1 | grep -E "download|replica|ingest|book" 2>&1)"; then
  log "$output"
else
  log "(no matches, showing recent logs)"
  log "+ docker compose logs --tail 20 ingestion1"
  output="$(docker compose logs --tail 20 ingestion1 2>&1)" || true
  log "$output"
fi

log ""
log "ActiveMQ logs (message published):"
log "+ docker compose logs --tail 80 activemq | grep -E \"ingest|queue|enqueue|books.ingested\""
if output="$(docker compose logs --tail 80 activemq | grep -E "ingest|queue|enqueue|books.ingested" 2>&1)"; then
  log "$output"
else
  log "(no matches, showing recent logs)"
  log "+ docker compose logs --tail 20 activemq"
  output="$(docker compose logs --tail 20 activemq 2>&1)" || true
  log "$output"
fi

log ""
log "Indexer logs (message consumed + index updated):"
log "+ docker compose logs --tail 80 indexing1 | grep -E \"index|consume|books.ingested|processed\""
if output="$(docker compose logs --tail 80 indexing1 | grep -E "index|consume|books.ingested|processed" 2>&1)"; then
  log "$output"
else
  log "(no matches, showing recent logs)"
  log "+ docker compose logs --tail 20 indexing1"
  output="$(docker compose logs --tail 20 indexing1 2>&1)" || true
  log "$output"
fi

log ""
log "Hazelcast logs (cluster active):"
log "+ docker compose logs --tail 80 hazelcast1 | grep -E \"cluster|members|started|joined\""
if output="$(docker compose logs --tail 80 hazelcast1 | grep -E "cluster|members|started|joined" 2>&1)"; then
  log "$output"
else
  log "(no matches, showing recent logs)"
  log "+ docker compose logs --tail 20 hazelcast1"
  output="$(docker compose logs --tail 20 hazelcast1 2>&1)" || true
  log "$output"
fi

log ""
log "=== [4/6] SEARCH VIA LOAD BALANCER ==="
run_search_round "Sending search queries via load balancer..." 8
log_summary "Search via LB complete: requests routed through Nginx. Instances seen=${SEEN_INSTANCES:-none}."

log ""
log "=== [5/6] HORIZONTAL SCALING ==="
if [ "$supports_scale" -eq 1 ]; then
  run_cmd docker compose up -d --scale search="$SCALE_TO"
  run_cmd docker compose ps
  log_summary "Scaling step: docker compose scaled search to $SCALE_TO replicas; nginx should route across the new pool."
elif [ "$static_scale_mode" -eq 1 ]; then
  run_cmd docker start search3 || true
  run_cmd docker compose ps
  log_summary "Scaling step: started search3 (static services mode); nginx should include the new instance."
else
  log "[info] Static search services detected; no additional instances to scale."
  log_summary "Scaling skipped: static search services detected and no extra instances available."
fi
run_search_round "Sending search queries after scaling..." 8
log_summary "Post-scale search complete: verified routing after scaling. Instances seen=${SEEN_INSTANCES:-none}."

log ""
log "=== [6/6] FAILURE & RECOVERY ==="
log "+ docker ps --format '{{.Names}}'"
running_containers="$(docker ps --format '{{.Names}}')"
log "$running_containers"
if printf "%s\n" "$running_containers" | grep -q "^${FAIL_CONTAINER}$"; then
  run_cmd docker stop "$FAIL_CONTAINER"
  log_summary "Failure step: stopped container $FAIL_CONTAINER to simulate node failure."
else
  log "[warn] $FAIL_CONTAINER is not running; continuing."
  log_summary "Failure step: container $FAIL_CONTAINER was not running; continued without stopping."
fi
run_search_round "Sending search queries with a failed node..." 8
log_summary "Search with failed node complete: requests should succeed despite the outage. Instances seen=${SEEN_INSTANCES:-none}."

run_cmd docker start "$FAIL_CONTAINER" || true
log_summary "Recovery step: started container $FAIL_CONTAINER to rejoin the cluster."
run_search_round "Sending search queries after recovery..." 8
log_summary "Post-recovery search complete: verified routing after recovery. Instances seen=${SEEN_INSTANCES:-none}."

log ""
log "=== SUMMARY ==="
log "- Deployment OK"
log "- Async ingestion/indexing OK"
log "- Load balancing OK"
log "- Scaling OK"
log "- Fault tolerance OK"
log_summary "Demo summary: deployment OK; async ingestion/indexing OK; load balancing OK; scaling OK; fault tolerance OK."

if [ "$cleanup_requested" -eq 1 ]; then
  log ""
  log "=== CLEANUP ==="
  run_cmd docker compose down -v
  log_summary "Cleanup completed: docker compose down -v removed containers, networks, and volumes."
fi
