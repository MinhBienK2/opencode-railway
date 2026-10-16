#!/usr/bin/env bash
# So sánh 3 chế độ egress bằng cùng 1 task multi-turn chạy qua Claude Code thật:
#   direct : chỉ IP direct (DISABLE_TOR=true)
#   tor    : chỉ Tor (FORCE_TOR=true)
#   hybrid : direct trước, 429 thì sang Tor (mặc định của gateway)
#
# Yêu cầu: docker, image đã build (docker build -t orw-new .), launcher claude-orw (xem README mục "Test với Claude Code").
# LƯU Ý: chế độ direct/hybrid dùng IP của máy này. Hạn mức free-tier tính theo IP nên đừng chạy khi IP đang được
# service khác dùng cho việc quan trọng. Mỗi lượt chỉ tốn vài chục request.
#
# Cách dùng: scripts/bench_claude.sh [model=deepseek] [số lượt=3] [chế độ="direct tor hybrid"]
set -u
MODEL=${1:-deepseek}
RUNS=${2:-3}
MODES=${3:-"direct tor hybrid"}
IMAGE=${IMAGE:-orw-new}
CLAUDE=${CLAUDE_BIN:-claude-orw}
BASE_PORT=${BASE_PORT:-18150}
WORK=$(mktemp -d)
trap 'docker rm -f $(docker ps -aq --filter name=orw-bench) >/dev/null 2>&1; rm -rf "$WORK"' EXIT

PROMPT='In the current directory: create stats.py with mean(xs), median(xs), mode(xs) (ValueError on empty list); create test_stats.py with plain asserts covering normal and empty cases; run it with python3 and fix any failure; reply with one line saying whether tests pass.'

envs_for() {
  case "$1" in
    direct) echo "-e DISABLE_TOR=true" ;;
    tor)    echo "-e FORCE_TOR=true -e TOR_WARM=2 -e TOR_MAX=4" ;;
    hybrid) echo "-e TOR_WARM=1 -e TOR_MAX=4" ;;
  esac
}

printf "%-8s %-4s %-8s %-6s %-8s %s\n" MODE RUN RESULT TURNS SECONDS EGRESS
port=$BASE_PORT
for mode in $MODES; do
  port=$((port + 1)); name="orw-bench-$mode"
  # shellcheck disable=SC2046
  docker run -d --name "$name" -p "$port:8080" $(envs_for "$mode") -e DEFAULT_MODEL="$MODEL" "$IMAGE" >/dev/null || exit 1
  [ "$mode" != direct ] && sleep 30 || sleep 3
  for run in $(seq 1 "$RUNS"); do
    dir="$WORK/$mode-$run"; mkdir -p "$dir"; cd "$dir" || exit 1
    start=$(date +%s)
    out=$(ORW_PORT=$port timeout 600 "$CLAUDE" -p "$PROMPT" --output-format json </dev/null 2>/dev/null)
    secs=$(( $(date +%s) - start ))
    res=$(printf '%s' "$out" | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin); print("ok" if not d.get("is_error") else "error", d.get("num_turns",0))
except Exception: print("fail 0")')
    set -- $res
    egress=$(curl -s "localhost:$port/status" | python3 -c 'import sys,json
d=json.load(sys.stdin); print(",".join("%s:%d/%d(429=%d)"%(e["name"],e["ok"],e["requests"],e["rate_limited"]) for e in d["egress"]))')
    printf "%-8s %-4s %-8s %-6s %-8s %s\n" "$mode" "$run" "$1" "$2" "$secs" "$egress"
  done
  echo "  [$mode] ttfb/total mỗi request (từ log gateway):"
  docker logs "$name" 2>&1 | grep -E '\[req\]' | sed -E 's/.*egress=([^ ]+) ttfb=([^ ]+) first=([^ ]+) total=([^ ]+).*cached=([0-9]+).*/    \1 ttfb=\2 first=\3 total=\4 cached=\5/' | head -40
  docker rm -f "$name" >/dev/null 2>&1
done
