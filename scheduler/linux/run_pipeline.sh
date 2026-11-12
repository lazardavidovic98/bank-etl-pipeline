#!/usr/bin/env bash
# Wrapper used by cron / systemd. Runs the pipeline with the project's virtual
# environment and writes one line per run to logs/scheduler.log.

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_DIR" || exit 1

PYTHON="$PROJECT_DIR/.venv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

mkdir -p logs
echo "$(date '+%F %T') START" >> logs/scheduler.log

# flock -n: if a previous run still holds the lock, exit immediately (code 3)
# instead of starting a second copy. The pipeline also has its own lock.
flock -n -E 3 logs/scheduler.flock "$PYTHON" scripts/run_pipeline.py > /dev/null 2>&1
code=$?

case $code in
    0) status="SUCCESS" ;;
    3) status="SKIPPED (already running)" ;;
    *) status="FAILED" ;;
esac
echo "$(date '+%F %T') END   exit=$code $status" >> logs/scheduler.log
exit $code
