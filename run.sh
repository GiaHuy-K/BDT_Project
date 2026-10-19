#!/usr/bin/env bash
# End-to-end pipeline: infra -> ingestion -> bronze -> silver -> gold -> PostgreSQL.
# Usage: ./run.sh            (all steps)
#        ./run.sh silver     (start from a given step: ingest|bronze|silver|gold|export)
set -euo pipefail
cd "$(dirname "$0")"
# Git Bash on Windows would rewrite container paths like /opt/spark into C:/Program Files/Git/opt/spark
export MSYS_NO_PATHCONV=1

START="${1:-infra}"
STEPS=(infra ingest bronze silver gold export)
mkdir -p logs
LOG="logs/run_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1
echo "== pipeline run $(date -Iseconds), log: $LOG"

[ -f .env ] || cp .env.example .env

submit() {
  echo "== spark-submit $1"
  docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    "/opt/pipeline/spark_jobs/$1.py"
}

run_step() {
  case "$1" in
    infra)
      docker compose up -d
      echo "== waiting for MinIO and PostgreSQL"
      until docker compose exec -T minio curl -fs http://localhost:9000/minio/health/live >/dev/null 2>&1; do sleep 2; done
      until docker compose exec -T postgres pg_isready >/dev/null 2>&1; do sleep 2; done ;;
    ingest) python ingestion/ingest.py ;;
    bronze) submit bronze ;;
    silver) submit silver ;;
    gold)   submit gold ;;
    export) submit export_postgres ;;
  esac
}

started=false
for step in "${STEPS[@]}"; do
  [ "$step" = "$START" ] && started=true
  if $started; then
    echo "== step: $step"
    run_step "$step"
  fi
done

echo "== done. Spark history: http://localhost:18080  Metabase: http://localhost:3000"
