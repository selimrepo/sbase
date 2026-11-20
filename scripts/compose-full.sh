#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI bulunamadı. Docker Engine ve Compose v2 kurun." >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 bulunamadı (docker compose)." >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  if [[ -f .env.example ]]; then
    cp .env.example .env
    echo ".env.example dosyasından .env oluşturuldu. Gerekirse .env değerlerini düzenleyin."
  else
    echo ".env.example bulunamadı." >&2
    exit 1
  fi
fi

COMPOSE=(docker compose -f compose.yaml -f compose.full.yaml --profile full)
if [[ $# -eq 0 ]]; then
  set -- up --build -d
fi

echo "SBase full stack: ${COMPOSE[*]} $*"
exec "${COMPOSE[@]}" "$@"
