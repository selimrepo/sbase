#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "Docker Engine ve Docker Compose v2 gerekli." >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo ".env.example dosyasından .env oluşturuldu."
fi

COMPOSE=(docker compose -f compose.yaml)
if [[ $# -eq 0 ]]; then
  set -- up --build -d
fi

echo "SBase base stack: ${COMPOSE[*]} $*"
exec "${COMPOSE[@]}" "$@"
