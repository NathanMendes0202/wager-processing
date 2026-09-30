#!/usr/bin/env bash
set -euo pipefail

docker compose up -d --build
trap 'docker compose down' EXIT
curl -fsS http://localhost:8080/health/live >/dev/null
curl -fsS http://localhost:8080/health/ready >/dev/null
curl -fsS http://localhost:8080/metrics >/dev/null
printf '%s\n' 'Base Compose health, readiness and metrics checks passed.'
printf '%s\n' 'Next: obtain provider-a/provider-b/wager-internal tokens from Keycloak and run the HTTP/SQS/restart scenarios documented in README.md.'
