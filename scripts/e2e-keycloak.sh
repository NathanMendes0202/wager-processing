#!/usr/bin/env bash
set -euo pipefail

KC="http://localhost:8081/realms/wager/protocol/openid-connect/token"
ADMIN="http://localhost:8081/admin/realms/wager"
ADMIN_TOKEN_URL="http://localhost:8081/realms/master/protocol/openid-connect/token"
API="http://localhost:8080"

TOKEN_JSON=''

token() {
  curl -fsS -X POST "$KC" -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode client_id="$1" --data-urlencode client_secret="$2"
}

A_JSON="$(token provider-a provider-a-secret)"
A="$(python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])' <<<"$A_JSON")"
B="$(token provider-b provider-b-secret | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
I="$(token wager-internal internal-secret | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"

# Missing/invalid bearer must be rejected while liveness remains public.
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/wagering/transactions")
test "$code" = 401
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Authorization: Bearer invalid-token' "$API/wagering/transactions")
test "$code" = 401
curl -fsS "$API/health/live" >/dev/null

PLAYER=$(python -c 'import uuid; print(uuid.uuid4())')
WALLET_JSON=$(curl -fsS -X POST "$API/wallets" -H "Authorization: Bearer $I" -H 'Content-Type: application/json' -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
WALLET=$(python -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$WALLET_JSON")
EXT="e2e-$(date +%s)-http"
IDEM="e2e-$(date +%s)-same"
BODY="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$EXT\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"r\",\"gameId\":\"g\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}"

# HTTP path must commit the financial operation.
HTTP_RESULT=$(curl -fsS -X POST "$API/wagering/transactions" -H "Authorization: Bearer $A" -H "Idempotency-Key: $IDEM" -H 'Content-Type: application/json' -d "$BODY")
TX=$(python -c 'import json,sys; print(json.load(sys.stdin)["transactionId"])' <<<"$HTTP_RESULT")

# Same operation through SQS/async must resolve to the persisted operation, not debit again.
curl -fsS -X POST "$API/wagering/transactions/async" -H "Authorization: Bearer $A" -H "Idempotency-Key: $IDEM" -H 'Content-Type: application/json' -d "$BODY" >/dev/null
for _ in $(seq 1 20); do
  TX_JSON=$(curl -fsS -H "Authorization: Bearer $A" "$API/wagering/transactions/$TX")
  STATUS=$(python -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$TX_JSON")
  test "$STATUS" = "PROCESSED" && break
  sleep 1
done
test "$STATUS" = "PROCESSED"
BALANCE=$(curl -fsS -H "Authorization: Bearer $I" "$API/wallets/$WALLET" | python -c 'import json,sys; print(json.load(sys.stdin)["balance"]["amount"])')
test "$BALANCE" = "90.00"

# Reverse direction: first accept through SQS, then submit the same operation
# through HTTP. The second path must be an idempotent replay, not a second debit.
PLAYER2=$(python -c 'import uuid; print(uuid.uuid4())')
WALLET_JSON2=$(curl -fsS -X POST "$API/wallets" -H "Authorization: Bearer $I" -H 'Content-Type: application/json' -d "{\"playerId\":\"$PLAYER2\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
WALLET2=$(python -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$WALLET_JSON2")
EXT2="e2e-$(date +%s)-sqs-first"
IDEM2="e2e-$(date +%s)-cross-path"
BODY2="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$EXT2\",\"playerId\":\"$PLAYER2\",\"walletId\":\"$WALLET2\",\"roundId\":\"r2\",\"gameId\":\"g2\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}"
curl -fsS -X POST "$API/wagering/transactions/async" -H "Authorization: Bearer $A" -H "Idempotency-Key: $IDEM2" -H 'Content-Type: application/json' -d "$BODY2" >/dev/null
for _ in $(seq 1 20); do
  TX2_JSON=$(curl -s -H "Authorization: Bearer $A" "$API/providers/provider-a/wagering/transactions/$EXT2" || true)
  STATUS2=$(python -c 'import json,sys; print(json.load(sys.stdin).get("status", ""))' <<<"$TX2_JSON" 2>/dev/null || true)
  test "$STATUS2" = "PROCESSED" && break
  sleep 1
done
test "$STATUS2" = "PROCESSED"
curl -fsS -X POST "$API/wagering/transactions" -H "Authorization: Bearer $A" -H "Idempotency-Key: $IDEM2" -H 'Content-Type: application/json' -d "$BODY2" >/dev/null
BALANCE2=$(curl -fsS -H "Authorization: Bearer $I" "$API/wallets/$WALLET2" | python -c 'import json,sys; print(json.load(sys.stdin)["balance"]["amount"])')
test "$BALANCE2" = "90.00"

# Invalid envelope (missing envelope messageId) must be moved to request DLQ
# without creating any financial transaction.
INVALID_BODY='{"type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{}}'
docker compose exec -T localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-body "$INVALID_BODY" --message-group-id e2e-invalid \
  --message-deduplication-id "invalid-$(date +%s%N)" >/dev/null
DLQ_COUNT=0
for _ in $(seq 1 20); do
  DLQ_COUNT=$(docker compose exec -T localstack awslocal sqs get-queue-attributes \
    --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo \
    --attribute-names ApproximateNumberOfMessages | python -c 'import json,sys; print(json.load(sys.stdin)["Attributes"].get("ApproximateNumberOfMessages", "0"))')
  test "$DLQ_COUNT" -gt 0 && break
  sleep 1
done
test "$DLQ_COUNT" -gt 0
curl -fsS "$API/metrics" | grep -Eq '^wager_request_dlq_total [1-9][0-9]*$'

# Provider isolation: provider-b cannot read provider-a's transaction.
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $B" "$API/providers/provider-a/wagering/transactions/$EXT")
test "$code" = 403

# Explicit expired-token verification: temporarily shorten realm access-token lifespan.
ADMIN_TOKEN=$(curl -fsS -X POST "$ADMIN_TOKEN_URL" -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode grant_type=password --data-urlencode client_id=admin-cli \
  --data-urlencode username=admin --data-urlencode password=admin | \
  python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
ORIGINAL_REALM=$(curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" "$ADMIN")
restore_realm() {
  printf '%s' "$ORIGINAL_REALM" | curl -fsS -X PUT "$ADMIN" -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' --data-binary @- >/dev/null || true
}
trap restore_realm EXIT
SHORT_REALM=$(python -c 'import json,sys; x=json.load(sys.stdin); x["accessTokenLifespan"]=2; print(json.dumps(x))' <<<"$ORIGINAL_REALM")
printf '%s' "$SHORT_REALM" | curl -fsS -X PUT "$ADMIN" -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' --data-binary @- >/dev/null
EXPIRED="$(token provider-a provider-a-secret | python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
sleep 3
code=$(curl -s -o /dev/null -w '%{http_code}' -X GET -H "Authorization: Bearer $EXPIRED" "$API/wagering/transactions/$TX")
test "$code" = 401
restore_realm
trap - EXIT

curl -fsS "$API/metrics" >/dev/null
printf '%s\n' 'Keycloak authentication, expired-token rejection, provider isolation, HTTP↔SQS idempotency, invalid-message DLQ and metrics checks passed.'
