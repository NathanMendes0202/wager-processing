#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:8080"
KC="http://localhost:8081/realms/wager/protocol/openid-connect/token"

token() {
  curl -fsS -X POST "$KC" -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode client_id="$1" --data-urlencode client_secret="$2" |
    python -c 'import json,sys; print(json.load(sys.stdin)["access_token"])'
}
A="$(token provider-a provider-a-secret)"
I="$(token wager-internal internal-secret)"
PLAYER=$(python -c 'import uuid; print(uuid.uuid4())')
WALLET=$(curl -fsS -X POST "$API/wallets" -H "Authorization: Bearer $I" -H 'Content-Type: application/json' -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')
EXT="restart-$(date +%s)"
IDEM="restart-idem-$(date +%s)"
BODY="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$EXT\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"restart-round\",\"gameId\":\"restart-game\",\"kind\":\"BET\",\"money\":{\"amount\":\"10.00\",\"currency\":\"BRL\"}}"
FIRST=$(curl -fsS -X POST "$API/wagering/transactions" -H "Authorization: Bearer $A" -H "Idempotency-Key: $IDEM" -H 'Content-Type: application/json' -d "$BODY")
TX=$(python -c 'import json,sys; print(json.load(sys.stdin)["transactionId"])' <<<"$FIRST")

docker compose restart api
for _ in $(seq 1 30); do curl -fsS "$API/health/ready" >/dev/null && break; sleep 1; done
curl -fsS "$API/health/ready" >/dev/null

REPLAY=$(curl -fsS -X POST "$API/wagering/transactions" -H "Authorization: Bearer $A" -H "Idempotency-Key: $IDEM" -H 'Content-Type: application/json' -d "$BODY")
python -c 'import json,sys; x=json.load(sys.stdin); assert x["transactionId"] == sys.argv[1]; assert x["idempotentReplay"] is True' "$TX" <<<"$REPLAY"
BALANCE=$(curl -fsS -H "Authorization: Bearer $I" "$API/wallets/$WALLET" | python -c 'import json,sys; print(json.load(sys.stdin)["balance"]["amount"])')
test "$BALANCE" = "90.00"
LEDGER=$(curl -fsS -H "Authorization: Bearer $I" "$API/wallets/$WALLET/ledger")
COUNT=$(python -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))' <<<"$LEDGER")
test "$COUNT" = "2"

printf '%s\n' 'Restart persistence, idempotency replay, balance and ledger checks passed.'
