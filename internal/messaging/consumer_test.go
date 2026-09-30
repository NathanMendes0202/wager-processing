package messaging

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

const validRequest = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

func TestParseRequestAcceptsChallengeEnvelope(t *testing.T) {
	msg, err := ParseRequest(validRequest)
	if err != nil {
		t.Fatal(err)
	}
	if msg.MessageID != "msg-123" || msg.Data.Money.Amount != "25.00" || msg.Data.Money.Currency != "BRL" {
		t.Fatalf("unexpected message %+v", msg)
	}
}

func TestParseRequestRejectionsArePermanent(t *testing.T) {
	for name, body := range map[string]string{
		"not json":     `{`,
		"wrong type":   `{"messageId":"m","type":"Other","data":{}}`,
		"no type":      `{"messageId":"m","data":{}}`,
		"no messageId": `{"type":"WagerTransactionRequested","data":{}}`,
	} {
		if _, err := ParseRequest(body); !errors.Is(err, ErrPermanent) {
			t.Fatalf("%s: expected permanent error, got %v", name, err)
		}
	}
}

func TestRetryDelayBackoffIsCapped(t *testing.T) {
	want := map[int]int32{0: 5, 1: 5, 2: 10, 3: 20, 6: 160, 7: 300, 50: 300}
	for count, delay := range want {
		if got := retryDelaySeconds(count); got != delay {
			t.Fatalf("retryDelaySeconds(%d)=%d want %d", count, got, delay)
		}
	}
}

func TestEventConstructorsOwnTypeAndVersion(t *testing.T) {
	tx, wallet := uuid.New(), uuid.New()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.FixedZone("x", 3*3600))
	money := MoneyPayload{Amount: "25.00", Currency: "BRL"}

	processed, err := NewWagerTransactionProcessed("corr", at, WagerTransactionProcessedData{TransactionID: tx, WalletID: wallet, Kind: "LOSS", Money: money, Balance: money, WalletVersion: 3})
	if err != nil {
		t.Fatal(err)
	}
	changed, err := NewWalletBalanceChanged("corr", at, WalletBalanceChangedData{WalletID: wallet, TransactionID: tx, Direction: "DEBIT", Money: money, BalanceBefore: money, BalanceAfter: money, WalletVersion: 3})
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := NewWagerTransactionRejected("corr", at, WagerTransactionRejectedData{TransactionID: tx, WalletID: wallet, FailureCode: "INSUFFICIENT_BALANCE"})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := NewWagerTransactionPendingReference("corr", at, WagerTransactionPendingReferenceData{TransactionID: tx, WalletID: wallet})
	if err != nil {
		t.Fatal(err)
	}

	types := map[string]string{
		processed.EventType: EventWagerTransactionProcessed,
		changed.EventType:   EventWalletBalanceChanged,
		rejected.EventType:  EventWagerTransactionRejected,
		pending.EventType:   EventWagerTransactionPendingReference,
	}
	for got, want := range types {
		if got != want {
			t.Fatalf("event type %q, want %q", got, want)
		}
	}
	for _, e := range []OutboxEvent{processed, changed, rejected, pending} {
		if e.Version != EventSchemaVersion {
			t.Fatalf("%s: version %d", e.EventType, e.Version)
		}
		if e.OccurredAt.Location() != time.UTC {
			t.Fatalf("%s: occurredAt must be UTC", e.EventType)
		}
		if e.Causation != tx.String() {
			t.Fatalf("%s: causation must be the transaction", e.EventType)
		}
	}
	if processed.AggregateID != tx || changed.AggregateID != wallet {
		t.Fatal("transaction events are keyed by transaction, balance events by wallet")
	}
	var data map[string]any
	if err := json.Unmarshal(changed.Payload, &data); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[key]; !ok {
			t.Fatalf("WalletBalanceChanged payload lacks %q", key)
		}
	}
}
