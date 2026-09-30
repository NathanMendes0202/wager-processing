package messaging

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWagerTransactionMessageContract(t *testing.T) {
	msg := WagerTransactionMessage{MessageID: "m1", Type: "WagerTransactionRequested", OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Data: WagerTransactionData{ProviderID: "provider-a", ExternalTransactionID: "ext", IdempotencyKey: "idem", PlayerID: "p", WalletID: "w", Kind: "BET", Money: MoneyData{Amount: "80.00", Currency: "BRL"}}}
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"messageId", "type", "occurredAt", "data"} {
		if _, ok := raw[k]; !ok {
			t.Fatalf("missing top-level field %q: %s", k, b)
		}
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(raw["data"], &data); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"providerId", "kind", "money"} {
		if _, ok := data[k]; !ok {
			t.Fatalf("missing data field %q: %s", k, b)
		}
	}
	var money map[string]json.RawMessage
	if err := json.Unmarshal(data["money"], &money); err != nil {
		t.Fatal(err)
	}
	if _, ok := money["amount"]; !ok {
		t.Fatal("money.amount missing")
	}
	if _, ok := money["currency"]; !ok {
		t.Fatal("money.currency missing")
	}
}

func TestOutboxEnvelopeContract(t *testing.T) {
	env := OutboxEventEnvelope{EventID: "e", EventType: "WagerTransactionProcessed", AggregateID: "a", Version: 1, OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"status":"PROCESSED"}`)}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "occurredAt", "version", "data"} {
		if _, ok := raw[k]; !ok {
			t.Fatalf("missing envelope field %q", k)
		}
	}
	if _, ok := raw["payload"]; ok {
		t.Fatal("legacy payload field must not be emitted")
	}
}
