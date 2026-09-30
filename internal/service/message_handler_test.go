package service

import (
	"errors"
	"testing"

	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
)

func validMessage() messaging.WagerTransactionMessage {
	return messaging.WagerTransactionMessage{MessageID: "m1", Type: messaging.RequestMessageType, Data: messaging.WagerTransactionData{
		ProviderID: "provider-a", ExternalTransactionID: "ext-1", IdempotencyKey: "provider-a:ext-1",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID: "r", GameID: "g", Kind: "BET", Money: messaging.MoneyData{Amount: "25.00", Currency: "BRL"},
	}}
}

func TestCommandFromMessageAcceptsValid(t *testing.T) {
	if _, err := commandFromMessage(validMessage()); err != nil {
		t.Fatal(err)
	}
	loss := validMessage()
	loss.Data.Kind, loss.Data.Money.Amount = "LOSS", "0.00"
	if _, err := commandFromMessage(loss); err != nil {
		t.Fatalf("LOSS 0.00 must be accepted: %v", err)
	}
}

func TestCommandFromMessageRejectsInvalid(t *testing.T) {
	cases := map[string]func(*messaging.WagerTransactionMessage){
		"opening over sqs": func(m *messaging.WagerTransactionMessage) { m.Data.Kind = "OPENING" },
		"loss with value":  func(m *messaging.WagerTransactionMessage) { m.Data.Kind, m.Data.Money.Amount = "LOSS", "1.00" },
		"zero bet":         func(m *messaging.WagerTransactionMessage) { m.Data.Money.Amount = "0.00" },
		"negative bet":     func(m *messaging.WagerTransactionMessage) { m.Data.Money.Amount = "-1.00" },
		"scientific":       func(m *messaging.WagerTransactionMessage) { m.Data.Money.Amount = "1e2" },
		"bad wallet id":    func(m *messaging.WagerTransactionMessage) { m.Data.WalletID = "nope" },
		"missing provider": func(m *messaging.WagerTransactionMessage) { m.Data.ProviderID = "" },
	}
	for name, mutate := range cases {
		m := validMessage()
		mutate(&m)
		if _, err := commandFromMessage(m); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func TestPermanentClassification(t *testing.T) {
	for _, err := range []error{repository.ErrUnsupportedKind, repository.ErrInvalidAmount, repository.ErrIdempotencyConflict, repository.ErrExternalIDConflict, repository.ErrWalletNotFound} {
		if !isPermanent(err) {
			t.Fatalf("%v should be permanent", err)
		}
	}
	if isPermanent(errors.New("connection refused")) {
		t.Fatal("infrastructure errors must stay retryable")
	}
}
