package domain

import (
	"github.com/google/uuid"
	"testing"
)

func TestLedgerEntryValidatesBalanceInvariant(t *testing.T) {
	before, _ := NewMoney("100.00", "BRL")
	amount, _ := NewMoney("20.00", "BRL")
	after, _ := NewMoney("80.00", "BRL")
	if _, err := NewLedgerEntry(uuid.New(), uuid.New(), uuid.New(), LedgerDebit, amount, before, after); err != nil {
		t.Fatalf("expected valid debit: %v", err)
	}
	wrong, _ := NewMoney("90.00", "BRL")
	if _, err := NewLedgerEntry(uuid.New(), uuid.New(), uuid.New(), LedgerDebit, amount, before, wrong); err == nil {
		t.Fatal("expected invalid debit balance")
	}
}
