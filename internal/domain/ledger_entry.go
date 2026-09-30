package domain

import (
	"errors"
	"github.com/google/uuid"
)

type LedgerDirection string

const (
	LedgerDebit  LedgerDirection = "DEBIT"
	LedgerCredit LedgerDirection = "CREDIT"
)

type LedgerEntry struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     LedgerDirection
	Amount        Money
	BalanceBefore Money
	BalanceAfter  Money
}

func NewLedgerEntry(id, walletID, transactionID uuid.UUID, direction LedgerDirection, amount, before, after Money) (LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return LedgerEntry{}, errors.New("invalid ledger identifiers")
	}
	if direction != LedgerDebit && direction != LedgerCredit {
		return LedgerEntry{}, errors.New("invalid ledger direction")
	}
	if !amount.IsPositive() || amount.Currency() != before.Currency() || amount.Currency() != after.Currency() {
		return LedgerEntry{}, errors.New("invalid ledger money")
	}
	if direction == LedgerDebit {
		expected, err := before.Sub(amount)
		if err != nil || expected.MinorUnits() != after.MinorUnits() {
			return LedgerEntry{}, errors.New("invalid debit ledger balance")
		}
	} else {
		expected, err := before.Add(amount)
		if err != nil || expected.MinorUnits() != after.MinorUnits() {
			return LedgerEntry{}, errors.New("invalid credit ledger balance")
		}
	}
	return LedgerEntry{ID: id, WalletID: walletID, TransactionID: transactionID, Direction: direction, Amount: amount, BalanceBefore: before, BalanceAfter: after}, nil
}
