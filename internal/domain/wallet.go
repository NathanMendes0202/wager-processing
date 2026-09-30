package domain

import (
	"errors"
	"github.com/google/uuid"
)

var (
	ErrInsufficientBalance    = errors.New("insufficient wallet balance")
	ErrWalletCurrencyMismatch = errors.New("wallet currency mismatch")
)

type Wallet struct {
	ID       uuid.UUID
	PlayerID uuid.UUID
	Currency string
	Balance  Money
	Version  int64
}

func RehydrateWallet(id, playerID uuid.UUID, balance Money, version int64) (Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil || version < 1 {
		return Wallet{}, errors.New("invalid wallet")
	}
	return Wallet{ID: id, PlayerID: playerID, Currency: balance.Currency(), Balance: balance, Version: version}, nil
}

func (w Wallet) Debit(amount Money) (Money, error) {
	if amount.Currency() != w.Currency {
		return Money{}, ErrWalletCurrencyMismatch
	}
	if !amount.IsPositive() {
		return Money{}, ErrInvalidMoney
	}
	cmp, err := w.Balance.Compare(amount)
	if err != nil {
		return Money{}, err
	}
	if cmp < 0 {
		return Money{}, ErrInsufficientBalance
	}
	return w.Balance.Sub(amount)
}
