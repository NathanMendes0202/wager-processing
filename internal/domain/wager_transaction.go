package domain

import "errors"

type TransactionKind string

const (
	TransactionOpening  TransactionKind = "OPENING"
	TransactionBet      TransactionKind = "BET"
	TransactionWin      TransactionKind = "WIN"
	TransactionLoss     TransactionKind = "LOSS"
	TransactionRefund   TransactionKind = "REFUND"
	TransactionRollback TransactionKind = "ROLLBACK"
)

type TransactionStatus string

const (
	TransactionPending          TransactionStatus = "PENDING"
	TransactionPendingReference TransactionStatus = "PENDING_REFERENCE"
	TransactionProcessed        TransactionStatus = "PROCESSED"
	TransactionRejected         TransactionStatus = "REJECTED"
	TransactionFailed           TransactionStatus = "FAILED"
)

var ErrInvalidTransactionTransition = errors.New("invalid wager transaction state transition")

// IsExternal reports whether the kind may be submitted by a provider.
func (k TransactionKind) IsExternal() bool {
	switch k {
	case TransactionBet, TransactionWin, TransactionLoss, TransactionRefund, TransactionRollback:
		return true
	default:
		return false
	}
}

func (s TransactionStatus) CanTransitionTo(next TransactionStatus) bool {
	switch s {
	case TransactionPending:
		return next == TransactionPendingReference || next == TransactionProcessed || next == TransactionRejected
	case TransactionPendingReference:
		return next == TransactionPendingReference || next == TransactionProcessed || next == TransactionRejected
	case TransactionProcessed, TransactionRejected, TransactionFailed:
		return false
	default:
		return false
	}
}

func ValidateTransactionTransition(current, next TransactionStatus) error {
	if !current.CanTransitionTo(next) {
		return ErrInvalidTransactionTransition
	}
	return nil
}
