package domain

import "testing"

func TestTransactionStateMachine(t *testing.T) {
	valid := [][2]TransactionStatus{
		{TransactionPending, TransactionProcessed},
		{TransactionPending, TransactionRejected},
		{TransactionPending, TransactionPendingReference},
		{TransactionPendingReference, TransactionProcessed},
		{TransactionPendingReference, TransactionRejected},
		{TransactionPendingReference, TransactionPendingReference},
	}
	for _, pair := range valid {
		if err := ValidateTransactionTransition(pair[0], pair[1]); err != nil {
			t.Fatalf("expected %s -> %s to be valid: %v", pair[0], pair[1], err)
		}
	}
	invalid := [][2]TransactionStatus{
		{TransactionProcessed, TransactionPending},
		{TransactionRejected, TransactionProcessed},
		{TransactionFailed, TransactionProcessed},
	}
	for _, pair := range invalid {
		if err := ValidateTransactionTransition(pair[0], pair[1]); err == nil {
			t.Fatalf("expected %s -> %s to be invalid", pair[0], pair[1])
		}
	}
}
