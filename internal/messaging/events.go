package messaging

import (
	"github.com/google/uuid"
)

const RequestMessageType = "WagerTransactionRequested"

// Event payloads explicitamente adaptados para aceitar uuid.UUID diretamente
type WagerTransactionProcessedData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	WalletID                       uuid.UUID `json:"walletId"`
	PlayerID                       uuid.UUID `json:"playerId"`
	Kind                           string    `json:"kind"`
	Money                          MoneyData `json:"money"`
	Status                         string    `json:"status"`
	Balance                        MoneyData `json:"balance"`
	WalletVersion                  int64     `json:"walletVersion"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

type WagerTransactionRejectedData struct {
	TransactionID         uuid.UUID `json:"transactionId"`
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	WalletID              uuid.UUID `json:"walletId"`
	PlayerID              uuid.UUID `json:"playerId"`
	Kind                  string    `json:"kind"`
	FailureCode           string    `json:"failureCode"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	Attempt                        int       `json:"attempt"`
	WalletID                       uuid.UUID `json:"walletId"`
}

type WalletBalanceChangedData struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         MoneyData `json:"money"`
	BalanceBefore MoneyData `json:"balanceBefore"`
	BalanceAfter  MoneyData `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}
