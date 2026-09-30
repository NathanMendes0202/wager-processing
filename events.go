package messaging

type WagerTransactionProcessedData struct {
	TransactionID                  string    `json:"transactionId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	WalletID                       string    `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	Kind                           string    `json:"kind"`
	Money                          MoneyData `json:"money"`
	Status                         string    `json:"status"`
	Balance                        MoneyData `json:"balance"`
	WalletVersion                  int64     `json:"walletVersion"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

type WagerTransactionRejectedData struct {
	TransactionID         string `json:"transactionId"`
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	WalletID              string `json:"walletId"`
	PlayerID              string `json:"playerId"`
	Kind                  string `json:"kind"`
	FailureCode           string `json:"failureCode"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  string `json:"transactionId"`
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	Attempt                        int    `json:"attempt"`
}

type WalletBalanceChangedData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         MoneyData `json:"money"`
	BalanceBefore MoneyData `json:"balanceBefore"`
	BalanceAfter  MoneyData `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}
