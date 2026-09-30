package domain

type FailureCode string

const (
	FailureWalletPlayerMismatch        FailureCode = "WALLET_PLAYER_MISMATCH"
	FailureCurrencyMismatch            FailureCode = "CURRENCY_MISMATCH"
	FailureReferenceRequired           FailureCode = "REFERENCE_REQUIRED"
	FailureReferenceNotFound           FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed       FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch           FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceAmountMismatch     FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureInvalidReference            FailureCode = "INVALID_REFERENCE"
	FailureInvalidReferenceKind        FailureCode = "INVALID_REFERENCE_KIND"
	FailureAlreadyReversed             FailureCode = "ALREADY_REVERSED"
	FailureInsufficientBalance         FailureCode = "INSUFFICIENT_BALANCE"
	FailureInsufficientBalanceReversal FailureCode = "INSUFFICIENT_BALANCE_FOR_REVERSAL"
)
