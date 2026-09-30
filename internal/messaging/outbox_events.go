package messaging

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	EventSchemaVersion                    = 1
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

type MoneyPayload = MoneyData

func retryDelaySeconds(attempt int) int32 {
	if attempt < 0 {
		attempt = 0
	}
	seconds := int64(5)
	for i := 0; i < attempt; i++ {
		if i >= 1 {
			seconds *= 2
		}
		if seconds >= 300 {
			return 300
		}
	}
	if seconds < 5 {
		return 5
	}
	if seconds > 300 {
		return 300
	}
	return int32(seconds)
}

func NewWagerTransactionProcessed(correlation string, occurredAt time.Time, data WagerTransactionProcessedData) (OutboxEvent, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{
		ID:          uuid.New(),
		EventType:   EventWagerTransactionProcessed,
		AggregateID: data.TransactionID,
		Version:     EventSchemaVersion,
		OccurredAt:  occurredAt.UTC(),
		Correlation: correlation,
		Causation:   data.TransactionID.String(),
		Payload:     payload,
	}, nil
}

func NewWalletBalanceChanged(correlation string, occurredAt time.Time, data WalletBalanceChangedData) (OutboxEvent, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{
		ID:          uuid.New(),
		EventType:   EventWalletBalanceChanged,
		AggregateID: data.WalletID,
		Version:     EventSchemaVersion,
		OccurredAt:  occurredAt.UTC(),
		Correlation: correlation,
		Causation:   data.TransactionID.String(),
		Payload:     payload,
	}, nil
}

func NewWagerTransactionRejected(correlation string, occurredAt time.Time, data WagerTransactionRejectedData) (OutboxEvent, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{
		ID:          uuid.New(),
		EventType:   EventWagerTransactionRejected,
		AggregateID: data.TransactionID,
		Version:     EventSchemaVersion,
		OccurredAt:  occurredAt.UTC(),
		Correlation: correlation,
		Causation:   data.TransactionID.String(),
		Payload:     payload,
	}, nil
}

func NewWagerTransactionPendingReference(correlation string, occurredAt time.Time, data WagerTransactionPendingReferenceData) (OutboxEvent, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{
		ID:          uuid.New(),
		EventType:   EventWagerTransactionPendingReference,
		AggregateID: data.TransactionID,
		Version:     EventSchemaVersion,
		OccurredAt:  occurredAt.UTC(),
		Correlation: correlation,
		Causation:   data.TransactionID.String(),
		Payload:     payload,
	}, nil
}
