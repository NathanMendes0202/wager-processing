package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type MoneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

var (
	ErrInvalidMessage   = errors.New("invalid message")
	ErrPermanentMessage = errors.New("permanent message error")
	ErrPermanent        = errors.New("permanent error")
)

type WagerTransactionData struct {
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	IdempotencyKey        string    `json:"idempotencyKey"`
	PlayerID              string    `json:"playerId"`
	WalletID              string    `json:"walletId"`
	RoundID               string    `json:"roundId"`
	GameID                string    `json:"gameId"`
	Kind                  string    `json:"kind"`
	Money                 MoneyData `json:"money"`
	ReferenceExternalID   string    `json:"referenceExternalTransactionId,omitempty"`
}

type WagerTransactionMessage struct {
	MessageID  string               `json:"messageId"`
	Type       string               `json:"type"`
	OccurredAt time.Time            `json:"occurredAt"`
	Data       WagerTransactionData `json:"data"`
	RawBody    string               `json:"-"`
}

func ParseRequest(body string) (WagerTransactionMessage, error) {
	var msg WagerTransactionMessage
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		return WagerTransactionMessage{}, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	if msg.MessageID == "" {
		return WagerTransactionMessage{}, fmt.Errorf("%w: messageId is required", ErrPermanent)
	}
	if msg.Type != "WagerTransactionRequested" {
		return WagerTransactionMessage{}, fmt.Errorf("%w: invalid or unsupported message type: %s", ErrPermanent, msg.Type)
	}
	msg.RawBody = body
	return msg, nil
}
