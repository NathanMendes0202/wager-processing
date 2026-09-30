package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const wagerConsumerName = "wager-transaction-consumer"

type WagerMessageHandler struct {
	wagers *WagerService
	db     *pgxpool.Pool
	inbox  *messaging.InboxRepository
}

func NewWagerMessageHandler(wagers *WagerService, db *pgxpool.Pool, inbox *messaging.InboxRepository) *WagerMessageHandler {
	return &WagerMessageHandler{wagers: wagers, db: db, inbox: inbox}
}

func (h *WagerMessageHandler) Handle(ctx context.Context, msg messaging.WagerTransactionMessage) error {
	cmd, err := commandFromMessage(msg)
	if err != nil {
		return err
	}

	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	claimed, err := h.inbox.ClaimTx(ctx, tx, wagerConsumerName, msg.MessageID, msg.RawBody)
	if err != nil {
		return err
	}
	if !claimed {
		return tx.Commit(ctx)
	}

	_, err = h.wagers.ProcessTx(ctx, tx, cmd) // Removida a variável não utilizada `result`
	if err != nil {
		if isPermanent(err) {
			return fmt.Errorf("%w: %v", messaging.ErrPermanentMessage, err)
		}
		return err
	}

	if err := h.inbox.CompleteTx(ctx, tx, wagerConsumerName, msg.MessageID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func commandFromMessage(msg messaging.WagerTransactionMessage) (WagerCommand, error) {
	if msg.MessageID == "" {
		return WagerCommand{}, fmt.Errorf("%w: messageId is required", messaging.ErrInvalidMessage)
	}
	if msg.Type != messaging.RequestMessageType {
		return WagerCommand{}, fmt.Errorf("%w: unsupported message type: %s", messaging.ErrInvalidMessage, msg.Type)
	}
	if msg.Data.ProviderID == "" || msg.Data.ExternalTransactionID == "" || msg.Data.IdempotencyKey == "" {
		return WagerCommand{}, fmt.Errorf("%w: providerId, externalTransactionId and idempotencyKey are required", messaging.ErrInvalidMessage)
	}
	if !repository.ExternalKind(msg.Data.Kind) {
		return WagerCommand{}, fmt.Errorf("%w: %v", messaging.ErrInvalidMessage, fmt.Errorf("%w: %q", repository.ErrUnsupportedKind, msg.Data.Kind))
	}
	player, err := uuid.Parse(msg.Data.PlayerID)
	if err != nil {
		return WagerCommand{}, fmt.Errorf("%w: invalid playerId: %v", messaging.ErrInvalidMessage, err)
	}
	wallet, err := uuid.Parse(msg.Data.WalletID)
	if err != nil {
		return WagerCommand{}, fmt.Errorf("%w: invalid walletId: %v", messaging.ErrInvalidMessage, err)
	}
	money, err := domain.NewMoney(msg.Data.Money.Amount, msg.Data.Money.Currency)
	if err != nil {
		return WagerCommand{}, fmt.Errorf("%w: invalid money: %v", messaging.ErrInvalidMessage, err)
	}
	if err := repository.ValidateAmount(msg.Data.Kind, money); err != nil {
		return WagerCommand{}, fmt.Errorf("%w: invalid amount: %v", messaging.ErrInvalidMessage, err)
	}
	return WagerCommand{
		ProviderID:            msg.Data.ProviderID,
		ExternalTransactionID: msg.Data.ExternalTransactionID,
		IdempotencyKey:        msg.Data.IdempotencyKey,
		PlayerID:              player,
		WalletID:              wallet,
		RoundID:               msg.Data.RoundID,
		GameID:                msg.Data.GameID,
		Kind:                  msg.Data.Kind,
		Amount:                money,
		ReferenceExternalID:   msg.Data.ReferenceExternalID,
	}, nil
}

func isPermanent(err error) bool {
	return errors.Is(err, repository.ErrIdempotencyConflict) ||
		errors.Is(err, repository.ErrExternalIDConflict) ||
		errors.Is(err, repository.ErrWalletNotFound) ||
		errors.Is(err, repository.ErrUnsupportedKind) ||
		errors.Is(err, repository.ErrInvalidAmount)
}
