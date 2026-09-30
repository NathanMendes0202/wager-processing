package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
)

type WalletRepository struct {
	db     *pgxpool.Pool
	outbox *messaging.OutboxRepository
}

func NewWalletRepository(db *pgxpool.Pool, outbox *messaging.OutboxRepository) *WalletRepository {
	return &WalletRepository{db: db, outbox: outbox}
}

type CreateWalletInput struct {
	PlayerID      uuid.UUID
	InitialAmount domain.Money
}

func (r *WalletRepository) Create(ctx context.Context, in CreateWalletInput) (domain.Wallet, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.Wallet{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	walletID := uuid.New()
	_, err = tx.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor, version)
		VALUES ($1,$2,$3,$4,1)
	`, walletID, in.PlayerID, in.InitialAmount.Currency(), in.InitialAmount.MinorUnits())
	if err != nil {
		return domain.Wallet{}, err
	}

	if in.InitialAmount.IsPositive() {
		txID := uuid.New()
		_, err = tx.Exec(ctx, `
			INSERT INTO wager_transactions
			(id, wallet_id, player_id, kind, amount_minor, currency, status, payload_hash)
			VALUES ($1,$2,$3,'OPENING',$4,$5,'PROCESSED','internal-opening')
		`, txID, walletID, in.PlayerID, in.InitialAmount.MinorUnits(), in.InitialAmount.Currency())
		if err != nil {
			return domain.Wallet{}, err
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO wallet_ledger_entries
			(transaction_id,wallet_id,direction,amount_minor,balance_before_minor,balance_after_minor)
			VALUES ($1,$2,'CREDIT',$3,0,$4)
		`, txID, walletID, in.InitialAmount.MinorUnits(), in.InitialAmount.MinorUnits())
		if err != nil {
			return domain.Wallet{}, err
		}

		if r.outbox != nil {
			payload, marshalErr := json.Marshal(messaging.WalletBalanceChangedData{
				WalletID:      walletID, // Removido .String() (agora passa o uuid.UUID direto)
				TransactionID: txID,     // Removido .String() (agora passa o uuid.UUID direto)
				Direction:     "CREDIT",
				Money:         messaging.MoneyData{Amount: in.InitialAmount.String(), Currency: in.InitialAmount.Currency()},
				BalanceBefore: messaging.MoneyData{Amount: "0.00", Currency: in.InitialAmount.Currency()},
				BalanceAfter:  messaging.MoneyData{Amount: in.InitialAmount.String(), Currency: in.InitialAmount.Currency()},
				WalletVersion: 1,
			})
			if marshalErr != nil {
				return domain.Wallet{}, marshalErr
			}
			if err = r.outbox.InsertTx(ctx, tx, messaging.OutboxEvent{
				ID:          uuid.New(),
				AggregateID: walletID,
				EventType:   "WalletBalanceChanged",
				Correlation: txID.String(), // Mantido .String() aqui (Correlation costuma ser string)
				Causation:   txID.String(), // Mantido .String() aqui (Causation costuma ser string)
				OccurredAt:  time.Now().UTC(),
				Version:     1,
				Payload:     payload,
			}); err != nil {
				return domain.Wallet{}, err
			}
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return domain.Wallet{}, err
	}
	return domain.RehydrateWallet(walletID, in.PlayerID, in.InitialAmount, 1)
}

func (r *WalletRepository) Ready(ctx context.Context) error { return r.db.Ping(ctx) }

func (r *WalletRepository) Get(ctx context.Context, walletID uuid.UUID) (domain.Wallet, error) {
	var playerID uuid.UUID
	var currency string
	var balance, version int64
	err := r.db.QueryRow(ctx, `
		SELECT player_id,currency,balance_minor,version
		FROM wallets WHERE id=$1
	`, walletID).Scan(&playerID, &currency, &balance, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Wallet{}, fmt.Errorf("wallet: %w", ErrNotFound)
		}
		return domain.Wallet{}, fmt.Errorf("get wallet: %w", err)
	}
	money, err := domain.NewMoneyFromMinorUnits(balance, currency)
	if err != nil {
		return domain.Wallet{}, err
	}
	return domain.RehydrateWallet(walletID, playerID, money, version)
}
