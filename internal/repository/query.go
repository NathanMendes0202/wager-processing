package repository

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
)

type WagerDetails struct {
	ID                    uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	WalletID              uuid.UUID
	PlayerID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  string
	Status                string
	Money                 domain.Money
	ReferenceExternalID   string
	ReferenceTransaction  *uuid.UUID
	FailureCode           string
	ResultBalance         domain.Money
	ResultWalletVersion   int64
	CreatedAt             string
	UpdatedAt             string
}

type LedgerEntry struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	Direction     string
	Money         domain.Money
	BalanceBefore domain.Money
	BalanceAfter  domain.Money
	CreatedAt     time.Time
}

type Reconciliation struct {
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	Difference        domain.Money
	Consistent        bool
	CheckedEntries    int
}

var ErrNotFound = errors.New("not found")

func getMoney(minor int64, currency string) (domain.Money, error) {
	return domain.NewMoneyFromMinorUnits(minor, strings.TrimSpace(currency))
}

func (r *WagerRepository) GetByID(ctx context.Context, id uuid.UUID) (WagerDetails, error) {
	return scanWager(r.db.QueryRow(ctx, `SELECT id,COALESCE(provider_id,''),COALESCE(external_transaction_id,''),wallet_id,player_id,COALESCE(round_id,''),COALESCE(game_id,''),kind,status,amount_minor,currency,COALESCE(reference_external_transaction_id,''),reference_transaction_id,COALESCE(failure_code,''),COALESCE(result_balance_minor,0),COALESCE(result_wallet_version,0),created_at::text,updated_at::text FROM wager_transactions WHERE id=$1`, id))
}

func (r *WagerRepository) GetByProviderExternal(ctx context.Context, providerID, externalID string) (WagerDetails, error) {
	return scanWager(r.db.QueryRow(ctx, `SELECT id,COALESCE(provider_id,''),COALESCE(external_transaction_id,''),wallet_id,player_id,COALESCE(round_id,''),COALESCE(game_id,''),kind,status,amount_minor,currency,COALESCE(reference_external_transaction_id,''),reference_transaction_id,COALESCE(failure_code,''),COALESCE(result_balance_minor,0),COALESCE(result_wallet_version,0),created_at::text,updated_at::text FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, providerID, externalID))
}

func scanWager(row pgx.Row) (WagerDetails, error) {
	var out WagerDetails
	var amount, resultBalance int64
	var currency, resultCurrency string
	err := row.Scan(&out.ID, &out.ProviderID, &out.ExternalTransactionID, &out.WalletID, &out.PlayerID, &out.RoundID, &out.GameID, &out.Kind, &out.Status, &amount, &currency, &out.ReferenceExternalID, &out.ReferenceTransaction, &out.FailureCode, &resultBalance, &out.ResultWalletVersion, &out.CreatedAt, &out.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, fmt.Errorf("wager: %w", ErrNotFound)
	}
	if err != nil {
		return out, err
	}
	out.Money, err = getMoney(amount, currency)
	if err != nil {
		return out, err
	}
	resultCurrency = currency
	out.ResultBalance, err = getMoney(resultBalance, resultCurrency)
	return out, err
}

func (r *WalletRepository) ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) ([]LedgerEntry, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	args := []any{walletID}
	query := `SELECT id,transaction_id,direction,amount_minor,balance_before_minor,balance_after_minor,currency,created_at FROM wallet_ledger_entries WHERE wallet_id=$1`
	if cursor != "" {
		cursorTime, cursorID, err := decodeLedgerCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		query += ` AND (created_at,id) < ($2,$3)`
		args = append(args, cursorTime, cursorID)
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT %d`, limit+1)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	entries := make([]LedgerEntry, 0, limit)
	for rows.Next() {
		var e LedgerEntry
		var amount, before, after int64
		var currency string
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.Direction, &amount, &before, &after, &currency, &e.CreatedAt); err != nil {
			return nil, "", err
		}
		e.Money, err = getMoney(amount, currency)
		if err != nil {
			return nil, "", err
		}
		e.BalanceBefore, err = getMoney(before, currency)
		if err != nil {
			return nil, "", err
		}
		e.BalanceAfter, err = getMoney(after, currency)
		if err != nil {
			return nil, "", err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(entries) > limit {
		next = encodeLedgerCursor(entries[limit-1].CreatedAt, entries[limit-1].ID)
		entries = entries[:limit]
	}
	return entries, next, nil
}

func (r *WalletRepository) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var stored int64
	var currency string
	if err := r.db.QueryRow(ctx, `SELECT balance_minor,currency FROM wallets WHERE id=$1`, walletID).Scan(&stored, &currency); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Reconciliation{}, fmt.Errorf("wallet: %w", ErrNotFound)
		}
		return Reconciliation{}, err
	}
	var credits, debits int64
	var count int
	err := r.db.QueryRow(ctx, `SELECT COALESCE(SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END),0), COALESCE(SUM(CASE WHEN direction='DEBIT' THEN amount_minor ELSE 0 END),0), COUNT(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, walletID).Scan(&credits, &debits, &count)
	if err != nil {
		return Reconciliation{}, err
	}
	calculated := credits - debits
	if calculated < 0 {
		return Reconciliation{}, fmt.Errorf("ledger calculated negative balance")
	}
	storedM, err := getMoney(stored, currency)
	if err != nil {
		return Reconciliation{}, err
	}
	calculatedM, err := getMoney(calculated, currency)
	if err != nil {
		return Reconciliation{}, err
	}
	diff, err := storedM.Sub(calculatedM)
	if err != nil {
		return Reconciliation{}, err
	}
	return Reconciliation{StoredBalance: storedM, CalculatedBalance: calculatedM, Difference: diff, Consistent: stored == calculated, CheckedEntries: count}, nil
}

func (r *WalletRepository) Exists(ctx context.Context, walletID uuid.UUID) (bool, error) {
	var one int
	err := r.db.QueryRow(ctx, `SELECT 1 FROM wallets WHERE id=$1`, walletID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func encodeLedgerCursor(t time.Time, id uuid.UUID) string {
	payload := t.UTC().Format(time.RFC3339Nano) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func decodeLedgerCursor(cursor string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor: %w", err)
	}
	parts := strings.Split(string(b), "|")
	if len(parts) != 2 {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor format")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor timestamp: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("invalid cursor id: %w", err)
	}
	return t, id, nil
}
