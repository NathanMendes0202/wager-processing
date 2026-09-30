package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
)

var (
	ErrUnsupportedKind     = errors.New("unsupported wager kind")
	ErrInvalidAmount       = errors.New("invalid amount for wager kind")
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
	ErrExternalIDConflict  = errors.New("external transaction id already used with another idempotency key")
	ErrWalletNotFound      = errors.New("wallet not found")
)

type WagerRepository struct {
	db     *pgxpool.Pool
	outbox *messaging.OutboxRepository
}

func NewWagerRepository(db *pgxpool.Pool, outbox *messaging.OutboxRepository) *WagerRepository {
	return &WagerRepository{db: db, outbox: outbox}
}

type WagerInput struct {
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              uuid.UUID
	WalletID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  string
	Money                 domain.Money
	ReferenceExternalID   string
	RetryAttempt          bool
}

type WagerResult struct {
	TransactionID uuid.UUID    `json:"transactionId"`
	Status        string       `json:"status"`
	Balance       domain.Money `json:"balance"`
	WalletVersion int64        `json:"walletVersion"`
	Replay        bool         `json:"idempotentReplay"`
	FailureCode   string       `json:"failureCode,omitempty"`
}

func PayloadHash(in WagerInput) string {
	payload := struct {
		ProviderID string `json:"providerId"`
		ExternalID string `json:"externalTransactionId"`
		PlayerID   string `json:"playerId"`
		WalletID   string `json:"walletId"`
		RoundID    string `json:"roundId"`
		GameID     string `json:"gameId"`
		Kind       string `json:"kind"`
		Amount     string `json:"amount"`
		Currency   string `json:"currency"`
		Reference  string `json:"referenceExternalTransactionId,omitempty"`
	}{in.ProviderID, in.ExternalTransactionID, in.PlayerID.String(), in.WalletID.String(), in.RoundID, in.GameID, in.Kind, in.Money.String(), in.Money.Currency(), in.ReferenceExternalID}
	b, _ := json.Marshal(payload)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func ExternalKind(kind string) bool {
	switch kind {
	case "BET", "WIN", "LOSS", "REFUND", "ROLLBACK":
		return true
	}
	return false
}

func ValidateAmount(kind string, m domain.Money) error {
	if kind == "LOSS" {
		if !m.IsZero() {
			return fmt.Errorf("%w: LOSS requires 0.00", ErrInvalidAmount)
		}
		return nil
	}
	if !m.IsPositive() {
		return fmt.Errorf("%w: %s requires an amount greater than zero", ErrInvalidAmount, kind)
	}
	return nil
}

func (r *WagerRepository) ProcessBet(ctx context.Context, in WagerInput) (WagerResult, error) {
	in.Kind = "BET"
	return r.Process(ctx, in)
}

type walletSnapshot struct {
	balance domain.Money
	version int64
}

type referenceRow struct {
	id       uuid.UUID
	kind     string
	status   string
	amount   int64
	currency string
	walletID uuid.UUID
	playerID uuid.UUID
	roundID  string
}

func (r *WagerRepository) Process(ctx context.Context, in WagerInput) (result WagerResult, err error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return result, err
	}
	result, err = r.ProcessTx(ctx, tx, in)
	if err != nil {
		_ = tx.Rollback(ctx)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return r.resolveConflict(ctx, in, PayloadHash(in), err)
		}
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (r *WagerRepository) ProcessTx(ctx context.Context, tx pgx.Tx, in WagerInput) (result WagerResult, err error) {
	if !ExternalKind(in.Kind) {
		return result, fmt.Errorf("%w: %q", ErrUnsupportedKind, in.Kind)
	}
	if err = ValidateAmount(in.Kind, in.Money); err != nil {
		return result, err
	}

	hash := PayloadHash(in)
	var txID uuid.UUID

	existing, lookupErr := lookupByKey(ctx, tx, in.ProviderID, in.IdempotencyKey)
	switch {
	case lookupErr == nil:
		if existing.hash != hash {
			return result, ErrIdempotencyConflict
		}
		if existing.status != "PENDING_REFERENCE" {
			replay, e := existing.result()
			if e != nil {
				return result, e
			}
			return replay, nil
		}
		txID = existing.id
	case errors.Is(lookupErr, pgx.ErrNoRows):
		var checkExists bool
		if err := tx.QueryRow(ctx, `SELECT true FROM wallets WHERE id=$1 AND player_id=$2 FOR UPDATE`, in.WalletID, in.PlayerID).Scan(&checkExists); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return result, ErrWalletNotFound
			}
			return result, err
		}

		txID = uuid.New()
		_, e := tx.Exec(ctx, `INSERT INTO wager_transactions (id,external_transaction_id,provider_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,reference_external_transaction_id,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING')`,
			txID, in.ExternalTransactionID, in.ProviderID, in.IdempotencyKey, hash, in.WalletID, in.PlayerID, in.RoundID, in.GameID, in.Kind, in.Money.MinorUnits(), in.Money.Currency(), nullableString(in.ReferenceExternalID))
		if e != nil {
			return r.handleInsertError(ctx, tx, in, hash, e)
		}
	default:
		return result, lookupErr
	}

	var balanceMinor, version int64
	var walletCurrency string
	e := tx.QueryRow(ctx, `SELECT balance_minor,currency,version FROM wallets WHERE id=$1 AND player_id=$2 FOR UPDATE`, in.WalletID, in.PlayerID).Scan(&balanceMinor, &walletCurrency, &version)
	if errors.Is(e, pgx.ErrNoRows) {
		return r.rejectTx(ctx, tx, txID, "WALLET_PLAYER_MISMATCH", nil)
	}
	if e != nil {
		return result, e
	}
	walletMoney, e := domain.NewMoneyFromMinorUnits(balanceMinor, walletCurrency)
	if e != nil {
		return result, e
	}
	snap := &walletSnapshot{balance: walletMoney, version: version}
	if walletCurrency != in.Money.Currency() {
		return r.rejectTx(ctx, tx, txID, "CURRENCY_MISMATCH", snap)
	}

	var ref *referenceRow
	if in.Kind == "REFUND" || in.Kind == "ROLLBACK" {
		if strings.TrimSpace(in.ReferenceExternalID) == "" {
			return r.rejectTx(ctx, tx, txID, "REFERENCE_REQUIRED", snap)
		}
		row, e := loadReference(ctx, tx, in)
		if errors.Is(e, pgx.ErrNoRows) {
			return r.markPendingReference(ctx, tx, txID)
		}
		if e != nil {
			return result, e
		}
		if row.id == txID {
			return r.rejectTx(ctx, tx, txID, "INVALID_REFERENCE", snap)
		}
		switch row.status {
		case "PROCESSED":
		case "PENDING", "PENDING_REFERENCE":
			return r.markPendingReference(ctx, tx, txID)
		default:
			return r.rejectTx(ctx, tx, txID, "REFERENCE_NOT_PROCESSED", snap)
		}
		if code := validateReference(in, row); code != "" {
			return r.rejectTx(ctx, tx, txID, code, snap)
		}
		var reversed bool
		e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wager_transactions WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED' AND id<>$2)`, row.id, txID).Scan(&reversed)
		if e != nil {
			return result, e
		}
		if reversed {
			return r.rejectTx(ctx, tx, txID, "ALREADY_REVERSED", snap)
		}
		ref = &row
	}

	direction := movementDirection(in.Kind, ref)
	newBalance := walletMoney
	newVersion := version

	switch direction {
	case "DEBIT":
		w, e := domain.RehydrateWallet(in.WalletID, in.PlayerID, walletMoney, version)
		if e != nil {
			return result, e
		}
		nb, e := w.Debit(in.Money)
		if errors.Is(e, domain.ErrInsufficientBalance) {
			code := "INSUFFICIENT_BALANCE"
			if in.Kind != "BET" {
				code = "INSUFFICIENT_BALANCE_FOR_REVERSAL"
			}
			return r.rejectTx(ctx, tx, txID, code, snap)
		}
		if e != nil {
			return result, e
		}
		newBalance = nb
	case "CREDIT":
		nb, e := walletMoney.Add(in.Money)
		if e != nil {
			return result, e
		}
		newBalance = nb
	}

	if direction != "" {
		newVersion = version + 1
		if _, e := tx.Exec(ctx, `UPDATE wallets SET balance_minor=$1,version=$2,updated_at=now() WHERE id=$3`, newBalance.MinorUnits(), newVersion, in.WalletID); e != nil {
			return result, e
		}
		if _, e := tx.Exec(ctx, `INSERT INTO wallet_ledger_entries (transaction_id,wallet_id,direction,amount_minor,balance_before_minor,balance_after_minor) VALUES ($1,$2,$3,$4,$5,$6)`, txID, in.WalletID, direction, in.Money.MinorUnits(), walletMoney.MinorUnits(), newBalance.MinorUnits()); e != nil {
			return result, e
		}
	}

	var referenceID *uuid.UUID
	if ref != nil {
		referenceID = &ref.id
	}
	var currentStatus string
	if e := tx.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id=$1 FOR UPDATE`, txID).Scan(&currentStatus); e != nil {
		return result, e
	}
	if e := domain.ValidateTransactionTransition(domain.TransactionStatus(currentStatus), domain.TransactionProcessed); e != nil {
		return result, e
	}
	if _, e := tx.Exec(ctx, `UPDATE wager_transactions SET status='PROCESSED',reference_transaction_id=$1,result_balance_minor=$2,result_wallet_version=$3,updated_at=now() WHERE id=$4`, referenceID, newBalance.MinorUnits(), newVersion, txID); e != nil {
		return result, e
	}

	now := time.Now().UTC()
	processed, e := json.Marshal(messaging.WagerTransactionProcessedData{
		TransactionID:                  txID,
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		WalletID:                       in.WalletID,
		PlayerID:                       in.PlayerID,
		Kind:                           in.Kind,
		Money:                          messaging.MoneyData{Amount: in.Money.String(), Currency: in.Money.Currency()},
		Status:                         "PROCESSED",
		Balance:                        messaging.MoneyData{Amount: newBalance.String(), Currency: newBalance.Currency()},
		WalletVersion:                  newVersion,
		ReferenceExternalTransactionID: in.ReferenceExternalID,
	})
	if e != nil {
		return result, e
	}

	if e = r.outbox.InsertTx(ctx, tx, messaging.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: txID,
		EventType:   "WagerTransactionProcessed",
		Correlation: in.IdempotencyKey,
		Causation:   txID.String(),
		OccurredAt:  now,
		Version:     1,
		Payload:     processed,
	}); e != nil {
		return result, e
	}

	if direction != "" {
		changed, e := json.Marshal(messaging.WalletBalanceChangedData{
			WalletID:      in.WalletID,
			TransactionID: txID,
			Direction:     direction,
			Money:         messaging.MoneyData{Amount: in.Money.String(), Currency: in.Money.Currency()},
			BalanceBefore: messaging.MoneyData{Amount: walletMoney.String(), Currency: walletMoney.Currency()},
			BalanceAfter:  messaging.MoneyData{Amount: newBalance.String(), Currency: newBalance.Currency()},
			WalletVersion: newVersion,
		})
		if e != nil {
			return result, e
		}
		if e = r.outbox.InsertTx(ctx, tx, messaging.OutboxEvent{ID: uuid.New(), AggregateID: in.WalletID, EventType: "WalletBalanceChanged", Correlation: in.IdempotencyKey, Causation: txID.String(), OccurredAt: now, Version: newVersion, Payload: changed}); e != nil {
			return result, e
		}
	}

	return WagerResult{TransactionID: txID, Status: "PROCESSED", Balance: newBalance, WalletVersion: newVersion}, nil
}

func movementDirection(kind string, ref *referenceRow) string {
	switch kind {
	case "BET":
		return "DEBIT"
	case "WIN", "REFUND":
		return "CREDIT"
	case "ROLLBACK":
		if ref != nil && ref.kind == "BET" {
			return "CREDIT"
		}
		return "DEBIT"
	}
	return ""
}

func validateReference(in WagerInput, ref referenceRow) string {
	if ref.walletID != in.WalletID || ref.playerID != in.PlayerID || ref.roundID != in.RoundID {
		return "REFERENCE_MISMATCH"
	}
	if ref.currency != in.Money.Currency() || ref.amount != in.Money.MinorUnits() {
		return "REFERENCE_AMOUNT_MISMATCH"
	}
	switch in.Kind {
	case "REFUND":
		if ref.kind != "BET" {
			return "INVALID_REFERENCE_KIND"
		}
	case "ROLLBACK":
		if ref.kind != "BET" && ref.kind != "WIN" && ref.kind != "REFUND" {
			return "INVALID_REFERENCE_KIND"
		}
	}
	return ""
}

func loadReference(ctx context.Context, tx pgx.Tx, in WagerInput) (referenceRow, error) {
	var row referenceRow
	err := tx.QueryRow(ctx, `SELECT id,kind,status,amount_minor,currency,wallet_id,player_id,COALESCE(round_id,'') FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, in.ProviderID, in.ReferenceExternalID).
		Scan(&row.id, &row.kind, &row.status, &row.amount, &row.currency, &row.walletID, &row.playerID, &row.roundID)
	return row, err
}

const (
	maxReferenceAttempts    = 10
	initialReferenceBackoff = 5 * time.Second
	maxReferenceBackoff     = 5 * time.Minute
)

type PendingReference struct {
	ID                    uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              uuid.UUID
	WalletID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  string
	AmountMinor           int64
	Currency              string
	ReferenceExternalID   string
}

func referenceBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := initialReferenceBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= maxReferenceBackoff {
			return maxReferenceBackoff
		}
	}
	if d > maxReferenceBackoff {
		return maxReferenceBackoff
	}
	return d
}

func (r *WagerRepository) markPendingReference(ctx context.Context, tx pgx.Tx, id uuid.UUID) (WagerResult, error) {
	var attempts int
	var currentStatus string
	if err := tx.QueryRow(ctx, `SELECT reference_attempts,status FROM wager_transactions WHERE id=$1 FOR UPDATE`, id).Scan(&attempts, &currentStatus); err != nil {
		return WagerResult{}, err
	}
	attempts++
	if attempts >= maxReferenceAttempts {
		if err := domain.ValidateTransactionTransition(domain.TransactionStatus(currentStatus), domain.TransactionRejected); err != nil {
			return WagerResult{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE wager_transactions SET status='REJECTED',failure_code='REFERENCE_NOT_FOUND',reference_attempts=$1,last_reference_error='reference could not be resolved after maximum retries',next_reference_attempt_at=NULL,updated_at=now() WHERE id=$2`, attempts, id); err != nil {
			return WagerResult{}, err
		}
		if err := r.insertRejectedEvent(ctx, tx, id, "REFERENCE_NOT_FOUND"); err != nil {
			return WagerResult{}, err
		}
		return WagerResult{TransactionID: id, Status: "REJECTED", FailureCode: "REFERENCE_NOT_FOUND"}, nil
	}

	if err := domain.ValidateTransactionTransition(domain.TransactionStatus(currentStatus), domain.TransactionPendingReference); err != nil {
		return WagerResult{}, err
	}
	next := time.Now().UTC().Add(referenceBackoff(attempts))
	if _, err := tx.Exec(ctx, `UPDATE wager_transactions SET status='PENDING_REFERENCE',reference_attempts=$1,next_reference_attempt_at=$2,last_reference_error='reference not available yet',updated_at=now() WHERE id=$3`, attempts, next, id); err != nil {
		return WagerResult{}, err
	}
	if attempts == 1 {
		if err := r.insertPendingReferenceEvent(ctx, tx, id, attempts); err != nil {
			return WagerResult{}, err
		}
	}
	return WagerResult{TransactionID: id, Status: "PENDING_REFERENCE"}, nil
}

func (r *WagerRepository) ClaimPendingReferences(ctx context.Context, limit int) ([]PendingReference, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id,provider_id,external_transaction_id,idempotency_key,player_id,wallet_id,
		       COALESCE(round_id,''),COALESCE(game_id,''),kind,amount_minor,currency,
		       COALESCE(reference_external_transaction_id,'')
		FROM wager_transactions
		WHERE status='PENDING_REFERENCE'
		  AND (next_reference_attempt_at IS NULL OR next_reference_attempt_at <= now())
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}

	var pending []PendingReference
	for rows.Next() {
		var p PendingReference
		if err := rows.Scan(&p.ID, &p.ProviderID, &p.ExternalTransactionID, &p.IdempotencyKey, &p.PlayerID, &p.WalletID, &p.RoundID, &p.GameID, &p.Kind, &p.AmountMinor, &p.Currency, &p.ReferenceExternalID); err != nil {
			rows.Close()
			return nil, err
		}
		pending = append(pending, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, p := range pending {
		if _, err := tx.Exec(ctx, `UPDATE wager_transactions SET next_reference_attempt_at=$1,updated_at=now() WHERE id=$2`, time.Now().UTC().Add(2*time.Minute), p.ID); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return pending, nil
}

func (r *WagerRepository) rejectTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, code string, snap *walletSnapshot) (WagerResult, error) {
	var currentStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id=$1 FOR UPDATE`, id).Scan(&currentStatus); err != nil {
		return WagerResult{}, err
	}
	if err := domain.ValidateTransactionTransition(domain.TransactionStatus(currentStatus), domain.TransactionRejected); err != nil {
		return WagerResult{}, err
	}
	var balance, version *int64
	res := WagerResult{TransactionID: id, Status: "REJECTED", FailureCode: code}
	if snap != nil {
		b, v := snap.balance.MinorUnits(), snap.version
		balance, version = &b, &v
		res.Balance, res.WalletVersion = snap.balance, snap.version
	}
	if _, err := tx.Exec(ctx, `UPDATE wager_transactions SET status='REJECTED',failure_code=$1,result_balance_minor=$2,result_wallet_version=$3,updated_at=now() WHERE id=$4`, code, balance, version, id); err != nil {
		return WagerResult{}, err
	}
	if err := r.insertRejectedEvent(ctx, tx, id, code); err != nil {
		return WagerResult{}, err
	}
	return res, nil
}

func (r *WagerRepository) insertRejectedEvent(ctx context.Context, tx pgx.Tx, id uuid.UUID, code string) error {
	var providerID, externalID, idempotencyKey, walletIDStr, playerIDStr, kind string
	if err := tx.QueryRow(ctx, `SELECT provider_id,external_transaction_id,idempotency_key,wallet_id::text,player_id::text,kind FROM wager_transactions WHERE id=$1`, id).Scan(&providerID, &externalID, &idempotencyKey, &walletIDStr, &playerIDStr, &kind); err != nil {
		return err
	}

	walletID, err := uuid.Parse(walletIDStr)
	if err != nil {
		return err
	}
	playerID, err := uuid.Parse(playerIDStr)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(messaging.WagerTransactionRejectedData{
		TransactionID:         id,
		ProviderID:            providerID,
		ExternalTransactionID: externalID,
		WalletID:              walletID,
		PlayerID:              playerID,
		Kind:                  kind,
		FailureCode:           code,
	})
	if err != nil {
		return err
	}
	return r.outbox.InsertTx(ctx, tx, messaging.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: id,
		EventType:   "WagerTransactionRejected",
		Correlation: idempotencyKey,
		Causation:   id.String(),
		OccurredAt:  time.Now().UTC(),
		Version:     1,
		Payload:     payload,
	})
}

func (r *WagerRepository) insertPendingReferenceEvent(ctx context.Context, tx pgx.Tx, id uuid.UUID, attempt int) error {
	var providerID, externalID, idempotencyKey, referenceID string
	if err := tx.QueryRow(ctx, `SELECT provider_id,external_transaction_id,idempotency_key,COALESCE(reference_external_transaction_id,'') FROM wager_transactions WHERE id=$1`, id).Scan(&providerID, &externalID, &idempotencyKey, &referenceID); err != nil {
		return err
	}

	payload, err := json.Marshal(messaging.WagerTransactionPendingReferenceData{
		TransactionID:                  id,
		ProviderID:                     providerID,
		ExternalTransactionID:          externalID,
		ReferenceExternalTransactionID: referenceID,
		Attempt:                        attempt,
	})
	if err != nil {
		return err
	}
	return r.outbox.InsertTx(ctx, tx, messaging.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: id,
		EventType:   "WagerTransactionPendingReference",
		Correlation: idempotencyKey,
		Causation:   id.String(),
		OccurredAt:  time.Now().UTC(),
		Version:     int64(attempt),
		Payload:     payload,
	})
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type existingTx struct {
	id       uuid.UUID
	status   string
	hash     string
	currency string
	balance  int64
	version  int64
	failure  string
}

func lookupByKey(ctx context.Context, q rowQuerier, providerID, key string) (existingTx, error) {
	var e existingTx
	var failure *string
	err := q.QueryRow(ctx, `SELECT id,status,payload_hash,COALESCE(result_balance_minor,0),currency,COALESCE(result_wallet_version,0),failure_code FROM wager_transactions WHERE provider_id=$1 AND idempotency_key=$2`, providerID, key).
		Scan(&e.id, &e.status, &e.hash, &e.balance, &e.currency, &e.version, &failure)
	if failure != nil {
		e.failure = *failure
	}
	return e, err
}

func (e existingTx) result() (WagerResult, error) {
	money, err := domain.NewMoneyFromMinorUnits(e.balance, e.currency)
	if err != nil {
		return WagerResult{}, err
	}
	return WagerResult{TransactionID: e.id, Status: e.status, Balance: money, WalletVersion: e.version, Replay: true, FailureCode: e.failure}, nil
}

func (r *WagerRepository) handleInsertError(ctx context.Context, tx pgx.Tx, in WagerInput, hash string, cause error) (WagerResult, error) {
	var pgErr *pgconn.PgError
	if !errors.As(cause, &pgErr) {
		return WagerResult{}, cause
	}
	switch pgErr.Code {
	case "23503": // wallet_id foreign key
		return WagerResult{}, ErrWalletNotFound
	case "23505":
		return WagerResult{}, cause
	}
	return WagerResult{}, cause
}

func (r *WagerRepository) resolveConflict(ctx context.Context, in WagerInput, hash string, cause error) (WagerResult, error) {
	ex, err := lookupByKey(ctx, r.db, in.ProviderID, in.IdempotencyKey)
	if err == nil {
		if ex.hash != hash {
			return WagerResult{}, ErrIdempotencyConflict
		}
		return ex.result()
	}

	var existingID uuid.UUID
	var existingKey string
	var existingHash string
	var existingStatus string
	var existingBalance int64
	var existingCurrency string
	var existingVersion int64
	var existingFailure *string

	queryErr := r.db.QueryRow(ctx, `
		SELECT id, idempotency_key, payload_hash, status, COALESCE(result_balance_minor, 0), currency, COALESCE(result_wallet_version, 0), failure_code 
		FROM wager_transactions 
		WHERE provider_id = $1 AND external_transaction_id = $2`,
		in.ProviderID, in.ExternalTransactionID,
	).Scan(&existingID, &existingKey, &existingHash, &existingStatus, &existingBalance, &existingCurrency, &existingVersion, &existingFailure)

	if queryErr == nil {
		if existingKey != in.IdempotencyKey {
			return WagerResult{}, ErrExternalIDConflict
		}
		if existingHash != hash {
			return WagerResult{}, ErrIdempotencyConflict
		}
		money, err := domain.NewMoneyFromMinorUnits(existingBalance, existingCurrency)
		if err != nil {
			return WagerResult{}, err
		}
		failStr := ""
		if existingFailure != nil {
			failStr = *existingFailure
		}
		return WagerResult{
			TransactionID: existingID,
			Status:        existingStatus,
			Balance:       money,
			WalletVersion: existingVersion,
			Replay:        true,
			FailureCode:   failStr,
		}, nil
	}

	return WagerResult{}, cause
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}
