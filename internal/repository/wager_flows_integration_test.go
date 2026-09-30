//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/platform/database"
)

type flowEnv struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	wallets  *WalletRepository
	wagers   *WagerRepository
	provider string
}

func newFlowEnv(t *testing.T) flowEnv {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	outbox := messaging.NewOutboxRepository(pool)
	provider := "provider-" + uuid.NewString()
	return flowEnv{
		ctx:      ctx,
		pool:     pool,
		wallets:  NewWalletRepository(pool, outbox),
		wagers:   NewWagerRepository(pool, outbox),
		provider: provider,
	}
}

func (e flowEnv) wallet(t *testing.T, initial string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	player := uuid.New()
	w, err := e.wallets.Create(e.ctx, CreateWalletInput{PlayerID: player, InitialAmount: money(t, initial)})
	if err != nil {
		t.Fatal(err)
	}
	return w.ID, player
}

func (e flowEnv) input(t *testing.T, wallet, player uuid.UUID, kind, ext, amount, ref string) WagerInput {
	t.Helper()
	return WagerInput{
		ProviderID:            e.provider,
		ExternalTransactionID: ext,
		IdempotencyKey:        "key-" + ext,
		PlayerID:              player,
		WalletID:              wallet,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Money:                 money(t, amount),
		ReferenceExternalID:   ref,
	}
}

func (e flowEnv) eventTypes(t *testing.T, txIDs ...uuid.UUID) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, id := range txIDs {
		rows, err := e.pool.Query(e.ctx, `SELECT event_type FROM outbox_events WHERE causation_id=$1`, id.String())
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var typ string
			if err := rows.Scan(&typ); err != nil {
				t.Fatal(err)
			}
			out[typ]++
		}
		rows.Close()
	}
	return out
}

func (e flowEnv) balance(t *testing.T, wallet uuid.UUID) int64 {
	t.Helper()
	var b int64
	if err := e.pool.QueryRow(e.ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, wallet).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOpeningEmitsProcessedAndBalanceEvents(t *testing.T) {
	e := newFlowEnv(t)
	wallet, _ := e.wallet(t, "100.00")
	var opening uuid.UUID
	if err := e.pool.QueryRow(e.ctx, `SELECT id FROM wager_transactions WHERE wallet_id=$1 AND kind='OPENING'`, wallet).Scan(&opening); err != nil {
		t.Fatal(err)
	}
	got := e.eventTypes(t, opening)
	if got[messaging.EventWagerTransactionProcessed] != 1 || got[messaging.EventWalletBalanceChanged] != 1 {
		t.Fatalf("OPENING must emit Processed and BalanceChanged, got %v", got)
	}
}

func TestExhaustedReferenceIsRejectedWithEvent(t *testing.T) {
	e := newFlowEnv(t)
	wallet, player := e.wallet(t, "100.00")
	in := e.input(t, wallet, player, "REFUND", "late-refund", "10.00", "bet-that-never-arrives")

	first, err := e.wagers.Process(e.ctx, in)
	if err != nil || first.Status != "PENDING_REFERENCE" {
		t.Fatalf("first call: %+v err=%v", first, err)
	}
	if again, err := e.wagers.Process(e.ctx, in); err != nil || again.Status != "PENDING_REFERENCE" {
		t.Fatalf("replay: %+v err=%v", again, err)
	}
	if n := e.eventTypes(t, first.TransactionID)[messaging.EventWagerTransactionPendingReference]; n != 1 {
		t.Fatalf("PendingReference must be emitted exactly once, got %d", n)
	}

	in.RetryAttempt = true
	var last WagerResult

	for i := 0; i <= maxReferenceAttempts+2; i++ {
		last, err = e.wagers.Process(e.ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if last.Status == "REJECTED" {
			break
		}
		if last.Status != "PENDING_REFERENCE" {
			t.Fatalf("unexpected status: %s", last.Status)
		}
	}

	if last.Status != "REJECTED" || last.FailureCode != "REFERENCE_NOT_FOUND" {
		t.Fatalf("exhausted reference must be REJECTED/REFERENCE_NOT_FOUND, got %+v", last)
	}
	types := e.eventTypes(t, first.TransactionID)
	if types[messaging.EventWagerTransactionRejected] != 1 {
		t.Fatalf("expected one Rejected event, got %v", types)
	}
	if e.balance(t, wallet) != 10000 {
		t.Fatal("a rejected reversal must not touch the balance")
	}

	in.RetryAttempt = false
	replay, err := e.wagers.Process(e.ctx, in)
	if err != nil || !replay.Replay || replay.Status != "REJECTED" || replay.FailureCode != "REFERENCE_NOT_FOUND" {
		t.Fatalf("replay of the terminal rejection: %+v err=%v", replay, err)
	}
}

func TestLossAndSingleReversalRules(t *testing.T) {
	e := newFlowEnv(t)
	wallet, player := e.wallet(t, "100.00")

	bet, err := e.wagers.Process(e.ctx, e.input(t, wallet, player, "BET", "bet-1", "30.00", ""))
	if err != nil || bet.Status != "PROCESSED" || e.balance(t, wallet) != 7000 {
		t.Fatalf("bet: %+v err=%v", bet, err)
	}

	loss, err := e.wagers.Process(e.ctx, e.input(t, wallet, player, "LOSS", "loss-1", "0.00", ""))
	if err != nil || loss.Status != "PROCESSED" {
		t.Fatalf("LOSS 0.00 must be processed: %+v err=%v", loss, err)
	}
	var entries int
	_ = e.pool.QueryRow(e.ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id=$1`, loss.TransactionID).Scan(&entries)
	types := e.eventTypes(t, loss.TransactionID)
	if entries != 0 || types[messaging.EventWalletBalanceChanged] != 0 || types[messaging.EventWagerTransactionProcessed] != 1 || e.balance(t, wallet) != 7000 {
		t.Fatalf("LOSS must not move money: entries=%d events=%v", entries, types)
	}
	if _, err := e.wagers.Process(e.ctx, e.input(t, wallet, player, "LOSS", "loss-2", "1.00", "")); err == nil {
		t.Fatal("LOSS with a value must be an error")
	}

	refund, err := e.wagers.Process(e.ctx, e.input(t, wallet, player, "REFUND", "refund-1", "30.00", "bet-1"))
	if err != nil || refund.Status != "PROCESSED" || e.balance(t, wallet) != 10000 {
		t.Fatalf("refund: %+v err=%v", refund, err)
	}
	second, err := e.wagers.Process(e.ctx, e.input(t, wallet, player, "ROLLBACK", "rollback-1", "30.00", "bet-1"))
	if err != nil || second.Status != "REJECTED" || second.FailureCode != "ALREADY_REVERSED" || e.balance(t, wallet) != 10000 {
		t.Fatalf("second reversal: %+v err=%v", second, err)
	}
	if e.eventTypes(t, second.TransactionID)[messaging.EventWagerTransactionRejected] != 1 {
		t.Fatal("rejection must emit WagerTransactionRejected")
	}

	var payload []byte
	if err := e.pool.QueryRow(e.ctx, `SELECT payload FROM outbox_events WHERE causation_id=$1 AND event_type=$2`, refund.TransactionID.String(), messaging.EventWalletBalanceChanged).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil || data["direction"] != "CREDIT" {
		t.Fatalf("unexpected balance event %s err=%v", payload, err)
	}
}
