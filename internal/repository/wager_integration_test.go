//go:build integration

package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/platform/database"
)

func TestFinancialFlowsIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set; start PostgreSQL and set TEST_DATABASE_URL to run integration tests")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	outbox := messaging.NewOutboxRepository(pool)
	wallets := NewWalletRepository(pool, messaging.NewOutboxRepository(pool))
	wagers := NewWagerRepository(pool, outbox)

	playerID := uuid.New()
	wallet, err := wallets.Create(ctx, CreateWalletInput{PlayerID: playerID, InitialAmount: money(t, "1000.00")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupWallet(t, ctx, pool, wallet.ID)
	})

	provider := "integration-provider-a-" + uuid.NewString()
	base := WagerInput{
		ProviderID: provider,
		PlayerID:   playerID,
		WalletID:   wallet.ID,
		RoundID:    "round-integration",
		GameID:     "game-integration",
	}

	bet := base
	bet.ExternalTransactionID = "bet-001"
	bet.IdempotencyKey = "idem-bet-001"
	bet.Kind = "BET"
	bet.Money = money(t, "100.00")

	first, err := wagers.Process(ctx, bet)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "PROCESSED" || first.Balance.String() != "900.00" {
		t.Fatalf("BET result = %+v", first)
	}

	replay, err := wagers.Process(ctx, bet)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replay || replay.TransactionID != first.TransactionID || replay.Balance.String() != "900.00" {
		t.Fatalf("idempotent replay = %+v", replay)
	}

	conflict := bet
	conflict.Money = money(t, "90.00")
	if _, err := wagers.Process(ctx, conflict); err != ErrIdempotencyConflict {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}

	externalConflict := bet
	externalConflict.IdempotencyKey = "idem-bet-002"
	if _, err := wagers.Process(ctx, externalConflict); err != ErrExternalIDConflict {
		t.Fatalf("expected external transaction conflict, got %v", err)
	}

	win := base
	win.ExternalTransactionID = "win-001"
	win.IdempotencyKey = "idem-win-001"
	win.Kind = "WIN"
	win.Money = money(t, "50.00")
	result, err := wagers.Process(ctx, win)
	if err != nil || result.Balance.String() != "950.00" {
		t.Fatalf("WIN result = %+v err=%v", result, err)
	}

	loss := base
	loss.ExternalTransactionID = "loss-001"
	loss.IdempotencyKey = "idem-loss-001"
	loss.Kind = "LOSS"
	loss.Money = money(t, "0.00")
	result, err = wagers.Process(ctx, loss)
	if err != nil || result.Balance.String() != "950.00" {
		t.Fatalf("LOSS result = %+v err=%v", result, err)
	}

	refund := base
	refund.ExternalTransactionID = "refund-001"
	refund.IdempotencyKey = "idem-refund-001"
	refund.Kind = "REFUND"
	refund.ReferenceExternalID = bet.ExternalTransactionID
	refund.Money = money(t, "100.00")
	result, err = wagers.Process(ctx, refund)
	if err != nil || result.Balance.String() != "1050.00" {
		t.Fatalf("REFUND result = %+v err=%v", result, err)
	}

	rollback := base
	rollback.ExternalTransactionID = "rollback-001"
	rollback.IdempotencyKey = "idem-rollback-001"
	rollback.Kind = "ROLLBACK"
	rollback.ReferenceExternalID = win.ExternalTransactionID
	rollback.Money = money(t, "50.00")
	result, err = wagers.Process(ctx, rollback)
	if err != nil || result.Balance.String() != "1000.00" {
		t.Fatalf("ROLLBACK result = %+v err=%v", result, err)
	}

	duplicateRefund := base
	duplicateRefund.ExternalTransactionID = "refund-002"
	duplicateRefund.IdempotencyKey = "idem-refund-002"
	duplicateRefund.Kind = "REFUND"
	duplicateRefund.ReferenceExternalID = bet.ExternalTransactionID
	duplicateRefund.Money = money(t, "100.00")
	result, err = wagers.Process(ctx, duplicateRefund)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "REJECTED" || result.FailureCode != "ALREADY_REVERSED" {
		t.Fatalf("duplicate REFUND result = %+v", result)
	}

	reconciliation, err := wallets.Reconcile(ctx, wallet.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reconciliation.Consistent || reconciliation.StoredBalance.String() != "1000.00" || reconciliation.CalculatedBalance.String() != "1000.00" {
		t.Fatalf("reconciliation = %+v", reconciliation)
	}
	if reconciliation.CheckedEntries != 5 {
		t.Fatalf("expected 5 ledger entries (opening, bet, win, refund, rollback), got %d", reconciliation.CheckedEntries)
	}

	otherProvider, err := wagers.GetByProviderExternal(ctx, "integration-provider-b", bet.ExternalTransactionID)
	if err == nil {
		t.Fatalf("provider isolation lookup unexpectedly returned %+v", otherProvider)
	}
	if _, err := wagers.GetByProviderExternal(ctx, provider, bet.ExternalTransactionID); err != nil {
		t.Fatalf("same-provider lookup failed: %v", err)
	}
}

func cleanupWallet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, walletID uuid.UUID) {
	t.Helper()
	_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE causation_id IN (SELECT id::text FROM wager_transactions WHERE wallet_id=$1)`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE wallet_id=$1`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wager_transactions WHERE wallet_id=$1`, walletID)
	_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id=$1`, walletID)
}
