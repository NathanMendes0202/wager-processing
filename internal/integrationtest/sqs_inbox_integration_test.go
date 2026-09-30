//go:build integration

package integrationtest

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/metrics"
	"github.com/NathanMendes0202/wager-processing/internal/platform/database"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
	"github.com/NathanMendes0202/wager-processing/internal/service"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSQSInboxAndFinancialCommitAreAtomic(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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

	playerID, walletID := uuid.New(), uuid.New()
	messageID := "atomic-sqs-" + uuid.NewString()
	providerID := "provider-" + uuid.NewString()
	externalID := "atomic-ext-" + uuid.NewString()
	idempotencyKey := "atomic-key-" + uuid.NewString()
	_, err = pool.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance_minor,version) VALUES($1,$2,'BRL',10000,1)`, walletID, playerID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM inbox_messages WHERE message_id=$1`, messageID)
		_, _ = pool.Exec(ctx, `DELETE FROM wallets WHERE id=$1`, walletID)
	})

	outbox := messaging.NewOutboxRepository(pool)
	inbox := messaging.NewInboxRepository(pool)
	wagers := service.NewWagerService(repository.NewWagerRepository(pool, outbox), metrics.New())
	handler := service.NewWagerMessageHandler(wagers, pool, inbox)
	body := `{"messageId":"` + messageID + `","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{"providerId":"` + providerID + `","externalTransactionId":"` + externalID + `","idempotencyKey":"` + idempotencyKey + `","playerId":"` + playerID.String() + `","walletId":"` + walletID.String() + `","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}}`
	msg := messaging.WagerTransactionMessage{}
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		t.Fatal(err)
	}
	msg.RawBody = body

	if err := handler.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}

	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 2000 {
		t.Fatalf("expected balance 20.00, got %d", balance)
	}
	var completed bool
	if err := pool.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2`, "wager-transaction-consumer", messageID).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("inbox message must be completed in the same committed transaction")
	}
}

func TestPendingReferenceIsDurableAndInboxCompletesAtomically(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
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

	playerID, walletID := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance_minor,version) VALUES($1,$2,'BRL',10000,1)`, walletID, playerID); err != nil {
		t.Fatal(err)
	}
	outbox := messaging.NewOutboxRepository(pool)
	inbox := messaging.NewInboxRepository(pool)
	wagerRepo := repository.NewWagerRepository(pool, outbox)
	wagers := service.NewWagerService(wagerRepo, metrics.New())
	handler := service.NewWagerMessageHandler(wagers, pool, inbox)
	provider := "pending-provider-" + uuid.NewString()
	refundExt := "refund-" + uuid.NewString()
	refundKey := "refund-key-" + uuid.NewString()
	messageID := "pending-msg-" + uuid.NewString()

	refundBody := `{"messageId":"` + messageID + `","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{"providerId":"` + provider + `","externalTransactionId":"` + refundExt + `","idempotencyKey":"` + refundKey + `","playerId":"` + playerID.String() + `","walletId":"` + walletID.String() + `","roundId":"round-pending","gameId":"game-pending","kind":"REFUND","money":{"amount":"10.00","currency":"BRL"},"referenceExternalTransactionId":"` + "bet-late-" + provider + `"}}`
	var refundMsg messaging.WagerTransactionMessage
	if err := json.Unmarshal([]byte(refundBody), &refundMsg); err != nil {
		t.Fatal(err)
	}
	refundMsg.RawBody = refundBody
	if err := handler.Handle(ctx, refundMsg); err != nil {
		t.Fatalf("pending refund delivery: %v", err)
	}

	var refundStatus string
	var completed bool
	if err := pool.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, provider, refundExt).Scan(&refundStatus); err != nil {
		t.Fatal(err)
	}
	if refundStatus != "PENDING_REFERENCE" {
		t.Fatalf("expected PENDING_REFERENCE, got %s", refundStatus)
	}
	if err := pool.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2`, "wager-transaction-consumer", messageID).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("durably persisted PENDING_REFERENCE must complete the Inbox in the same transaction")
	}

	betExt := "bet-late-" + provider
	betBody := `{"messageId":"bet-msg-` + uuid.NewString() + `","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{"providerId":"` + provider + `","externalTransactionId":"` + betExt + `","idempotencyKey":"bet-key-` + uuid.NewString() + `","playerId":"` + playerID.String() + `","walletId":"` + walletID.String() + `","roundId":"round-pending","gameId":"game-pending","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}`
	var betMsg messaging.WagerTransactionMessage
	if err := json.Unmarshal([]byte(betBody), &betMsg); err != nil {
		t.Fatal(err)
	}
	betMsg.RawBody = betBody
	if err := handler.Handle(ctx, betMsg); err != nil {
		t.Fatalf("late reference BET delivery: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET next_reference_attempt_at=now() WHERE provider_id=$1 AND external_transaction_id=$2`, provider, refundExt); err != nil {
		t.Fatal(err)
	}

	worker := service.NewPendingReferenceWorker(wagerRepo, metrics.New())
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); worker.Run(workerCtx) }()
	defer func() { stopWorker(); <-workerDone }()

	for {
		if err := pool.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, provider, refundExt).Scan(&refundStatus); err != nil {
			t.Fatal(err)
		}
		if refundStatus == "PROCESSED" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pending reference was not resolved; last status=%s", refundStatus)
		case <-time.After(100 * time.Millisecond):
		}
	}

	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 10000 {
		t.Fatalf("BET followed by REFUND should restore opening balance; got %d minor units", balance)
	}

	// Exhaustion is a terminal business rejection, not a generic FAILED state.
	expiredExt := "refund-expired-" + uuid.NewString()
	expiredMessageID := "expired-msg-" + uuid.NewString()
	expiredBody := `{"messageId":"` + expiredMessageID + `","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{"providerId":"` + provider + `","externalTransactionId":"` + expiredExt + `","idempotencyKey":"expired-key-` + uuid.NewString() + `","playerId":"` + playerID.String() + `","walletId":"` + walletID.String() + `","roundId":"round-pending","gameId":"game-pending","kind":"REFUND","money":{"amount":"10.00","currency":"BRL"},"referenceExternalTransactionId":"never-arrived-` + provider + `"}}`
	var expiredMsg messaging.WagerTransactionMessage
	if err := json.Unmarshal([]byte(expiredBody), &expiredMsg); err != nil {
		t.Fatal(err)
	}
	expiredMsg.RawBody = expiredBody
	if err := handler.Handle(ctx, expiredMsg); err != nil {
		t.Fatalf("expired-reference initial delivery: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_attempts=9,next_reference_attempt_at=now() WHERE provider_id=$1 AND external_transaction_id=$2`, provider, expiredExt); err != nil {
		t.Fatal(err)
	}
	for {
		var failureCode string
		if err := pool.QueryRow(ctx, `SELECT status,COALESCE(failure_code,'') FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, provider, expiredExt).Scan(&refundStatus, &failureCode); err != nil {
			t.Fatal(err)
		}
		if refundStatus == "REJECTED" {
			if failureCode != "REFERENCE_NOT_FOUND" {
				t.Fatalf("expected REFERENCE_NOT_FOUND after retries, got %q", failureCode)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("expired reference did not become REJECTED; last status=%s", refundStatus)
		case <-time.After(100 * time.Millisecond):
		}
	}
	var rejectionEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id=(SELECT id FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2) AND event_type='WagerTransactionRejected'`, provider, expiredExt).Scan(&rejectionEvents); err != nil {
		t.Fatal(err)
	}
	if rejectionEvents != 1 {
		t.Fatalf("expected one rejection event after reference exhaustion, got %d", rejectionEvents)
	}
}
