//go:build integration

package integrationtest

import (
	"context"
	"encoding/json"
	"os"
	"sync"
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

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := database.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func TestOutboxClaimIsExclusiveAcrossPublishers(t *testing.T) {
	pool := integrationPool(t)
	defer pool.Close()

	ctx := context.Background()
	eventID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO outbox_events
		(id, aggregate_id, event_type, occurred_at, version, payload)
		VALUES ($1,$2,$3,'1900-01-01T00:00:00Z',$4,$5::jsonb)`, eventID, uuid.New(), "TestEvent", 1, `{"ok":true}`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE id=$1`, eventID) })

	r1 := messaging.NewOutboxRepository(pool)
	r2 := messaging.NewOutboxRepository(pool)
	var wg sync.WaitGroup
	results := make(chan []uuid.UUID, 2)
	for _, repo := range []*messaging.OutboxRepository{r1, r2} {
		wg.Add(1)
		go func(repo *messaging.OutboxRepository) {
			defer wg.Done()
			events, err := repo.ClaimBatch(ctx, 1)
			if err != nil {
				t.Errorf("claim batch: %v", err)
				return
			}
			ids := make([]uuid.UUID, 0, len(events))
			for _, event := range events {
				ids = append(ids, event.ID)
			}
			results <- ids
		}(repo)
	}
	wg.Wait()
	close(results)

	targetClaims := 0
	for ids := range results {
		for _, id := range ids {
			if id == eventID {
				targetClaims++
			}
		}
	}
	if targetClaims != 1 {
		t.Fatalf("expected the target event to be claimed by exactly one publisher, got %d", targetClaims)
	}
}

func TestOutboxMovesToDeadStateAfterMaximumFailures(t *testing.T) {
	pool := integrationPool(t)
	defer pool.Close()

	ctx := context.Background()
	eventID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO outbox_events
		(id, aggregate_id, event_type, occurred_at, version, payload)
		VALUES ($1,$2,$3,'1900-01-01T00:00:00Z',$4,$5::jsonb)`, eventID, uuid.New(), "TestDeadEvent", 1, `{"ok":true}`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE id=$1`, eventID) })

	repo := messaging.NewOutboxRepository(pool)
	for attempt := 0; attempt < messaging.MaxOutboxAttempts; attempt++ {
		dead, err := repo.MarkFailed(ctx, eventID, attempt, "simulated publisher failure")
		if err != nil {
			t.Fatal(err)
		}
		if attempt < messaging.MaxOutboxAttempts-1 && dead {
			t.Fatalf("event became dead too early at attempt %d", attempt+1)
		}
		if attempt == messaging.MaxOutboxAttempts-1 && !dead {
			t.Fatal("expected max-attempt signal")
		}
	}

	if err := repo.MarkDead(ctx, eventID, "simulated publisher failure"); err != nil {
		t.Fatal(err)
	}

	var deadAt *time.Time
	var attempts int
	var reason *string
	err = pool.QueryRow(ctx, `SELECT dead_at, attempts, dead_reason FROM outbox_events WHERE id=$1`, eventID).Scan(&deadAt, &attempts, &reason)
	if err != nil {
		t.Fatal(err)
	}
	if deadAt == nil || attempts != messaging.MaxOutboxAttempts || reason == nil || *reason == "" {
		t.Fatalf("unexpected dead-letter state: deadAt=%v attempts=%d reason=%v", deadAt, attempts, reason)
	}

	events, err := repo.ClaimBatch(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.ID == eventID {
			t.Fatal("dead event must not be claimable")
		}
	}
}

func TestInboxRedeliveryAfterFinancialCommitIsIdempotent(t *testing.T) {
	pool := integrationPool(t)
	defer pool.Close()

	ctx := context.Background()
	playerID, walletID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO wallets (id, player_id, currency, balance_minor, version) VALUES ($1,$2,'BRL',10000,1)`, walletID, playerID)
	if err != nil {
		t.Fatal(err)
	}

	inbox := messaging.NewInboxRepository(pool)
	outbox := messaging.NewOutboxRepository(pool)
	wagers := service.NewWagerService(repository.NewWagerRepository(pool, outbox), metrics.New())
	handler := service.NewWagerMessageHandler(wagers, pool, inbox)
	messageID := "atomic-redelivery-" + uuid.NewString()
	body := `{"messageId":"` + messageID + `","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"` + uuid.NewString() + `","idempotencyKey":"` + uuid.NewString() + `","playerId":"` + playerID.String() + `","walletId":"` + walletID.String() + `","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}}`
	var msg messaging.WagerTransactionMessage
	if err := json.Unmarshal([]byte(body), &msg); err != nil {
		t.Fatal(err)
	}
	msg.RawBody = body

	if err := handler.Handle(ctx, msg); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if err := handler.Handle(ctx, msg); err != nil {
		t.Fatalf("redelivery: %v", err)
	}

	var balance int64
	if err := pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 2000 {
		t.Fatalf("expected one debit after redelivery, got balance=%d", balance)
	}
	var completed bool
	if err := pool.QueryRow(ctx, `SELECT completed_at IS NOT NULL FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2`, "wager-transaction-consumer", messageID).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("inbox completion must be committed with the financial operation")
	}
	var ledgerEntries int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, walletID).Scan(&ledgerEntries); err != nil {
		t.Fatal(err)
	}
	if ledgerEntries != 1 {
		t.Fatalf("expected exactly one BET ledger entry for the directly inserted wallet, got %d", ledgerEntries)
	}
}
