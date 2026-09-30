//go:build integration

package messaging_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/platform/database"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
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
		VALUES ($1,$2,$3,now(),$4,$5::jsonb)`, eventID, uuid.New(), "TestEvent", 1, `{"ok":true}`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE id=$1`, eventID) })

	r1 := messaging.NewOutboxRepository(pool)
	r2 := messaging.NewOutboxRepository(pool)
	var wg sync.WaitGroup
	results := make(chan int, 2)
	for _, repo := range []*messaging.OutboxRepository{r1, r2} {
		wg.Add(1)
		go func(repo *messaging.OutboxRepository) {
			defer wg.Done()
			events, err := repo.ClaimBatch(ctx, 1)
			if err != nil {
				t.Errorf("claim batch: %v", err)
				return
			}
			results <- len(events)
		}(repo)
	}
	wg.Wait()
	close(results)

	claimed := 0
	for n := range results {
		claimed += n
	}
	if claimed != 1 {
		t.Fatalf("expected exactly one publisher to claim event, got %d", claimed)
	}
}

func TestOutboxMovesToDeadStateAfterMaximumFailures(t *testing.T) {
	pool := integrationPool(t)
	defer pool.Close()

	ctx := context.Background()
	eventID := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO outbox_events
		(id, aggregate_id, event_type, occurred_at, version, payload)
		VALUES ($1,$2,$3,now(),$4,$5::jsonb)`, eventID, uuid.New(), "TestDeadEvent", 1, `{"ok":true}`)
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
	if len(events) != 0 {
		t.Fatalf("dead event must not be claimable, got %d events", len(events))
	}
}

func TestInboxAndFinancialEffectShareOneTransaction(t *testing.T) {
	pool := integrationPool(t)
	defer pool.Close()

	ctx := context.Background()
	playerID := uuid.New()
	walletID := uuid.New()
	messageID := "redelivery-" + uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO wallets(id, player_id, currency, balance_minor, version) VALUES($1,$2,'BRL',10000,1)`, walletID, playerID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DELETE FROM inbox_messages WHERE message_id=$1`, messageID) })

	inbox := messaging.NewInboxRepository(pool)
	wagers := repository.NewWagerRepository(pool, messaging.NewOutboxRepository(pool))
	money, err := domain.NewMoney("80.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	input := repository.WagerInput{
		ProviderID: "provider-a", ExternalTransactionID: "ext-" + messageID, IdempotencyKey: "key-" + messageID,
		PlayerID: playerID, WalletID: walletID, RoundID: "round", GameID: "game", Kind: "BET", Money: money,
	}
	body := `{"messageId":"` + messageID + `"}`

	deliver := func(commit bool) (fresh bool, res repository.WagerResult) {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		fresh, err = inbox.ClaimTx(ctx, tx, messaging.ConsumerName, messageID, body)
		if err != nil {
			t.Fatal(err)
		}
		if fresh {
			if res, err = wagers.ProcessInTx(ctx, tx, input); err != nil {
				t.Fatal(err)
			}
		}
		if commit {
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return fresh, res
	}

	// 1) Crash before commit: neither the inbox row nor the debit may survive.
	deliver(false)
	var inboxRows int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM inbox_messages WHERE message_id=$1`, messageID).Scan(&inboxRows)
	var balance int64
	_ = pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance)
	if inboxRows != 0 || balance != 10000 {
		t.Fatalf("rollback must undo inbox and debit together: inbox=%d balance=%d", inboxRows, balance)
	}

	// 2) Normal delivery commits both.
	if fresh, res := deliver(true); !fresh || res.Status != "PROCESSED" {
		t.Fatalf("first delivery: fresh=%v res=%+v", fresh, res)
	}
	// 3) Redelivery after the commit (crash before DeleteMessage) is a no-op.
	if fresh, _ := deliver(true); fresh {
		t.Fatal("redelivery must be recognised as duplicate by the inbox")
	}
	_ = pool.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance)
	if balance != 2000 {
		t.Fatalf("expected exactly one debit, balance=%d", balance)
	}

	// 4) Same messageId with a different body is a permanent conflict.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := inbox.ClaimTx(ctx, tx, messaging.ConsumerName, messageID, `{"messageId":"`+messageID+`","tampered":true}`); !errors.Is(err, messaging.ErrPermanent) {
		t.Fatalf("expected permanent payload conflict, got %v", err)
	}
}
