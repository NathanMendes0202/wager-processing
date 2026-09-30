package repository

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Integration test for the challenge's critical race: two R$80 BETs against
// the same R$100 wallet must produce one success and one insufficient-balance.
// Set TEST_DATABASE_URL to run it; otherwise it is skipped.
func TestConcurrentBetsSingleApproval(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	walletID := uuid.New()
	playerID := uuid.New()
	testRunID := uuid.NewString()
	_, err = db.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance_minor,version) VALUES($1,$2,'BRL',10000,1)`, walletID, playerID)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Exec(context.Background(), `DELETE FROM wallets WHERE id=$1`, walletID)

	outbox := messaging.NewOutboxRepository(db)
	repo := NewWagerRepository(db, outbox)
	money, _ := domain.NewMoney("80.00", "BRL")
	mk := func(key, external string) WagerInput {
		return WagerInput{ProviderID: "provider-a", ExternalTransactionID: testRunID + "-" + external, IdempotencyKey: testRunID + "-" + key, PlayerID: playerID, WalletID: walletID, RoundID: "round", GameID: "game", Kind: "BET", Money: money}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan WagerResult, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			defer wg.Done()
			r, e := repo.Process(ctx, mk(fmt.Sprintf("race-%d", i), fmt.Sprintf("ext-race-%d", i)))
			results <- r
			errs <- e
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)

	processed, rejected := 0, 0
	for r := range results {
		if r.Status == "PROCESSED" {
			processed++
		}
		if r.Status == "REJECTED" {
			rejected++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("expected exactly one processed and one rejected, got processed=%d rejected=%d", processed, rejected)
	}

	var balance int64
	if err := db.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 2000 {
		t.Fatalf("expected final balance 2000 minor units, got %d", balance)
	}
}

func TestSameBet50Times(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	walletID, playerID := uuid.New(), uuid.New()
	testRunID := uuid.NewString()
	if _, err := db.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance_minor,version) VALUES($1,$2,'BRL',10000,1)`, walletID, playerID); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(context.Background(), `DELETE FROM wallets WHERE id=$1`, walletID)
	repo := NewWagerRepository(db, messaging.NewOutboxRepository(db))
	money, _ := domain.NewMoney("80.00", "BRL")
	input := WagerInput{ProviderID: "provider-a", ExternalTransactionID: testRunID + "-same-ext-50", IdempotencyKey: testRunID + "-same-idem-50", PlayerID: playerID, WalletID: walletID, RoundID: "round", GameID: "game", Kind: "BET", Money: money}
	results := make(chan WagerResult, 50)
	errs := make(chan error, 50)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, e := repo.Process(ctx, input); results <- r; errs <- e }()
	}
	wg.Wait()
	close(results)
	close(errs)
	processed, replays := 0, 0
	var txID uuid.UUID
	for r := range results {
		if r.Status == "PROCESSED" && !r.Replay {
			processed++
			txID = r.TransactionID
		}

		if r.Replay {
			replays++
		}
	}
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if processed != 1 || replays != 49 {
		t.Fatalf("expected 1 processed and 49 replays, got processed=%d replays=%d", processed, replays)
	}
	var balance int64
	if err := db.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 2000 {
		t.Fatalf("expected balance 2000, got %d", balance)
	}
	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND transaction_id=$2`, walletID, txID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one ledger debit, got %d", count)
	}
}
