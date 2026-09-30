package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type childResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func main() {
	if os.Getenv("MULTIPROCESS_CHILD") == "1" {
		runChild()
		return
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "TEST_DATABASE_URL is required")
		os.Exit(2)
	}

	if err := runScenario(dsn, "distinct-keys", false); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := runScenario(dsn, "same-idempotency-key", true); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Println("multiprocess concurrency checks passed")
}

func runScenario(dsn, name string, sameKey bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("%s: create pool: %w", name, err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("%s: ping database: %w", name, err)
	}

	walletID := uuid.New()
	playerID := uuid.New()
	if _, err := db.Exec(ctx, `INSERT INTO wallets(id, player_id, currency, balance_minor, version) VALUES($1,$2,'BRL',10000,1)`, walletID, playerID); err != nil {
		return fmt.Errorf("%s: create wallet: %w", name, err)
	}
	defer db.Exec(context.Background(), `DELETE FROM wallets WHERE id=$1`, walletID)

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("%s: executable: %w", name, err)
	}

	keys := []string{"process-1", "process-2", "process-3"}
	if sameKey {
		keys[1] = keys[0]
		keys[2] = keys[0]
	}

	results := make(chan childResult, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.Command(exe)
			cmd.Env = append(os.Environ(),
				"MULTIPROCESS_CHILD=1",
				"TEST_DATABASE_URL="+dsn,
				"TEST_WALLET_ID="+walletID.String(),
				"TEST_PLAYER_ID="+playerID.String(),
				"TEST_IDEMPOTENCY_KEY="+keys[i],
				"TEST_EXTERNAL_ID="+name+"-external-"+strconv.Itoa(i),
			)
			output, err := cmd.Output()
			if err != nil {
				results <- childResult{Error: fmt.Sprintf("child %d: %v: %s", i+1, err, output)}
				return
			}
			var result childResult
			if err := json.Unmarshal(output, &result); err != nil {
				results <- childResult{Error: fmt.Sprintf("child %d invalid output: %v (%s)", i+1, err, output)}
				return
			}
			results <- result
		}(i)
	}
	wg.Wait()
	close(results)

	var processed, rejected, replayed, errors int
	for result := range results {
		if result.Error != "" {
			errors++
			fmt.Fprintln(os.Stderr, result.Error)
			continue
		}
		switch result.Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			rejected++
		case "REPLAY":
			replayed++
		}
	}
	if errors > 0 {
		return fmt.Errorf("%s: child process errors=%d", name, errors)
	}

	var balance int64
	if err := db.QueryRow(ctx, `SELECT balance_minor FROM wallets WHERE id=$1`, walletID).Scan(&balance); err != nil {
		return fmt.Errorf("%s: read balance: %w", name, err)
	}

	if sameKey {
		if processed != 1 || replayed != 2 || rejected != 0 || balance != 2000 {
			return fmt.Errorf("%s: expected processed=1 replay=2 rejected=0 balance=2000, got processed=%d replay=%d rejected=%d balance=%d", name, processed, replayed, rejected, balance)
		}
	} else if processed != 1 || rejected != 2 || replayed != 0 || balance != 2000 {
		return fmt.Errorf("%s: expected processed=1 rejected=2 replay=0 balance=2000, got processed=%d rejected=%d replay=%d balance=%d", name, processed, rejected, replayed, balance)
	}

	fmt.Printf("%s: processed=%d rejected=%d replay=%d balance=%d\n", name, processed, rejected, replayed, balance)
	return nil
}

func runChild() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dsn := os.Getenv("TEST_DATABASE_URL")
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		emit(childResult{Error: err.Error()})
		return
	}
	defer db.Close()

	walletID, err := uuid.Parse(os.Getenv("TEST_WALLET_ID"))
	if err != nil {
		emit(childResult{Error: err.Error()})
		return
	}
	playerID, err := uuid.Parse(os.Getenv("TEST_PLAYER_ID"))
	if err != nil {
		emit(childResult{Error: err.Error()})
		return
	}
	money, err := domain.NewMoney("80.00", "BRL")
	if err != nil {
		emit(childResult{Error: err.Error()})
		return
	}

	repo := repository.NewWagerRepository(db, messaging.NewOutboxRepository(db))
	result, err := repo.Process(ctx, repository.WagerInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: os.Getenv("TEST_EXTERNAL_ID"),
		IdempotencyKey:        os.Getenv("TEST_IDEMPOTENCY_KEY"),
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               "multiprocess-round",
		GameID:                "multiprocess-game",
		Kind:                  "BET",
		Money:                 money,
	})
	if err != nil {
		emit(childResult{Error: err.Error()})
		return
	}

	status := result.Status
	if status == "PROCESSED" && result.Replay {
		status = "REPLAY"
	}
	emit(childResult{Status: status})
}

func emit(result childResult) {
	data, _ := json.Marshal(result)
	fmt.Print(string(data))
	os.Exit(0)
}
