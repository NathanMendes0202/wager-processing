package service

import (
	"context"
	"errors"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/metrics"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"
)

type WagerService struct {
	repo    *repository.WagerRepository
	metrics *metrics.Metrics
}

func NewWagerService(repo *repository.WagerRepository, m *metrics.Metrics) *WagerService {
	return &WagerService{repo: repo, metrics: m}
}

type WagerCommand struct {
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              uuid.UUID
	WalletID              uuid.UUID
	RoundID               string
	GameID                string
	Kind                  string
	Amount                domain.Money
	ReferenceExternalID   string
}
type BetCommand = WagerCommand

func (s *WagerService) Process(ctx context.Context, cmd WagerCommand) (repository.WagerResult, error) {
	start := time.Now()
	result, err := s.repo.Process(ctx, toRepositoryInput(cmd))
	s.recordMetrics(start, result)
	if err != nil {
		s.recordErrorMetrics(err)
		return result, err
	}
	if result.Status == "PENDING_REFERENCE" {
		return result, ErrPendingReference
	}
	return result, nil
}

// ProcessTx executes the financial operation inside a caller-owned PostgreSQL
// transaction. It is used by the SQS consumer so Inbox, wallet, ledger and
// Outbox commit atomically.
func (s *WagerService) ProcessTx(ctx context.Context, tx pgx.Tx, cmd WagerCommand) (repository.WagerResult, error) {
	start := time.Now()
	result, err := s.repo.ProcessTx(ctx, tx, toRepositoryInput(cmd))
	s.recordMetrics(start, result)
	if err != nil {
		s.recordErrorMetrics(err)
		return result, err
	}
	return result, nil
}

func toRepositoryInput(cmd WagerCommand) repository.WagerInput {
	return repository.WagerInput{ProviderID: cmd.ProviderID, ExternalTransactionID: cmd.ExternalTransactionID, IdempotencyKey: cmd.IdempotencyKey, PlayerID: cmd.PlayerID, WalletID: cmd.WalletID, RoundID: cmd.RoundID, GameID: cmd.GameID, Kind: cmd.Kind, Money: cmd.Amount, ReferenceExternalID: cmd.ReferenceExternalID}
}

func (s *WagerService) recordErrorMetrics(err error) {
	if s.metrics == nil || err == nil {
		return
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
		s.metrics.ConcurrencyConflict()
	}
}

func (s *WagerService) recordMetrics(start time.Time, result repository.WagerResult) {
	if s.metrics == nil {
		return
	}
	s.metrics.ProcessingDuration(time.Since(start))
	s.metrics.Transaction(result.Status)
	if result.Replay {
		s.metrics.Duplicate()
	}
}
func (s *WagerService) Bet(ctx context.Context, cmd BetCommand) (repository.WagerResult, error) {
	cmd.Kind = "BET"
	return s.Process(ctx, cmd)
}
