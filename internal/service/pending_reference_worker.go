package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/metrics"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
)

const (
	pendingReferenceWorkerInterval = 2 * time.Second
	initialReferenceBackoff        = 5 * time.Second
	maxReferenceBackoff            = 5 * time.Minute
)

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

type PendingReferenceWorker struct {
	repo    *repository.WagerRepository
	metrics *metrics.Metrics
}

func NewPendingReferenceWorker(repo *repository.WagerRepository, m *metrics.Metrics) *PendingReferenceWorker {
	return &PendingReferenceWorker{repo: repo, metrics: m}
}

func (w *PendingReferenceWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(pendingReferenceWorkerInterval)
	defer ticker.Stop()

	w.process(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.process(ctx)
		}
	}
}

func (w *PendingReferenceWorker) process(ctx context.Context) {
	pending, err := w.repo.ClaimPendingReferences(ctx, 20)
	if err != nil {
		slog.Error("pending reference worker claim failed", "error", err)
		return
	}
	for _, p := range pending {
		amount, err := domain.NewMoneyFromMinorUnits(p.AmountMinor, p.Currency)
		if err != nil {
			slog.Error("pending reference has invalid money", "transaction_id", p.ID, "error", err)
			continue
		}
		_, err = w.repo.Process(ctx, repository.WagerInput{
			ProviderID:            p.ProviderID,
			ExternalTransactionID: p.ExternalTransactionID,
			IdempotencyKey:        p.IdempotencyKey,
			PlayerID:              p.PlayerID,
			WalletID:              p.WalletID,
			RoundID:               p.RoundID,
			GameID:                p.GameID,
			Kind:                  p.Kind,
			Money:                 amount,
			ReferenceExternalID:   p.ReferenceExternalID,
		})
		if err != nil {
			if w.metrics != nil {
				w.metrics.Retry()
			}
			slog.Warn("pending reference retry failed", "transaction_id", p.ID, "error", err)
			continue
		}
		slog.Info("pending reference retry processed", "transaction_id", p.ID)
	}
}
