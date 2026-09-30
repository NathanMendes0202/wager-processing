package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
)

type WalletService struct {
	repo *repository.WalletRepository
}

func NewWalletService(repo *repository.WalletRepository) *WalletService {
	return &WalletService{repo: repo}
}

func (s *WalletService) Create(ctx context.Context, playerID uuid.UUID, initial domain.Money) (domain.Wallet, error) {
	return s.repo.Create(ctx, repository.CreateWalletInput{PlayerID: playerID, InitialAmount: initial})
}

func (s *WalletService) Get(ctx context.Context, walletID uuid.UUID) (domain.Wallet, error) {
	return s.repo.Get(ctx, walletID)
}
