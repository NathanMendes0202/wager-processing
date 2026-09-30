package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/NathanMendes0202/wager-processing/internal/config"
)

func New(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}

	// Timeout dedicado para o health check/ping inicial
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		if err := pool.Ping(ctx); err == nil {
			break
		} else if ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("database ping timeout: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}
