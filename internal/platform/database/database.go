package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/NathanMendes0202/wager-processing/internal/config"
)

func New(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10_000_000_000)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	for {
		if err := pool.Ping(ctx); err == nil {
			break
		} else if ctx.Err() != nil {
			pool.Close()
			return nil, err
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
