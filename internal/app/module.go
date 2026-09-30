package app

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/NathanMendes0202/wager-processing/internal/auth"
	"github.com/NathanMendes0202/wager-processing/internal/config"
	"github.com/NathanMendes0202/wager-processing/internal/httpapi"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/metrics"
	"github.com/NathanMendes0202/wager-processing/internal/platform/database"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
	"github.com/NathanMendes0202/wager-processing/internal/service"
)

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			func(cfg config.Config) (*auth.Middleware, error) {
				return auth.NewMiddleware(cfg.KeycloakIssuer, cfg.KeycloakDiscoveryURL, cfg.OIDCClientID)
			},
			config.Load,
			metrics.New,
			database.New,
			repository.NewWagerRepository,
			repository.NewWalletRepository,
			service.NewWagerService,
			service.NewWalletService,
			service.NewPendingReferenceWorker,
			messaging.NewInboxRepository,
			messaging.NewOutboxRepository,
			messaging.NewPublisher,
			messaging.NewOutboxPublisher,
			fx.Annotate(
				service.NewWagerMessageHandler,
				fx.As(new(messaging.Handler)),
			),
			messaging.NewConsumer,
			httpapi.NewServer,
		),
		fx.Invoke(
			runMigrations,
			httpapi.RegisterLifecycle,
			registerConsumerLifecycle,
			registerOutboxPublisherLifecycle,
			registerPendingReferenceWorkerLifecycle,
		),
	)
}

func runMigrations(pool *pgxpool.Pool) error {
	return database.Migrate(context.Background(), pool)
}

func registerConsumerLifecycle(lc fx.Lifecycle, consumer *messaging.Consumer) {
	var cancel context.CancelFunc
	var done chan struct{}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			consumerCtx, c := context.WithCancel(context.Background())
			cancel, done = c, make(chan struct{})
			go func() { defer close(done); consumer.Run(consumerCtx) }()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if cancel != nil {
				cancel()
			}
			if done != nil {
				select {
				case <-done:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	})
}

func registerOutboxPublisherLifecycle(lc fx.Lifecycle, publisher *messaging.OutboxPublisher) {
	var cancel context.CancelFunc
	var done chan struct{}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			publisherCtx, c := context.WithCancel(context.Background())
			cancel, done = c, make(chan struct{})
			go func() { defer close(done); publisher.Run(publisherCtx) }()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if cancel != nil {
				cancel()
			}
			if done != nil {
				select {
				case <-done:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	})
}

func registerPendingReferenceWorkerLifecycle(lc fx.Lifecycle, worker *service.PendingReferenceWorker) {
	var cancel context.CancelFunc
	var done chan struct{}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			workerCtx, c := context.WithCancel(context.Background())
			cancel, done = c, make(chan struct{})
			go func() { defer close(done); worker.Run(workerCtx) }()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if cancel != nil {
				cancel()
			}
			if done != nil {
				select {
				case <-done:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	})
}
