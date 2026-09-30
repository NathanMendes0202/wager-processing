package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct{ db *pgxpool.Pool }

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository { return &OutboxRepository{db: db} }

type OutboxEvent struct {
	ID          uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Correlation string
	Causation   string
	OccurredAt  time.Time
	Version     int64
	Payload     json.RawMessage
	Attempts    int
}

func (r *OutboxRepository) InsertTx(ctx context.Context, tx pgx.Tx, event OutboxEvent) error {
	_, err := tx.Exec(ctx, `INSERT INTO outbox_events
        (id,aggregate_id,event_type,correlation_id,causation_id,occurred_at,version,payload)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		event.ID, event.AggregateID, event.EventType, event.Correlation, event.Causation,
		event.OccurredAt, event.Version, event.Payload)
	return err
}

func (r *OutboxRepository) ClaimBatch(ctx context.Context, limit int) ([]OutboxEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
        WITH candidates AS (
            SELECT id FROM outbox_events
            WHERE published_at IS NULL
              AND dead_at IS NULL
              AND next_attempt_at <= now()
              AND (locked_at IS NULL OR locked_at < now() - interval '2 minutes')
            ORDER BY occurred_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE outbox_events o
        SET locked_at = now()
        FROM candidates c
        WHERE o.id = c.id
        RETURNING o.id,o.aggregate_id,o.event_type,COALESCE(o.correlation_id,''),COALESCE(o.causation_id,''),o.occurred_at,o.version,o.payload,o.attempts`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &e.Correlation, &e.Causation, &e.OccurredAt, &e.Version, &e.Payload, &e.Attempts); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id uuid.UUID) error {
	tag, err := r.db.Exec(ctx, `UPDATE outbox_events SET published_at=now(),locked_at=NULL WHERE id=$1 AND published_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox event %s is already published or missing", id)
	}
	return nil
}

const MaxOutboxAttempts = 8

func (r *OutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID, attempts int, reason string) (bool, error) {
	nextAttempt := attempts + 1
	exponent := time.Duration(math.Min(float64(attempts), 8))
	delay := time.Second * time.Duration(1<<uint(exponent))
	if delay > 5*time.Minute || nextAttempt >= MaxOutboxAttempts {
		delay = 5 * time.Minute
	}
	_, err := r.db.Exec(ctx, `UPDATE outbox_events
        SET attempts=$2,next_attempt_at=now()+$3::interval,locked_at=NULL,dead_reason=$4
        WHERE id=$1 AND published_at IS NULL AND dead_at IS NULL`, id, nextAttempt, fmt.Sprintf("%f seconds", delay.Seconds()), reason)
	return nextAttempt >= MaxOutboxAttempts, err
}

func (r *OutboxRepository) MarkDead(ctx context.Context, id uuid.UUID, reason string) error {
	_, err := r.db.Exec(ctx, `UPDATE outbox_events
        SET dead_at=now(),dead_reason=$2,locked_at=NULL
        WHERE id=$1 AND published_at IS NULL AND dead_at IS NULL`, id, reason)
	return err
}
