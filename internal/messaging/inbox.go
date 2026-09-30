package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InboxRepository struct{ db *pgxpool.Pool }

func NewInboxRepository(db *pgxpool.Pool) *InboxRepository { return &InboxRepository{db: db} }

// Claim returns true when the message must be processed. An existing incomplete
// message is deliberately claimable again so a crash after the business
// transaction but before Complete does not permanently lose the event.
func (r *InboxRepository) Claim(ctx context.Context, consumer, messageID, payload string) (bool, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	claimed, err := r.ClaimTx(ctx, tx, consumer, messageID, payload)
	if err != nil {
		_ = tx.Rollback(ctx)
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return claimed, nil
}

func (r *InboxRepository) ClaimTx(ctx context.Context, tx pgx.Tx, consumer, messageID, payload string) (bool, error) {
	sum := sha256.Sum256([]byte(payload))
	hash := hex.EncodeToString(sum[:])

	var storedHash string
	var completedAt interface{}
	err := tx.QueryRow(ctx, `
        INSERT INTO inbox_messages (consumer_name,message_id,payload_hash)
        VALUES ($1,$2,$3)
        ON CONFLICT (consumer_name,message_id) DO UPDATE
          SET payload_hash=inbox_messages.payload_hash
        RETURNING payload_hash, completed_at`, consumer, messageID, hash).Scan(&storedHash, &completedAt)
	if err != nil {
		return false, err
	}
	if storedHash != hash {
		return false, fmt.Errorf("inbox message payload conflict")
	}
	return completedAt == nil, nil
}

func (r *InboxRepository) Complete(ctx context.Context, consumer, messageID string) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	if err := r.CompleteTx(ctx, tx, consumer, messageID); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (r *InboxRepository) CompleteTx(ctx context.Context, tx pgx.Tx, consumer, messageID string) error {
	tag, err := tx.Exec(ctx, `UPDATE inbox_messages SET completed_at=now() WHERE consumer_name=$1 AND message_id=$2 AND completed_at IS NULL`, consumer, messageID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
