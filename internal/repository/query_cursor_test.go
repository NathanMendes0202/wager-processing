package repository

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLedgerCursorRoundTrip(t *testing.T) {
	id := uuid.New()
	created := time.Date(2026, 9, 29, 1, 23, 45, 123456789, time.UTC)
	cursor := encodeLedgerCursor(created, id)
	gotTime, gotID, err := decodeLedgerCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if !gotTime.Equal(created) {
		t.Fatalf("time mismatch: got %s want %s", gotTime, created)
	}
	if gotID != id {
		t.Fatalf("id mismatch: got %s want %s", gotID, id)
	}
}

func TestLedgerCursorRejectsMalformedValue(t *testing.T) {
	if _, _, err := decodeLedgerCursor("not-a-cursor"); err == nil {
		t.Fatal("expected malformed cursor error")
	}
}
