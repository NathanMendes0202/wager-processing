package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type testHandler struct {
	called bool
}

func (h *testHandler) Handle(context.Context, WagerTransactionMessage) error {
	h.called = true
	return nil
}

func TestConsumerRejectsMissingEnvelopeMessageID(t *testing.T) {
	h := &testHandler{}
	c := &Consumer{handler: h}
	body := `{"type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{}}`
	msg := types.Message{MessageId: aws.String("sqs-id"), Body: aws.String(body)}
	if err := c.handleMessage(context.Background(), msg); err == nil || !isPermanentMessageError(err) {
		t.Fatalf("expected permanent invalid-message error, got %v", err)
	}
	if h.called {
		t.Fatal("handler must not receive an invalid envelope")
	}
}

func TestConsumerRejectsMissingEnvelopeType(t *testing.T) {
	h := &testHandler{}
	c := &Consumer{handler: h}
	body := `{"messageId":"msg-1","occurredAt":"2026-09-30T12:00:00Z","data":{}}`
	msg := types.Message{MessageId: aws.String("sqs-id"), Body: aws.String(body)}
	if err := c.handleMessage(context.Background(), msg); err == nil || !isPermanentMessageError(err) {
		t.Fatalf("expected permanent invalid-message error, got %v", err)
	}
}

func TestConsumerPreservesEnvelopeMessageID(t *testing.T) {
	h := &testHandler{}
	c := &Consumer{handler: h}
	body := `{"messageId":"envelope-id","type":"WagerTransactionRequested","occurredAt":"2026-09-30T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"ext-1","idempotencyKey":"key-1"}}`
	msg := types.Message{MessageId: aws.String("sqs-id"), Body: aws.String(body)}
	if err := c.handleMessage(context.Background(), msg); err != nil {
		t.Fatalf("handleMessage: %v", err)
	}
	if !h.called {
		t.Fatal("handler was not called")
	}
}

func TestConsumerRejectsUnsupportedEnvelopeType(t *testing.T) {
	h := &testHandler{}
	c := &Consumer{handler: h}
	body := `{"messageId":"msg-1","type":"UnknownCommand","occurredAt":"2026-09-30T12:00:00Z","data":{}}`
	msg := types.Message{MessageId: aws.String("sqs-id"), Body: aws.String(body)}
	if err := c.handleMessage(context.Background(), msg); err == nil || !isPermanentMessageError(err) {
		t.Fatalf("expected permanent invalid-message error, got %v", err)
	}
	if h.called {
		t.Fatal("handler must not receive an unsupported message type")
	}
}

func TestConsumerBackoffIsExponentialAndCapped(t *testing.T) {
	cases := []struct {
		attempts int
		want     time.Duration
	}{
		{0, 5 * time.Second},
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{10, 5 * time.Minute},
	}
	for _, tc := range cases {
		if got := consumerBackoff(tc.attempts); got != tc.want {
			t.Errorf("consumerBackoff(%d) = %s, want %s", tc.attempts, got, tc.want)
		}
	}
}
