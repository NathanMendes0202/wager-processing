package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	appconfig "github.com/NathanMendes0202/wager-processing/internal/config"
	"github.com/NathanMendes0202/wager-processing/internal/metrics"
)

type MoneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type WagerTransactionData struct {
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	IdempotencyKey        string    `json:"idempotencyKey"`
	PlayerID              string    `json:"playerId"`
	WalletID              string    `json:"walletId"`
	RoundID               string    `json:"roundId"`
	GameID                string    `json:"gameId"`
	Kind                  string    `json:"kind"`
	Money                 MoneyData `json:"money"`
	ReferenceExternalID   string    `json:"referenceExternalTransactionId,omitempty"`
}

type WagerTransactionMessage struct {
	MessageID  string               `json:"messageId"`
	Type       string               `json:"type"`
	OccurredAt time.Time            `json:"occurredAt"`
	Data       WagerTransactionData `json:"data"`
	RawBody    string               `json:"-"`
}

type Publisher struct {
	client   *sqs.Client
	queueURL string
}

func NewPublisher(cfg appconfig.Config) (*Publisher, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.AWSRegion), config.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if cfg.AWSEndpoint != "" {
				return aws.Endpoint{URL: cfg.AWSEndpoint, SigningRegion: cfg.AWSRegion, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		}),
	))
	if err != nil {
		return nil, err
	}
	return &Publisher{client: sqs.NewFromConfig(awsCfg), queueURL: cfg.SQSQueueURL}, nil
}

func (p *Publisher) Ready(ctx context.Context) error {
	if p.queueURL == "" {
		return fmt.Errorf("SQS_QUEUE_URL is required")
	}
	_, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(p.queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}

func (p *Publisher) Publish(ctx context.Context, msg WagerTransactionMessage) error {
	if p.queueURL == "" {
		return fmt.Errorf("SQS_QUEUE_URL is required")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(msg.Data.ProviderID),
		MessageDeduplicationId: aws.String(msg.MessageID),
	})
	return err
}

var (
	ErrInvalidMessage   = errors.New("invalid SQS message")
	ErrPermanentMessage = errors.New("permanent SQS message failure")
)

type Consumer struct {
	client   *sqs.Client
	queueURL string
	dlqURL   string
	inbox    *InboxRepository
	handler  Handler
	logger   *slog.Logger
	metrics  *metrics.Metrics
}

const consumerName = "wager-transaction-consumer"

type Handler interface {
	Handle(context.Context, WagerTransactionMessage) error
}

func NewConsumer(cfg appconfig.Config, inbox *InboxRepository, handler Handler, m *metrics.Metrics) (*Consumer, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.AWSRegion), config.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if cfg.AWSEndpoint != "" {
				return aws.Endpoint{URL: cfg.AWSEndpoint, SigningRegion: cfg.AWSRegion, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		}),
	))
	if err != nil {
		return nil, err
	}
	return &Consumer{client: sqs.NewFromConfig(awsCfg), queueURL: cfg.SQSQueueURL, dlqURL: cfg.SQSDeadLetter, inbox: inbox, handler: handler, logger: slog.Default(), metrics: m}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	if c.queueURL == "" {
		return
	}
	for ctx.Err() == nil {
		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(c.queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 10,
			VisibilityTimeout: 30, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("failed to receive SQS messages", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, msg := range out.Messages {
			if ctx.Err() != nil {
				return
			}
			if err := c.handleMessage(ctx, msg); err != nil {
				c.logger.Error("sqs message processing failed", "messageId", aws.ToString(msg.MessageId), "error", err)
				if isPermanentMessageError(err) {
					if dlqErr := c.moveToDLQ(ctx, msg, err); dlqErr != nil {
						c.logger.Error("failed to move invalid message to DLQ", "messageId", aws.ToString(msg.MessageId), "error", dlqErr)
					} else if c.metrics != nil {
						c.metrics.RequestDLQ()
					}
					continue
				}
				if c.metrics != nil {
					c.metrics.Retry()
				}
				if backoffErr := c.backoffMessage(ctx, msg); backoffErr != nil && ctx.Err() == nil {
					c.logger.Error("failed to update SQS message visibility for retry", "messageId", aws.ToString(msg.MessageId), "error", backoffErr)
				}
				continue
			}
			if msg.ReceiptHandle != nil {
				if _, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: msg.ReceiptHandle}); err != nil {
					c.logger.Error("failed to delete processed SQS message", "messageId", aws.ToString(msg.MessageId), "error", err)
				}
			}
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg types.Message) error {
	if msg.MessageId == nil || msg.Body == nil || *msg.Body == "" {
		return fmt.Errorf("%w: messageId and body are required", ErrInvalidMessage)
	}
	var event WagerTransactionMessage
	if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
		return fmt.Errorf("%w: invalid JSON: %v", ErrInvalidMessage, err)
	}
	if event.MessageID == "" {
		return fmt.Errorf("%w: messageId is required", ErrInvalidMessage)
	}
	if event.Type == "" {
		return fmt.Errorf("%w: type is required", ErrInvalidMessage)
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurredAt is required", ErrInvalidMessage)
	}
	event.RawBody = *msg.Body
	return c.handler.Handle(ctx, event)
}

func isPermanentMessageError(err error) bool {
	return errors.Is(err, ErrInvalidMessage) || errors.Is(err, ErrPermanentMessage)
}

func (c *Consumer) backoffMessage(ctx context.Context, msg types.Message) error {
	if msg.ReceiptHandle == nil {
		return nil
	}
	attempts := 1
	if countStr, ok := msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		if parsed, err := strconv.Atoi(countStr); err == nil && parsed > 0 {
			attempts = parsed
		}
	}
	delay := 5 * time.Second
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= 5*time.Minute {
			delay = 5 * time.Minute
			break
		}
	}
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queueURL), ReceiptHandle: msg.ReceiptHandle, VisibilityTimeout: int32(delay / time.Second),
	})
	return err
}

func consumerBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := 5 * time.Second
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= 5*time.Minute {
			return 5 * time.Minute
		}
	}
	return delay
}

func (c *Consumer) moveToDLQ(ctx context.Context, msg types.Message, cause error) error {
	if c.dlqURL == "" || msg.Body == nil {
		return cause
	}
	body := *msg.Body
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(c.dlqURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String("invalid-messages"), MessageDeduplicationId: msg.MessageId,
	})
	if err != nil {
		return err
	}
	if msg.ReceiptHandle != nil {
		_, err = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.queueURL), ReceiptHandle: msg.ReceiptHandle})
	}
	return err
}

// OutboxPublisher publishes durable outbox events. It is intentionally separate
// from the request consumer: the outbox contains facts produced by committed
// database transactions, while the request queue carries commands.
type OutboxPublisher struct {
	client   *sqs.Client
	queueURL string
	dlqURL   string
	outbox   *OutboxRepository
	metrics  *metrics.Metrics
}

type OutboxEventEnvelope struct {
	EventID     string          `json:"eventId"`
	EventType   string          `json:"eventType"`
	AggregateID string          `json:"aggregateId"`
	Version     int64           `json:"version"`
	OccurredAt  time.Time       `json:"occurredAt"`
	Correlation string          `json:"correlationId"`
	Causation   string          `json:"causationId"`
	Data        json.RawMessage `json:"data"`
}

func NewOutboxPublisher(cfg appconfig.Config, outbox *OutboxRepository, m *metrics.Metrics) (*OutboxPublisher, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.AWSRegion), config.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if cfg.AWSEndpoint != "" {
				return aws.Endpoint{URL: cfg.AWSEndpoint, SigningRegion: cfg.AWSRegion, HostnameImmutable: true}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		}),
	))
	if err != nil {
		return nil, err
	}
	return &OutboxPublisher{client: sqs.NewFromConfig(awsCfg), queueURL: cfg.SQSOutboxURL, dlqURL: cfg.SQSOutboxDLQURL, outbox: outbox, metrics: m}, nil
}

func (p *OutboxPublisher) Run(ctx context.Context) {
	if p.queueURL == "" {
		return
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := p.publishBatch(ctx); err != nil && ctx.Err() == nil {
			time.Sleep(time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *OutboxPublisher) publishBatch(ctx context.Context) error {
	events, err := p.outbox.ClaimBatch(ctx, 20)
	if err != nil {
		return err
	}
	for _, event := range events {
		if p.metrics != nil {
			p.metrics.OutboxLag(time.Since(event.OccurredAt))
		}
		envelope := OutboxEventEnvelope{EventID: event.ID.String(), EventType: event.EventType, AggregateID: event.AggregateID.String(), Version: event.Version, OccurredAt: event.OccurredAt, Correlation: event.Correlation, Causation: event.Causation, Data: event.Payload}
		body, err := json.Marshal(envelope)
		if err != nil {
			_, _ = p.outbox.MarkFailed(ctx, event.ID, event.Attempts, err.Error())
			continue
		}
		_, err = p.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(string(body)), MessageGroupId: aws.String(event.AggregateID.String()), MessageDeduplicationId: aws.String(event.ID.String())})
		if err != nil {
			if p.metrics != nil {
				p.metrics.Retry()
			}
			dead, markErr := p.outbox.MarkFailed(ctx, event.ID, event.Attempts, err.Error())
			if markErr == nil && dead {
				if p.dlqURL == "" {
					_ = p.outbox.MarkDead(ctx, event.ID, "outbox max attempts reached: "+err.Error())
					continue
				}
				_, dlqErr := p.client.SendMessage(ctx, &sqs.SendMessageInput{
					QueueUrl:               aws.String(p.dlqURL),
					MessageBody:            aws.String(string(body)),
					MessageGroupId:         aws.String("transaction-group"),
					MessageDeduplicationId: aws.String(event.ID.String()),
				})
				if dlqErr == nil {
					if p.metrics != nil {
						p.metrics.DLQ()
					}
					_ = p.outbox.MarkDead(ctx, event.ID, "outbox max attempts reached: "+err.Error())
				}
			}
			continue
		}
		_ = p.outbox.MarkPublished(ctx, event.ID)
	}
	return nil
}
