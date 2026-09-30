package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPAddr             string
	DatabaseURL          string
	AWSEndpoint          string
	AWSRegion            string
	SQSQueueURL          string
	SQSDeadLetter        string
	SQSOutboxURL         string
	SQSOutboxDLQURL      string
	KeycloakIssuer       string
	KeycloakDiscoveryURL string
	ClientID             string
	ClientSecret         string
	OIDCClientID         string
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:             getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		AWSEndpoint:          os.Getenv("AWS_ENDPOINT"),
		AWSRegion:            getenv("AWS_REGION", "us-east-1"),
		SQSQueueURL:          os.Getenv("SQS_QUEUE_URL"),
		SQSDeadLetter:        os.Getenv("SQS_DLQ_URL"),
		SQSOutboxURL:         os.Getenv("SQS_OUTBOX_QUEUE_URL"),
		SQSOutboxDLQURL:      os.Getenv("SQS_OUTBOX_DLQ_URL"),
		KeycloakIssuer:       os.Getenv("KEYCLOAK_ISSUER"),
		KeycloakDiscoveryURL: os.Getenv("KEYCLOAK_DISCOVERY_URL"),
		ClientID:             os.Getenv("KEYCLOAK_CLIENT_ID"),
		ClientSecret:         os.Getenv("KEYCLOAK_CLIENT_SECRET"),
		OIDCClientID:         getenv("OIDC_AUDIENCE", "wager-api"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
