//go:build integration

package app

import (
	"context"
	"os"
	"testing"
	"time"

	"go.uber.org/fx"
)

func TestModuleStartsAndStopsRealWorkers(t *testing.T) {
	required := []string{"DATABASE_URL", "KEYCLOAK_ISSUER", "KEYCLOAK_DISCOVERY_URL", "OIDC_AUDIENCE", "SQS_QUEUE_URL", "SQS_OUTBOX_QUEUE_URL"}
	for _, key := range required {
		if os.Getenv(key) == "" {
			t.Skip("integration environment missing " + key)
		}
	}
	app := fx.New(Module())
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}
