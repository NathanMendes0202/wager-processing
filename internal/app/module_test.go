package app

import (
	"testing"

	"go.uber.org/fx"
)

// ValidateApp checks the Fx dependency graph without executing constructors.
// Full startup is covered by the Compose E2E flow because production requires
// PostgreSQL, LocalStack and Keycloak.
func TestModuleIsFxComposable(t *testing.T) {
	if err := fx.ValidateApp(Module()); err != nil {
		t.Fatalf("fx graph error: %v", err)
	}
}
