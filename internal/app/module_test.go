package app

import (
	"testing"

	"go.uber.org/fx"
)

func TestModuleIsFxComposable(t *testing.T) {
	if err := fx.ValidateApp(Module()); err != nil {
		t.Fatalf("fx graph error: %v", err)
	}
}
