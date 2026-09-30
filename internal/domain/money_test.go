package domain

import (
	"errors"
	"testing"
)

func TestNewMoney(t *testing.T) {
	tests := []struct {
		input string
		valid bool
		want  string
	}{
		{"25.00", true, "25.00"},
		{"0.00", true, "0.00"},
		{"25.5", true, "25.50"},
		{"25.000", false, ""},
		{"1e2", false, ""},
		{"NaN", false, ""},
		{"-1.00", false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			m, err := NewMoney(tt.input, "BRL")
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%v err=%v", tt.valid, err)
			}
			if tt.valid && m.String() != tt.want {
				t.Fatalf("got %s want %s", m.String(), tt.want)
			}
		})
	}
}

func TestMoneyCurrency(t *testing.T) {
	brl, _ := NewMoney("10.00", "BRL")
	usd, _ := NewMoney("1.00", "USD")
	_, err := brl.Add(usd)
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("expected currency mismatch, got %v", err)
	}
}
