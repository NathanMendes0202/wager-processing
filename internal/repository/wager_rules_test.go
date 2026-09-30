package repository

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/NathanMendes0202/wager-processing/internal/domain"
)

func money(t *testing.T, amount string) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestValidateAmountPolicyPerKind(t *testing.T) {
	cases := []struct {
		kind   string
		amount string
		ok     bool
	}{
		{"LOSS", "0.00", true},
		{"LOSS", "1.00", false},
		{"BET", "0.00", false},
		{"BET", "25.00", true},
		{"WIN", "0.00", false},
		{"REFUND", "10.00", true},
		{"ROLLBACK", "0.00", false},
	}
	for _, c := range cases {
		err := ValidateAmount(c.kind, money(t, c.amount))
		if (err == nil) != c.ok {
			t.Fatalf("%s %s: ok=%v err=%v", c.kind, c.amount, c.ok, err)
		}
		if err != nil && !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("expected ErrInvalidAmount, got %v", err)
		}
	}
}

func TestOpeningIsNotAnExternalKind(t *testing.T) {
	if ExternalKind("OPENING") {
		t.Fatal("OPENING must be rejected on external entrances")
	}
	for _, k := range []string{"BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		if !ExternalKind(k) {
			t.Fatalf("%s should be external", k)
		}
	}
}

func TestMovementDirection(t *testing.T) {
	bet := &referenceRow{kind: "BET"}
	win := &referenceRow{kind: "WIN"}
	refund := &referenceRow{kind: "REFUND"}
	cases := []struct {
		kind string
		ref  *referenceRow
		want string
	}{
		{"BET", nil, "DEBIT"},
		{"WIN", nil, "CREDIT"},
		{"LOSS", nil, ""},
		{"REFUND", bet, "CREDIT"},
		{"ROLLBACK", bet, "CREDIT"},
		{"ROLLBACK", win, "DEBIT"},
		{"ROLLBACK", refund, "DEBIT"},
	}
	for _, c := range cases {
		if got := movementDirection(c.kind, c.ref); got != c.want {
			t.Fatalf("%s/%v: got %q want %q", c.kind, c.ref, got, c.want)
		}
	}
}

func TestValidateReference(t *testing.T) {
	wallet, player := uuid.New(), uuid.New()
	base := referenceRow{id: uuid.New(), kind: "BET", status: "PROCESSED", amount: 2500, currency: "BRL", walletID: wallet, playerID: player, roundID: "r1"}
	in := WagerInput{Kind: "REFUND", WalletID: wallet, PlayerID: player, RoundID: "r1", Money: money(t, "25.00")}

	if code := validateReference(in, base); code != "" {
		t.Fatalf("expected valid reference, got %s", code)
	}
	other := base
	other.roundID = "r2"
	if code := validateReference(in, other); code != "REFERENCE_MISMATCH" {
		t.Fatalf("got %s", code)
	}
	other = base
	other.amount = 1000
	if code := validateReference(in, other); code != "REFERENCE_AMOUNT_MISMATCH" {
		t.Fatalf("got %s", code)
	}
	other = base
	other.kind = "WIN"
	if code := validateReference(in, other); code != "INVALID_REFERENCE_KIND" {
		t.Fatalf("REFUND of a WIN must be rejected, got %s", code)
	}
	in.Kind = "ROLLBACK"
	if code := validateReference(in, other); code != "" {
		t.Fatalf("ROLLBACK of a WIN is valid, got %s", code)
	}
}
