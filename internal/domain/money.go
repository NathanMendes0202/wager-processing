package domain

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidMoney     = errors.New("invalid money")
	ErrCurrencyMismatch = errors.New("currency mismatch")
)

var iso4217 = map[string]struct{}{
	"AED": {}, "ARS": {}, "AUD": {}, "BRL": {}, "CAD": {}, "CHF": {}, "CLP": {}, "CNY": {}, "COP": {}, "CZK": {}, "DKK": {}, "EGP": {}, "EUR": {}, "GBP": {}, "HKD": {}, "HUF": {}, "IDR": {}, "INR": {}, "JPY": {}, "KRW": {}, "MXN": {}, "MYR": {}, "NGN": {}, "NOK": {}, "NZD": {}, "PEN": {}, "PHP": {}, "PLN": {}, "RON": {}, "RUB": {}, "SAR": {}, "SEK": {}, "SGD": {}, "THB": {}, "TRY": {}, "TWD": {}, "UAH": {}, "USD": {}, "UYU": {}, "VND": {}, "ZAR": {},
}

func validCurrency(c string) bool { _, ok := iso4217[strings.ToUpper(c)]; return ok }

var moneyPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,2})?$`)

type Money struct {
	cents    int64
	currency string
}

func NewMoney(amount, currency string) (Money, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if !validCurrency(currency) || !moneyPattern.MatchString(amount) {
		return Money{}, ErrInvalidMoney
	}

	parts := []byte(amount)
	dot := -1
	for i, c := range parts {
		if c == '.' {
			dot = i
			break
		}
	}

	whole := amount
	fraction := "00"
	if dot >= 0 {
		whole = amount[:dot]
		fraction = amount[dot+1:]
		if len(fraction) == 1 {
			fraction += "0"
		}
	}

	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || w > math.MaxInt64/100 {
		return Money{}, ErrInvalidMoney
	}
	f, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return Money{}, ErrInvalidMoney
	}

	cents := w*100 + f
	if cents < 0 {
		return Money{}, ErrInvalidMoney
	}
	return Money{cents: cents, currency: currency}, nil
}

func NewMoneyFromMinorUnits(minor int64, currency string) (Money, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if !validCurrency(currency) || minor < 0 {
		return Money{}, ErrInvalidMoney
	}
	return Money{cents: minor, currency: currency}, nil
}

func (m Money) MinorUnits() int64 { return m.cents }

func ZeroMoney(currency string) (Money, error) {
	return NewMoney("0.00", currency)
}

func (m Money) Currency() string { return m.currency }
func (m Money) IsZero() bool     { return m.cents == 0 }
func (m Money) IsPositive() bool { return m.cents > 0 }

func (m Money) String() string {
	return fmt.Sprintf("%d.%02d", m.cents/100, m.cents%100)
}

func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if other.cents > math.MaxInt64-m.cents {
		return Money{}, ErrInvalidMoney
	}
	return Money{cents: m.cents + other.cents, currency: m.currency}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}
	if other.cents > 0 && m.cents < math.MinInt64+other.cents {
		return Money{}, ErrInvalidMoney
	}
	return Money{cents: m.cents - other.cents, currency: m.currency}, nil
}

func (m Money) Negate() (Money, error) {
	if m.cents == math.MinInt64 {
		return Money{}, ErrInvalidMoney
	}
	return Money{cents: -m.cents, currency: m.currency}, nil
}

func (m Money) Compare(other Money) (int, error) {
	if m.currency != other.currency {
		return 0, ErrCurrencyMismatch
	}
	if m.cents < other.cents {
		return -1, nil
	}
	if m.cents > other.cents {
		return 1, nil
	}
	return 0, nil
}
