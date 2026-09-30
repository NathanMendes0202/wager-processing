package service

import (
	"testing"
	"time"
)

func TestReferenceBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{5, 80 * time.Second},
		{6, 160 * time.Second},
		{7, 5 * time.Minute},
		{10, 5 * time.Minute},
	}
	for _, tc := range cases {
		if got := referenceBackoff(tc.attempt); got != tc.want {
			t.Fatalf("referenceBackoff(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}
