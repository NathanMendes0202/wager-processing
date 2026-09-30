package metrics

import (
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type Metrics struct {
	transactions              [6]atomic.Uint64
	duplicates                atomic.Uint64
	retries                   atomic.Uint64
	dlq                       atomic.Uint64
	concurrencyConflicts      atomic.Uint64
	reconciliationDifferences atomic.Uint64
	processingNanos           atomic.Uint64
	processingCount           atomic.Uint64
	processingBuckets         [8]atomic.Uint64
	outboxLagNanos            atomic.Uint64
	outboxLagCount            atomic.Uint64
	outboxLagBuckets          [8]atomic.Uint64
	requestDLQ                atomic.Uint64
}

func New() *Metrics { return &Metrics{} }
func (m *Metrics) Transaction(status string) {
	idx := map[string]int{"PROCESSED": 0, "REJECTED": 1, "PENDING_REFERENCE": 2, "FAILED": 3, "QUEUED": 4, "OTHER": 5}[status]
	m.transactions[idx].Add(1)
}
func (m *Metrics) Duplicate()                { m.duplicates.Add(1) }
func (m *Metrics) Retry()                    { m.retries.Add(1) }
func (m *Metrics) DLQ()                      { m.dlq.Add(1) }
func (m *Metrics) RequestDLQ()               { m.requestDLQ.Add(1) }
func (m *Metrics) ConcurrencyConflict()      { m.concurrencyConflicts.Add(1) }
func (m *Metrics) ReconciliationDifference() { m.reconciliationDifferences.Add(1) }

var durationBuckets = [...]time.Duration{100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}

func observeBuckets(b *[8]atomic.Uint64, d time.Duration) {
	for i, bound := range durationBuckets {
		if d <= bound {
			b[i].Add(1)
		}
	}
}

func (m *Metrics) ProcessingDuration(d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.processingNanos.Add(uint64(d))
	m.processingCount.Add(1)
	observeBuckets(&m.processingBuckets, d)
}
func (m *Metrics) OutboxLag(d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.outboxLagNanos.Add(uint64(d))
	m.outboxLagCount.Add(1)
	observeBuckets(&m.outboxLagBuckets, d)
}
func avg(n, c *atomic.Uint64) float64 {
	count := c.Load()
	if count == 0 {
		return 0
	}
	return float64(n.Load()) / float64(count) / 1e9
}
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "# TYPE wager_transactions_total counter")
		for _, x := range []struct {
			k string
			i int
		}{{"processed", 0}, {"rejected", 1}, {"pending_reference", 2}, {"failed", 3}, {"queued", 4}, {"other", 5}} {
			fmt.Fprintf(w, "wager_transactions_total{status=%q} %d\n", x.k, m.transactions[x.i].Load())
		}
		fmt.Fprintf(w, "wager_idempotency_replays_total %d\n", m.duplicates.Load())
		fmt.Fprintf(w, "wager_retries_total %d\n", m.retries.Load())
		fmt.Fprintf(w, "wager_dlq_total %d\n", m.dlq.Load())
		fmt.Fprintf(w, "wager_request_dlq_total %d\n", m.requestDLQ.Load())
		fmt.Fprintf(w, "wager_concurrency_conflicts_total %d\n", m.concurrencyConflicts.Load())
		fmt.Fprintf(w, "wager_reconciliation_differences_total %d\n", m.reconciliationDifferences.Load())
		fmt.Fprintln(w, "# TYPE wager_processing_duration_seconds histogram")
		for i, bound := range durationBuckets {
			fmt.Fprintf(w, "wager_processing_duration_seconds_bucket{le=%q} %d\n", strconv.FormatFloat(bound.Seconds(), 'f', 3, 64), m.processingBuckets[i].Load())
		}
		fmt.Fprintf(w, "wager_processing_duration_seconds_bucket{le=\"+Inf\"} %d\n", m.processingCount.Load())
		fmt.Fprintf(w, "wager_processing_duration_seconds_count %d\n", m.processingCount.Load())
		fmt.Fprintf(w, "wager_processing_duration_seconds_sum %s\n", strconv.FormatFloat(float64(m.processingNanos.Load())/1e9, 'f', 6, 64))
		fmt.Fprintf(w, "wager_processing_duration_seconds_avg %s\n", strconv.FormatFloat(avg(&m.processingNanos, &m.processingCount), 'f', 6, 64))
		fmt.Fprintln(w, "# TYPE wager_outbox_lag_seconds histogram")
		for i, bound := range durationBuckets {
			fmt.Fprintf(w, "wager_outbox_lag_seconds_bucket{le=%q} %d\n", strconv.FormatFloat(bound.Seconds(), 'f', 3, 64), m.outboxLagBuckets[i].Load())
		}
		fmt.Fprintf(w, "wager_outbox_lag_seconds_bucket{le=\"+Inf\"} %d\n", m.outboxLagCount.Load())
		fmt.Fprintf(w, "wager_outbox_lag_seconds_count %d\n", m.outboxLagCount.Load())
		fmt.Fprintf(w, "wager_outbox_lag_seconds_sum %s\n", strconv.FormatFloat(float64(m.outboxLagNanos.Load())/1e9, 'f', 6, 64))
		fmt.Fprintf(w, "wager_outbox_lag_seconds_avg %s\n", strconv.FormatFloat(avg(&m.outboxLagNanos, &m.outboxLagCount), 'f', 6, 64))
	})
}
