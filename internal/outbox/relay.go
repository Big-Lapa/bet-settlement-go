// Package outbox publishes messages written by the transactional outbox.
//
// Why an outbox: writing to Postgres and to Kafka are two separate systems,
// there is no shared transaction. If we committed the bet and then crashed
// before producing to Kafka, the event would be lost. Instead the event is
// written to the outbox table in the same DB transaction as the bet, and
// this relay publishes it afterwards. Delivery is at-least-once, so
// consumers must be idempotent (they get bet_id to deduplicate).
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/app"
)

// Store reads unsent messages. FetchUnsent must lock the rows
// (FOR UPDATE SKIP LOCKED) so several relay instances can run in parallel.
type Store interface {
	FetchUnsent(ctx context.Context, limit int) ([]app.OutboxMessage, error)
	MarkSent(ctx context.Context, ids []int64, at time.Time) error
}

// Publisher sends messages to the broker.
type Publisher interface {
	Publish(ctx context.Context, msgs []app.OutboxMessage) error
}

// Relay polls the outbox and publishes new messages.
type Relay struct {
	store     Store
	tx        app.TxManager
	pub       Publisher
	interval  time.Duration
	batchSize int
	log       *slog.Logger
}

// NewRelay creates a relay.
func NewRelay(store Store, tx app.TxManager, pub Publisher, interval time.Duration, batchSize int, log *slog.Logger) *Relay {
	return &Relay{store: store, tx: tx, pub: pub, interval: interval, batchSize: batchSize, log: log}
}

// Run polls until ctx is cancelled. Errors are logged and retried on the
// next tick: the outbox is durable, nothing is lost while Kafka is down.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Drain: keep going while full batches come back.
			for {
				n, err := r.publishBatch(ctx)
				if err != nil {
					r.log.ErrorContext(ctx, "outbox publish failed", "err", err)
					break
				}
				if n < r.batchSize {
					break
				}
			}
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	var n int
	err := r.tx.WithinTx(ctx, func(ctx context.Context) error {
		msgs, err := r.store.FetchUnsent(ctx, r.batchSize)
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}
		if len(msgs) == 0 {
			return nil
		}
		// Publishing inside the transaction keeps rows locked, so another
		// relay instance can't pick the same batch. If MarkSent fails after
		// a successful publish, the batch is sent again (at-least-once).
		if err := r.pub.Publish(ctx, msgs); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		ids := make([]int64, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		if err := r.store.MarkSent(ctx, ids, time.Now()); err != nil {
			return fmt.Errorf("mark sent: %w", err)
		}
		n = len(msgs)
		return nil
	})
	return n, err
}
