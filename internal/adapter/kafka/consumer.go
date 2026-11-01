package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
	"github.com/Big-Lapa/bet-settlement-go/internal/worker"
)

// ResultHandler is what the consumer needs from the application layer.
type ResultHandler interface {
	SettleMatch(ctx context.Context, r domain.MatchResult) (int, error)
}

// ConsumerConfig configures ResultConsumer.
type ConsumerConfig struct {
	Brokers   []string
	Topic     string
	GroupID   string
	Workers   int           // parallel handlers per batch
	BatchSize int           // max messages per batch
	BatchWait time.Duration // max time to wait for a full batch
}

// ResultConsumer reads match results and settles bets.
//
// Processing model: read a batch, handle it in parallel with a worker pool,
// then commit offsets for the whole batch. Offsets are committed only after
// every message succeeded, so a crash means re-delivery, never loss.
// Re-delivery is safe because SettleMatch is idempotent.
type ResultConsumer struct {
	r       *kafkago.Reader
	cfg     ConsumerConfig
	handler ResultHandler
	log     *slog.Logger
	retry   worker.RetryConfig
}

// NewResultConsumer creates a consumer in a consumer group.
func NewResultConsumer(cfg ConsumerConfig, h ResultHandler, log *slog.Logger) *ResultConsumer {
	return &ResultConsumer{
		r: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:  cfg.Brokers,
			Topic:    cfg.Topic,
			GroupID:  cfg.GroupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		cfg:     cfg,
		handler: h,
		log:     log,
		retry:   worker.RetryConfig{Attempts: 5, Base: 200 * time.Millisecond, Max: 5 * time.Second},
	}
}

// Run consumes until ctx is cancelled. A non-nil error means processing
// can't continue (e.g. DB down after all retries); the process exits and
// the orchestrator restarts it. Uncommitted messages are re-delivered.
func (c *ResultConsumer) Run(ctx context.Context) error {
	for {
		batch, err := c.readBatch(ctx)
		if ctx.Err() != nil {
			return nil // graceful shutdown
		}
		if err != nil {
			return fmt.Errorf("read batch: %w", err)
		}
		if len(batch) == 0 {
			continue
		}

		if err := worker.Process(ctx, batch, c.cfg.Workers, c.handle); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("process batch: %w", err)
		}
		if err := c.r.CommitMessages(ctx, batch...); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit: %w", err)
		}
	}
}

// readBatch collects up to BatchSize messages or whatever arrives in BatchWait.
func (c *ResultConsumer) readBatch(ctx context.Context) ([]kafkago.Message, error) {
	waitCtx, cancel := context.WithTimeout(ctx, c.cfg.BatchWait)
	defer cancel()

	batch := make([]kafkago.Message, 0, c.cfg.BatchSize)
	for len(batch) < c.cfg.BatchSize {
		m, err := c.r.FetchMessage(waitCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				return batch, nil // batch window closed
			}
			return batch, err
		}
		batch = append(batch, m)
	}
	return batch, nil
}

func (c *ResultConsumer) handle(ctx context.Context, m kafkago.Message) error {
	var r domain.MatchResult
	if err := json.Unmarshal(m.Value, &r); err != nil {
		// Poison message: retrying won't help. A real service would send it
		// to a dead-letter topic; here we log it and move on.
		c.log.ErrorContext(ctx, "skip malformed result", "offset", m.Offset, "err", err)
		return nil
	}

	err := worker.Retry(ctx, c.retry, func(ctx context.Context) error {
		n, err := c.handler.SettleMatch(ctx, r)
		if errors.Is(err, domain.ErrValidation) {
			return worker.Permanent(err)
		}
		if err == nil {
			c.log.InfoContext(ctx, "match settled", "match_id", r.MatchID, "bets", n)
		}
		return err
	})
	if errors.Is(err, domain.ErrValidation) {
		c.log.ErrorContext(ctx, "skip invalid result", "match_id", r.MatchID, "err", err)
		return nil
	}
	return err
}

// Close closes the reader (leaves the consumer group).
func (c *ResultConsumer) Close() error { return c.r.Close() }
