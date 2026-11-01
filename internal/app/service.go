package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

// Service implements the use cases. Dependencies come in through the
// constructor (no globals, no DI container) so tests can pass fakes.
type Service struct {
	bets         BetRepository
	results      ResultLog
	outbox       OutboxWriter
	tx           TxManager
	settledTopic string
	now          func() time.Time
	newID        func() string
	log          *slog.Logger
}

// Deps groups the dependencies of Service.
type Deps struct {
	Bets         BetRepository
	Results      ResultLog
	Outbox       OutboxWriter
	Tx           TxManager
	SettledTopic string
	Logger       *slog.Logger
}

// Option customizes Service (functional options pattern).
type Option func(*Service)

// WithClock replaces time.Now, useful in tests.
func WithClock(now func() time.Time) Option { return func(s *Service) { s.now = now } }

// WithIDGenerator replaces the random ID generator, useful in tests.
func WithIDGenerator(f func() string) Option { return func(s *Service) { s.newID = f } }

// NewService wires the use cases.
func NewService(d Deps, opts ...Option) *Service {
	s := &Service{
		bets:         d.Bets,
		results:      d.Results,
		outbox:       d.Outbox,
		tx:           d.Tx,
		settledTopic: d.SettledTopic,
		now:          time.Now,
		newID:        randomID,
		log:          d.Logger,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// PlaceBetCmd is the input of PlaceBet.
type PlaceBetCmd struct {
	PlayerID  string
	MatchID   string
	Selection domain.Selection
	Stake     domain.Money
	Odds      domain.Odds
}

// PlaceBet validates and stores a new bet.
func (s *Service) PlaceBet(ctx context.Context, cmd PlaceBetCmd) (domain.Bet, error) {
	bet, err := domain.NewBet(domain.NewBetParams{
		ID:        s.newID(),
		PlayerID:  cmd.PlayerID,
		MatchID:   cmd.MatchID,
		Selection: cmd.Selection,
		Stake:     cmd.Stake,
		Odds:      cmd.Odds,
		Now:       s.now(),
	})
	if err != nil {
		return domain.Bet{}, err
	}
	if err := s.bets.Create(ctx, bet); err != nil {
		return domain.Bet{}, fmt.Errorf("create bet: %w", err)
	}
	return bet, nil
}

// GetBet returns a bet by ID.
func (s *Service) GetBet(ctx context.Context, id string) (domain.Bet, error) {
	return s.bets.Get(ctx, id)
}

// PlayerStats returns aggregated statistics for a player.
func (s *Service) PlayerStats(ctx context.Context, playerID string) (PlayerStats, error) {
	return s.bets.PlayerStats(ctx, playerID)
}

// SettleMatch settles all pending bets of a match in one transaction:
// bet updates and outbox messages commit together or not at all.
// It is idempotent: a repeated result for the same match is a no-op.
func (s *Service) SettleMatch(ctx context.Context, r domain.MatchResult) (settled int, err error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}

	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := s.results.MarkProcessed(ctx, r.MatchID)
		if err != nil {
			return fmt.Errorf("mark processed: %w", err)
		}
		if !first {
			s.log.InfoContext(ctx, "duplicate result skipped", "match_id", r.MatchID)
			return nil
		}

		bets, err := s.bets.PendingByMatch(ctx, r.MatchID)
		if err != nil {
			return fmt.Errorf("load pending bets: %w", err)
		}

		now := s.now()
		for i := range bets {
			bet := &bets[i]
			if err := bet.Settle(r, now); err != nil {
				return fmt.Errorf("settle bet %s: %w", bet.ID, err)
			}
			if err := s.bets.Update(ctx, *bet); err != nil {
				return fmt.Errorf("update bet %s: %w", bet.ID, err)
			}
			if err := s.enqueueSettled(ctx, *bet); err != nil {
				return err
			}
		}
		settled = len(bets)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return settled, nil
}

func (s *Service) enqueueSettled(ctx context.Context, b domain.Bet) error {
	payload, err := json.Marshal(domain.BetSettled{
		BetID:     b.ID,
		PlayerID:  b.PlayerID,
		MatchID:   b.MatchID,
		Status:    b.Status,
		Payout:    b.Payout,
		SettledAt: b.SettledAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	// Key = player ID: all events of one player land in one partition, in order.
	msg := OutboxMessage{Topic: s.settledTopic, Key: b.PlayerID, Payload: payload}
	if err := s.outbox.Add(ctx, msg); err != nil {
		return fmt.Errorf("outbox add: %w", err)
	}
	return nil
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails on supported platforms
	return hex.EncodeToString(b[:])
}
