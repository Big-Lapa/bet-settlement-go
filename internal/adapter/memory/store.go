// Package memory is an in-memory adapter for all storage ports.
// It is used in unit tests and in STORAGE=memory mode (no Postgres needed).
package memory

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/app"
	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

type txKey struct{}

// Store keeps all data in maps guarded by one mutex. A "transaction"
// holds the lock for its whole duration and restores a snapshot on error.
type Store struct {
	mu        sync.Mutex
	bets      map[string]domain.Bet
	processed map[string]struct{}
	outbox    []app.OutboxMessage
	nextID    int64
}

// Compile-time checks that Store satisfies the ports.
var (
	_ app.BetRepository = (*Store)(nil)
	_ app.ResultLog     = (*Store)(nil)
	_ app.OutboxWriter  = (*Store)(nil)
	_ app.TxManager     = (*Store)(nil)
)

// New returns an empty store.
func New() *Store {
	return &Store{bets: map[string]domain.Bet{}, processed: map[string]struct{}{}}
}

// lock takes the mutex unless we are already inside WithinTx.
// It returns the matching unlock function.
func (s *Store) lock(ctx context.Context) func() {
	if ctx.Value(txKey{}) != nil {
		return func() {}
	}
	s.mu.Lock()
	return s.mu.Unlock
}

// WithinTx implements app.TxManager.
func (s *Store) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if ctx.Value(txKey{}) != nil {
		return fn(ctx) // nested call joins the outer transaction
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	bets, processed, outbox, nextID := maps.Clone(s.bets), maps.Clone(s.processed), slices.Clone(s.outbox), s.nextID
	if err := fn(context.WithValue(ctx, txKey{}, true)); err != nil {
		s.bets, s.processed, s.outbox, s.nextID = bets, processed, outbox, nextID // rollback
		return err
	}
	return nil
}

// Create implements app.BetRepository.
func (s *Store) Create(ctx context.Context, b domain.Bet) error {
	defer s.lock(ctx)()
	if _, ok := s.bets[b.ID]; ok {
		return fmt.Errorf("bet %s already exists", b.ID)
	}
	s.bets[b.ID] = b
	return nil
}

// Get implements app.BetRepository.
func (s *Store) Get(ctx context.Context, id string) (domain.Bet, error) {
	defer s.lock(ctx)()
	b, ok := s.bets[id]
	if !ok {
		return domain.Bet{}, fmt.Errorf("bet %s: %w", id, domain.ErrNotFound)
	}
	return b, nil
}

// PendingByMatch implements app.BetRepository.
func (s *Store) PendingByMatch(ctx context.Context, matchID string) ([]domain.Bet, error) {
	defer s.lock(ctx)()
	var out []domain.Bet
	for _, b := range s.bets {
		if b.MatchID == matchID && b.Status == domain.StatusPending {
			out = append(out, b)
		}
	}
	slices.SortFunc(out, func(a, b domain.Bet) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out, nil
}

// Update implements app.BetRepository.
func (s *Store) Update(ctx context.Context, b domain.Bet) error {
	defer s.lock(ctx)()
	if _, ok := s.bets[b.ID]; !ok {
		return fmt.Errorf("bet %s: %w", b.ID, domain.ErrNotFound)
	}
	s.bets[b.ID] = b
	return nil
}

// PlayerStats implements app.BetRepository.
func (s *Store) PlayerStats(ctx context.Context, playerID string) (app.PlayerStats, error) {
	defer s.lock(ctx)()
	st := app.PlayerStats{PlayerID: playerID}
	for _, b := range s.bets {
		if b.PlayerID != playerID {
			continue
		}
		st.TotalBets++
		st.TotalStake += b.Stake
		st.TotalPayout += b.Payout
		switch b.Status {
		case domain.StatusPending:
			st.PendingBets++
		case domain.StatusWon:
			st.Won++
		case domain.StatusLost:
			st.Lost++
		}
		if b.Status != domain.StatusPending {
			st.NetResult += b.Payout - b.Stake
		}
	}
	return st, nil
}

// MarkProcessed implements app.ResultLog.
func (s *Store) MarkProcessed(ctx context.Context, matchID string) (bool, error) {
	defer s.lock(ctx)()
	if _, ok := s.processed[matchID]; ok {
		return false, nil
	}
	s.processed[matchID] = struct{}{}
	return true, nil
}

// Add implements app.OutboxWriter.
func (s *Store) Add(ctx context.Context, msg app.OutboxMessage) error {
	defer s.lock(ctx)()
	s.nextID++
	msg.ID = s.nextID
	s.outbox = append(s.outbox, msg)
	return nil
}

// Outbox returns a copy of all outbox messages (for tests).
func (s *Store) Outbox() []app.OutboxMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.outbox)
}

// FetchUnsent implements outbox.Store: in memory mode messages are
// only logged, never published, so this simply returns nothing.
func (s *Store) FetchUnsent(context.Context, int) ([]app.OutboxMessage, error) { return nil, nil }

// MarkSent implements outbox.Store.
func (s *Store) MarkSent(context.Context, []int64, time.Time) error { return nil }
