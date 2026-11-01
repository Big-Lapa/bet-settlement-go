package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/memory"
	"github.com/Big-Lapa/bet-settlement-go/internal/app"
	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

var fixedNow = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func newService(t *testing.T) (*app.Service, *memory.Store) {
	t.Helper()
	store := memory.New()
	var seq atomic.Int64
	svc := app.NewService(app.Deps{
		Bets: store, Results: store, Outbox: store, Tx: store, SettledTopic: "bet-settled",
	},
		app.WithClock(func() time.Time { return fixedNow }),
		app.WithIDGenerator(func() string { return fmt.Sprintf("bet-%d", seq.Add(1)) }),
	)
	return svc, store
}

func place(t *testing.T, svc *app.Service, player string, sel domain.Selection) domain.Bet {
	t.Helper()
	b, err := svc.PlaceBet(context.Background(), app.PlaceBetCmd{
		PlayerID: player, MatchID: "m1", Selection: sel, Stake: 1000, Odds: 2000,
	})
	if err != nil {
		t.Fatalf("place bet: %v", err)
	}
	return b
}

func TestSettleMatch_SettlesAndWritesOutbox(t *testing.T) {
	svc, store := newService(t)
	ctx := context.Background()
	won := place(t, svc, "p1", domain.SelectionHome)
	lost := place(t, svc, "p2", domain.SelectionAway)

	n, err := svc.SettleMatch(ctx, domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionHome})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("settled %d, want 2", n)
	}

	got, _ := svc.GetBet(ctx, won.ID)
	if got.Status != domain.StatusWon || got.Payout != 2000 {
		t.Errorf("winner: got %s/%s", got.Status, got.Payout)
	}
	got, _ = svc.GetBet(ctx, lost.ID)
	if got.Status != domain.StatusLost {
		t.Errorf("loser: got %s", got.Status)
	}

	msgs := store.Outbox()
	if len(msgs) != 2 {
		t.Fatalf("outbox has %d messages, want 2", len(msgs))
	}
	var ev domain.BetSettled
	if err := json.Unmarshal(msgs[0].Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if msgs[0].Topic != "bet-settled" || msgs[0].Key != ev.PlayerID {
		t.Errorf("bad message routing: %+v", msgs[0])
	}
}

func TestSettleMatch_IsIdempotent(t *testing.T) {
	svc, store := newService(t)
	place(t, svc, "p1", domain.SelectionHome)
	r := domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionHome}

	// The same result delivered concurrently many times: settled exactly once.
	var wg sync.WaitGroup
	var total atomic.Int64
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := svc.SettleMatch(context.Background(), r)
			if err != nil {
				t.Error(err)
			}
			total.Add(int64(n))
		}()
	}
	wg.Wait()

	if total.Load() != 1 {
		t.Fatalf("settled %d times, want 1", total.Load())
	}
	if len(store.Outbox()) != 1 {
		t.Fatalf("outbox has %d messages, want 1", len(store.Outbox()))
	}
}

func TestSettleMatch_InvalidResult(t *testing.T) {
	svc, _ := newService(t)
	_, err := svc.SettleMatch(context.Background(), domain.MatchResult{MatchID: "m1", Outcome: "X"})
	if !errors.Is(err, domain.ErrValidation) {
		t.Fatalf("want ErrValidation, got %v", err)
	}
}

// failingOutbox shows how a small interface makes failure injection easy.
type failingOutbox struct{}

func (failingOutbox) Add(context.Context, app.OutboxMessage) error { return errors.New("disk full") }

func TestSettleMatch_RollsBackOnOutboxFailure(t *testing.T) {
	store := memory.New()
	svc := app.NewService(app.Deps{Bets: store, Results: store, Outbox: failingOutbox{}, Tx: store})
	b, _ := svc.PlaceBet(context.Background(), app.PlaceBetCmd{
		PlayerID: "p1", MatchID: "m1", Selection: domain.SelectionHome, Stake: 1000, Odds: 2000,
	})

	_, err := svc.SettleMatch(context.Background(), domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionHome})
	if err == nil {
		t.Fatal("want error")
	}

	got, _ := svc.GetBet(context.Background(), b.ID)
	if got.Status != domain.StatusPending {
		t.Fatalf("bet must stay pending after rollback, got %s", got.Status)
	}
	// The result must not be marked processed either, so a retry can succeed.
	if first, _ := store.MarkProcessed(context.Background(), "m1"); !first {
		t.Fatal("result was marked processed despite rollback")
	}
}

func TestPlayerStats(t *testing.T) {
	svc, _ := newService(t)
	ctx := context.Background()
	place(t, svc, "p1", domain.SelectionHome)
	place(t, svc, "p1", domain.SelectionAway)
	_, _ = svc.SettleMatch(ctx, domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionHome})

	st, err := svc.PlayerStats(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	want := app.PlayerStats{PlayerID: "p1", TotalBets: 2, Won: 1, Lost: 1, TotalStake: 2000, TotalPayout: 2000, NetResult: 0}
	if st != want {
		t.Fatalf("got %+v, want %+v", st, want)
	}
}
