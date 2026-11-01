package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/postgres"
	"github.com/Big-Lapa/bet-settlement-go/internal/app"
	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

// Integration tests run only when TEST_DATABASE_URL is set
// (CI starts a Postgres service container; locally: make test-integration).
func openDB(t *testing.T) *postgres.DB {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := postgres.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPostgres_ConcurrentSettlementIsExactlyOnce(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	match := fmt.Sprintf("match-%d", time.Now().UnixNano())

	var seq int
	svc := app.NewService(app.Deps{Bets: db, Results: db, Outbox: db, Tx: db, SettledTopic: "bet-settled"},
		app.WithIDGenerator(func() string { seq++; return fmt.Sprintf("%s-bet-%d", match, seq) }))

	for i := range 50 {
		sel := domain.SelectionHome
		if i%2 == 0 {
			sel = domain.SelectionAway
		}
		if _, err := svc.PlaceBet(ctx, app.PlaceBetCmd{
			PlayerID: "p-" + match, MatchID: match, Selection: sel, Stake: 1000, Odds: 2500,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Ten concurrent deliveries of the same result.
	var wg sync.WaitGroup
	results := make(chan int, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := svc.SettleMatch(ctx, domain.MatchResult{MatchID: match, Outcome: domain.SelectionHome})
			if err != nil {
				t.Error(err)
			}
			results <- n
		}()
	}
	wg.Wait()
	close(results)

	total := 0
	for n := range results {
		total += n
	}
	if total != 50 {
		t.Fatalf("settled %d bets in total, want 50", total)
	}

	st, err := svc.PlayerStats(ctx, "p-"+match)
	if err != nil {
		t.Fatal(err)
	}
	// 25 winners * 25.00 payout - 50 * 10.00 stake = 125.00
	if st.Won != 25 || st.Lost != 25 || st.NetResult != 12500 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestPostgres_GetNotFound(t *testing.T) {
	db := openDB(t)
	_, err := db.Get(context.Background(), "missing")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
