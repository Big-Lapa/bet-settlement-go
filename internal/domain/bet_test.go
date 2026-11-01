package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

var now = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func validParams() domain.NewBetParams {
	return domain.NewBetParams{
		ID: "b1", PlayerID: "p1", MatchID: "m1",
		Selection: domain.SelectionHome, Stake: 1000, Odds: 1850, Now: now,
	}
}

// Table-driven test: one loop, many cases, each run as a named subtest.
func TestNewBet_Validation(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(p *domain.NewBetParams)
		wantField string // empty means "no error"
	}{
		{name: "valid", mutate: func(p *domain.NewBetParams) {}},
		{name: "empty player", mutate: func(p *domain.NewBetParams) { p.PlayerID = " " }, wantField: "player_id"},
		{name: "empty match", mutate: func(p *domain.NewBetParams) { p.MatchID = "" }, wantField: "match_id"},
		{name: "bad selection", mutate: func(p *domain.NewBetParams) { p.Selection = "X" }, wantField: "selection"},
		{name: "stake too low", mutate: func(p *domain.NewBetParams) { p.Stake = domain.MinStake - 1 }, wantField: "stake"},
		{name: "stake too high", mutate: func(p *domain.NewBetParams) { p.Stake = domain.MaxStake + 1 }, wantField: "stake"},
		{name: "odds too low", mutate: func(p *domain.NewBetParams) { p.Odds = 1000 }, wantField: "odds"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := validParams()
			tt.mutate(&p)

			_, err := domain.NewBet(p)

			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, domain.ErrValidation) {
				t.Fatalf("want ErrValidation, got %v", err)
			}
			var ve *domain.ValidationError
			if !errors.As(err, &ve) || ve.Field != tt.wantField {
				t.Fatalf("want field %q, got %v", tt.wantField, err)
			}
		})
	}
}

func TestBet_Settle(t *testing.T) {
	tests := []struct {
		name       string
		result     domain.MatchResult
		wantStatus domain.BetStatus
		wantPayout domain.Money
	}{
		{"won", domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionHome}, domain.StatusWon, 1850},
		{"lost", domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionAway}, domain.StatusLost, 0},
		{"void", domain.MatchResult{MatchID: "m1", Cancelled: true}, domain.StatusVoid, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bet, err := domain.NewBet(validParams())
			if err != nil {
				t.Fatal(err)
			}

			if err := bet.Settle(tt.result, now); err != nil {
				t.Fatalf("settle: %v", err)
			}

			if bet.Status != tt.wantStatus || bet.Payout != tt.wantPayout {
				t.Fatalf("got %s/%s, want %s/%s", bet.Status, bet.Payout, tt.wantStatus, tt.wantPayout)
			}
			if bet.SettledAt == nil {
				t.Fatal("SettledAt not set")
			}
		})
	}
}

func TestBet_SettleTwice(t *testing.T) {
	bet, _ := domain.NewBet(validParams())
	r := domain.MatchResult{MatchID: "m1", Outcome: domain.SelectionHome}
	_ = bet.Settle(r, now)

	if err := bet.Settle(r, now); !errors.Is(err, domain.ErrAlreadySettled) {
		t.Fatalf("want ErrAlreadySettled, got %v", err)
	}
}

func TestOdds_PayoutRoundsDown(t *testing.T) {
	// 3.33 * 1.555 = 5.17815 -> 5.17
	if got := domain.Odds(1555).Payout(333); got != 517 {
		t.Fatalf("got %d, want 517", got)
	}
}

func TestMoney_String(t *testing.T) {
	for in, want := range map[domain.Money]string{0: "0.00", 5: "0.05", 1234: "12.34", -150: "-1.50"} {
		if got := in.String(); got != want {
			t.Errorf("Money(%d) = %q, want %q", in, got, want)
		}
	}
}
