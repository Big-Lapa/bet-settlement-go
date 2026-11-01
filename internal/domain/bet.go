// Package domain holds the business rules. It has no dependencies on
// databases, Kafka or HTTP: everything here is plain Go and easy to test.
package domain

import (
	"strings"
	"time"
)

// Selection is the outcome a player bets on (1X2 market).
type Selection string

const (
	SelectionHome Selection = "HOME"
	SelectionDraw Selection = "DRAW"
	SelectionAway Selection = "AWAY"
)

// Valid reports whether s is a known selection.
func (s Selection) Valid() bool {
	switch s {
	case SelectionHome, SelectionDraw, SelectionAway:
		return true
	}
	return false
}

// BetStatus is the lifecycle state of a bet.
type BetStatus string

const (
	StatusPending BetStatus = "PENDING"
	StatusWon     BetStatus = "WON"
	StatusLost    BetStatus = "LOST"
	StatusVoid    BetStatus = "VOID" // match cancelled: stake is returned
)

// Limits for a single bet.
const (
	MinStake Money = 10        // 0.10
	MaxStake Money = 1_000_000 // 10,000.00
	MinOdds  Odds  = 1010      // 1.01
	MaxOdds  Odds  = 1_000_000 // 1000.00
)

// Bet is the aggregate root. State changes go through methods,
// so invariants live in one place.
type Bet struct {
	ID        string
	PlayerID  string
	MatchID   string
	Selection Selection
	Stake     Money
	Odds      Odds
	Status    BetStatus
	Payout    Money
	CreatedAt time.Time
	SettledAt *time.Time
}

// NewBetParams groups constructor arguments; a struct scales better than a
// long positional parameter list.
type NewBetParams struct {
	ID        string
	PlayerID  string
	MatchID   string
	Selection Selection
	Stake     Money
	Odds      Odds
	Now       time.Time
}

// NewBet validates input and returns a pending bet.
func NewBet(p NewBetParams) (Bet, error) {
	switch {
	case strings.TrimSpace(p.PlayerID) == "":
		return Bet{}, &ValidationError{Field: "player_id", Reason: "is required"}
	case strings.TrimSpace(p.MatchID) == "":
		return Bet{}, &ValidationError{Field: "match_id", Reason: "is required"}
	case !p.Selection.Valid():
		return Bet{}, &ValidationError{Field: "selection", Reason: "must be HOME, DRAW or AWAY"}
	case p.Stake < MinStake || p.Stake > MaxStake:
		return Bet{}, &ValidationError{Field: "stake", Reason: "out of range"}
	case p.Odds < MinOdds || p.Odds > MaxOdds:
		return Bet{}, &ValidationError{Field: "odds", Reason: "out of range"}
	}
	return Bet{
		ID:        p.ID,
		PlayerID:  p.PlayerID,
		MatchID:   p.MatchID,
		Selection: p.Selection,
		Stake:     p.Stake,
		Odds:      p.Odds,
		Status:    StatusPending,
		CreatedAt: p.Now.UTC(),
	}, nil
}

// Settle applies a match result. It is a pure function of the bet state and
// the result, which makes it trivial to unit test.
func (b *Bet) Settle(r MatchResult, now time.Time) error {
	if b.Status != StatusPending {
		return ErrAlreadySettled
	}
	if r.MatchID != b.MatchID {
		return &ValidationError{Field: "match_id", Reason: "result is for another match"}
	}

	switch {
	case r.Cancelled:
		b.Status, b.Payout = StatusVoid, b.Stake
	case r.Outcome == b.Selection:
		b.Status, b.Payout = StatusWon, b.Odds.Payout(b.Stake)
	default:
		b.Status, b.Payout = StatusLost, 0
	}
	t := now.UTC()
	b.SettledAt = &t
	return nil
}
