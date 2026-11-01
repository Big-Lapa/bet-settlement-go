package domain

// MatchResult is the final result of a match, published by the odds/feed side.
type MatchResult struct {
	MatchID   string    `json:"match_id"`
	Outcome   Selection `json:"outcome"`
	Cancelled bool      `json:"cancelled"`
}

// Validate checks that the result can be applied.
func (r MatchResult) Validate() error {
	if r.MatchID == "" {
		return &ValidationError{Field: "match_id", Reason: "is required"}
	}
	if !r.Cancelled && !r.Outcome.Valid() {
		return &ValidationError{Field: "outcome", Reason: "must be HOME, DRAW or AWAY"}
	}
	return nil
}

// BetSettled is the domain event emitted for every settled bet.
// It goes to Kafka through the transactional outbox.
type BetSettled struct {
	BetID     string    `json:"bet_id"`
	PlayerID  string    `json:"player_id"`
	MatchID   string    `json:"match_id"`
	Status    BetStatus `json:"status"`
	Payout    Money     `json:"payout"`
	SettledAt string    `json:"settled_at"`
}
