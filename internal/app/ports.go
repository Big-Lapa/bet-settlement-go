// Package app contains use cases. It depends on the domain and on small
// interfaces (ports) declared here, on the consumer side. Adapters
// (postgres, memory, kafka) implement these interfaces implicitly.
package app

import (
	"context"

	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

// BetRepository stores bets.
type BetRepository interface {
	Create(ctx context.Context, b domain.Bet) error
	Get(ctx context.Context, id string) (domain.Bet, error)
	// PendingByMatch returns pending bets of a match and locks them
	// for the current transaction.
	PendingByMatch(ctx context.Context, matchID string) ([]domain.Bet, error)
	Update(ctx context.Context, b domain.Bet) error
	PlayerStats(ctx context.Context, playerID string) (PlayerStats, error)
}

// ResultLog makes result processing idempotent: Kafka delivers at least once,
// so the same result can arrive twice.
type ResultLog interface {
	// MarkProcessed returns false if the match was already processed.
	MarkProcessed(ctx context.Context, matchID string) (bool, error)
}

// OutboxWriter adds a message to the outbox inside the current transaction.
type OutboxWriter interface {
	Add(ctx context.Context, msg OutboxMessage) error
}

// TxManager runs fn in one database transaction. The transaction travels
// in ctx, so repositories join it without extra parameters.
type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// OutboxMessage is a message waiting to be published to Kafka.
type OutboxMessage struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte
}

// PlayerStats is an aggregated view over a player's bets.
type PlayerStats struct {
	PlayerID    string       `json:"player_id"`
	TotalBets   int64        `json:"total_bets"`
	PendingBets int64        `json:"pending_bets"`
	Won         int64        `json:"won"`
	Lost        int64        `json:"lost"`
	TotalStake  domain.Money `json:"total_stake"`
	TotalPayout domain.Money `json:"total_payout"`
	// NetResult is payout minus stake of settled bets (player's view).
	NetResult domain.Money `json:"net_result"`
}
