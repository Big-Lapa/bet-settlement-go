package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Big-Lapa/bet-settlement-go/internal/app"
	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

// Compile-time interface checks.
var (
	_ app.BetRepository = (*DB)(nil)
	_ app.ResultLog     = (*DB)(nil)
	_ app.OutboxWriter  = (*DB)(nil)
	_ app.TxManager     = (*DB)(nil)
)

const betColumns = `id, player_id, match_id, selection, stake, odds, status, payout, created_at, settled_at`

// Create implements app.BetRepository.
func (db *DB) Create(ctx context.Context, b domain.Bet) error {
	_, err := db.q(ctx).Exec(ctx,
		`INSERT INTO bets (`+betColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		b.ID, b.PlayerID, b.MatchID, b.Selection, b.Stake, b.Odds, b.Status, b.Payout, b.CreatedAt, b.SettledAt)
	if err != nil {
		return fmt.Errorf("insert bet: %w", err)
	}
	return nil
}

// Get implements app.BetRepository.
func (db *DB) Get(ctx context.Context, id string) (domain.Bet, error) {
	rows, err := db.q(ctx).Query(ctx, `SELECT `+betColumns+` FROM bets WHERE id = $1`, id)
	if err != nil {
		return domain.Bet{}, fmt.Errorf("query bet: %w", err)
	}
	b, err := pgx.CollectExactlyOneRow(rows, scanBet)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Bet{}, fmt.Errorf("bet %s: %w", id, domain.ErrNotFound)
	}
	return b, err
}

// PendingByMatch implements app.BetRepository.
//
// FOR UPDATE locks the rows until the transaction ends, so two settlement
// workers can never update the same bet concurrently.
func (db *DB) PendingByMatch(ctx context.Context, matchID string) ([]domain.Bet, error) {
	rows, err := db.q(ctx).Query(ctx, `
		SELECT `+betColumns+`
		FROM bets
		WHERE match_id = $1 AND status = 'PENDING'
		ORDER BY created_at
		FOR UPDATE`, matchID)
	if err != nil {
		return nil, fmt.Errorf("query pending: %w", err)
	}
	return pgx.CollectRows(rows, scanBet)
}

// Update implements app.BetRepository. The status guard in WHERE is an
// extra safety net: a settled bet can never be settled again.
func (db *DB) Update(ctx context.Context, b domain.Bet) error {
	tag, err := db.q(ctx).Exec(ctx, `
		UPDATE bets SET status = $2, payout = $3, settled_at = $4
		WHERE id = $1 AND status = 'PENDING'`,
		b.ID, b.Status, b.Payout, b.SettledAt)
	if err != nil {
		return fmt.Errorf("update bet: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("bet %s: %w", b.ID, domain.ErrAlreadySettled)
	}
	return nil
}

// PlayerStats implements app.BetRepository with one aggregate query
// (conditional aggregation via FILTER instead of several round trips).
func (db *DB) PlayerStats(ctx context.Context, playerID string) (app.PlayerStats, error) {
	st := app.PlayerStats{PlayerID: playerID}
	err := db.q(ctx).QueryRow(ctx, `
		SELECT
			count(*),
			count(*) FILTER (WHERE status = 'PENDING'),
			count(*) FILTER (WHERE status = 'WON'),
			count(*) FILTER (WHERE status = 'LOST'),
			coalesce(sum(stake), 0),
			coalesce(sum(payout), 0),
			coalesce(sum(payout - stake) FILTER (WHERE status <> 'PENDING'), 0)
		FROM bets
		WHERE player_id = $1`, playerID).
		Scan(&st.TotalBets, &st.PendingBets, &st.Won, &st.Lost, &st.TotalStake, &st.TotalPayout, &st.NetResult)
	if err != nil {
		return app.PlayerStats{}, fmt.Errorf("player stats: %w", err)
	}
	return st, nil
}

// MarkProcessed implements app.ResultLog. ON CONFLICT DO NOTHING turns the
// primary key into an idempotency check that is safe under concurrency.
func (db *DB) MarkProcessed(ctx context.Context, matchID string) (bool, error) {
	tag, err := db.q(ctx).Exec(ctx,
		`INSERT INTO processed_results (match_id) VALUES ($1) ON CONFLICT DO NOTHING`, matchID)
	if err != nil {
		return false, fmt.Errorf("mark processed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Add implements app.OutboxWriter.
func (db *DB) Add(ctx context.Context, m app.OutboxMessage) error {
	_, err := db.q(ctx).Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`, m.Topic, m.Key, m.Payload)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

// FetchUnsent implements outbox.Store. SKIP LOCKED lets several relay
// instances work in parallel without blocking each other.
func (db *DB) FetchUnsent(ctx context.Context, limit int) ([]app.OutboxMessage, error) {
	rows, err := db.q(ctx).Query(ctx, `
		SELECT id, topic, key, payload
		FROM outbox
		WHERE sent_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (app.OutboxMessage, error) {
		var m app.OutboxMessage
		err := r.Scan(&m.ID, &m.Topic, &m.Key, &m.Payload)
		return m, err
	})
}

// MarkSent implements outbox.Store.
func (db *DB) MarkSent(ctx context.Context, ids []int64, at time.Time) error {
	_, err := db.q(ctx).Exec(ctx, `UPDATE outbox SET sent_at = $2 WHERE id = ANY($1)`, ids, at)
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	return nil
}

func scanBet(r pgx.CollectableRow) (domain.Bet, error) {
	var b domain.Bet
	err := r.Scan(&b.ID, &b.PlayerID, &b.MatchID, &b.Selection, &b.Stake, &b.Odds,
		&b.Status, &b.Payout, &b.CreatedAt, &b.SettledAt)
	return b, err
}
