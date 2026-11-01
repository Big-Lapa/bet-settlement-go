// Package httpapi exposes the use cases over HTTP using only net/http
// (Go 1.22+ pattern routing: "POST /bets", "GET /bets/{id}").
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/app"
	"github.com/Big-Lapa/bet-settlement-go/internal/domain"
)

// BetService is the subset of app.Service the HTTP layer uses.
// Declared here, at the consumer, so handlers can be tested with a fake.
type BetService interface {
	PlaceBet(ctx context.Context, cmd app.PlaceBetCmd) (domain.Bet, error)
	GetBet(ctx context.Context, id string) (domain.Bet, error)
	SettleMatch(ctx context.Context, r domain.MatchResult) (int, error)
	PlayerStats(ctx context.Context, playerID string) (app.PlayerStats, error)
}

// Pinger checks a dependency for the readiness probe.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Handler holds HTTP handlers.
type Handler struct {
	svc   BetService
	ready Pinger // may be nil
	log   *slog.Logger
}

// NewRouter builds the HTTP handler with routes and middleware.
func NewRouter(svc BetService, ready Pinger, log *slog.Logger) http.Handler {
	h := &Handler{svc: svc, ready: ready, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /bets", h.placeBet)
	mux.HandleFunc("GET /bets/{id}", h.getBet)
	mux.HandleFunc("GET /players/{id}/stats", h.playerStats)
	mux.HandleFunc("POST /results", h.submitResult) // manual entry; normally results come from Kafka
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", h.readyz)

	return Chain(mux, RequestID(), Logging(log), Recover(log), Timeout(5*time.Second))
}

type placeBetRequest struct {
	PlayerID  string           `json:"player_id"`
	MatchID   string           `json:"match_id"`
	Selection domain.Selection `json:"selection"`
	Stake     domain.Money     `json:"stake"` // cents
	Odds      domain.Odds      `json:"odds"`  // decimal odds * 1000
}

type betResponse struct {
	ID        string           `json:"id"`
	PlayerID  string           `json:"player_id"`
	MatchID   string           `json:"match_id"`
	Selection domain.Selection `json:"selection"`
	Stake     domain.Money     `json:"stake"`
	Odds      domain.Odds      `json:"odds"`
	Status    domain.BetStatus `json:"status"`
	Payout    domain.Money     `json:"payout"`
	CreatedAt time.Time        `json:"created_at"`
	SettledAt *time.Time       `json:"settled_at,omitempty"`
}

func toResponse(b domain.Bet) betResponse {
	return betResponse{
		ID: b.ID, PlayerID: b.PlayerID, MatchID: b.MatchID, Selection: b.Selection,
		Stake: b.Stake, Odds: b.Odds, Status: b.Status, Payout: b.Payout,
		CreatedAt: b.CreatedAt, SettledAt: b.SettledAt,
	}
}

func (h *Handler) placeBet(w http.ResponseWriter, r *http.Request) {
	req, err := decode[placeBetRequest](w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	bet, err := h.svc.PlaceBet(r.Context(), app.PlaceBetCmd(req))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toResponse(bet))
}

func (h *Handler) getBet(w http.ResponseWriter, r *http.Request) {
	bet, err := h.svc.GetBet(r.Context(), r.PathValue("id"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(bet))
}

func (h *Handler) playerStats(w http.ResponseWriter, r *http.Request) {
	st, err := h.svc.PlayerStats(r.Context(), r.PathValue("id"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (h *Handler) submitResult(w http.ResponseWriter, r *http.Request) {
	res, err := decode[domain.MatchResult](w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := h.svc.SettleMatch(r.Context(), res)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"settled": n})
}

func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if h.ready != nil {
		if err := h.ready.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not ready")
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// fail maps domain errors to HTTP status codes in one place.
func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrValidation):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrAlreadySettled):
		writeError(w, http.StatusConflict, err.Error())
	default:
		// Internal details go to the log, not to the client.
		h.log.ErrorContext(r.Context(), "request failed", "err", err, "request_id", RequestIDFrom(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// decode is a generic JSON body decoder with a size limit and strict fields.
func decode[T any](w http.ResponseWriter, r *http.Request) (T, error) {
	var v T
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return v, fmt.Errorf("invalid json: %w", err)
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
