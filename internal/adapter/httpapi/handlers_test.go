package httpapi_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/httpapi"
	"github.com/Big-Lapa/bet-settlement-go/internal/adapter/memory"
	"github.com/Big-Lapa/bet-settlement-go/internal/app"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	store := memory.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := app.NewService(app.Deps{Bets: store, Results: store, Outbox: store, Tx: store, Logger: log})
	srv := httptest.NewServer(httpapi.NewRouter(svc, nil, log))
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestAPI_FullFlow(t *testing.T) {
	srv := newServer(t)

	code, bet := do(t, "POST", srv.URL+"/bets",
		`{"player_id":"p1","match_id":"m1","selection":"HOME","stake":1000,"odds":1850}`)
	if code != http.StatusCreated {
		t.Fatalf("place bet: %d %v", code, bet)
	}
	id := bet["id"].(string)

	code, res := do(t, "POST", srv.URL+"/results", `{"match_id":"m1","outcome":"HOME"}`)
	if code != http.StatusOK || res["settled"].(float64) != 1 {
		t.Fatalf("settle: %d %v", code, res)
	}

	code, bet = do(t, "GET", srv.URL+"/bets/"+id, "")
	if code != http.StatusOK || bet["status"] != "WON" || bet["payout"].(float64) != 1850 {
		t.Fatalf("get bet: %d %v", code, bet)
	}

	code, st := do(t, "GET", srv.URL+"/players/p1/stats", "")
	if code != http.StatusOK || st["net_result"].(float64) != 850 {
		t.Fatalf("stats: %d %v", code, st)
	}
}

func TestAPI_Errors(t *testing.T) {
	srv := newServer(t)
	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"validation", "POST", "/bets", `{"player_id":"p1","match_id":"m1","selection":"X","stake":1000,"odds":1850}`, 400},
		{"unknown field", "POST", "/bets", `{"player_id":"p1","foo":1}`, 400},
		{"broken json", "POST", "/bets", `{`, 400},
		{"not found", "GET", "/bets/nope", "", 404},
		{"wrong method", "DELETE", "/bets/1", "", 405},
		{"health", "GET", "/healthz", "", 200},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code, body := do(t, tt.method, srv.URL+tt.path, tt.body); code != tt.want {
				t.Fatalf("got %d (%v), want %d", code, body, tt.want)
			}
		})
	}
}
