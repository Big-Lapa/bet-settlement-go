package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/app"
)

type fakeStore struct {
	mu   sync.Mutex
	msgs []app.OutboxMessage
	sent map[int64]bool
}

func (s *fakeStore) FetchUnsent(_ context.Context, limit int) ([]app.OutboxMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []app.OutboxMessage
	for _, m := range s.msgs {
		if !s.sent[m.ID] && len(out) < limit {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *fakeStore) MarkSent(_ context.Context, ids []int64, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.sent[id] = true
	}
	return nil
}

type noTx struct{}

func (noTx) WithinTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }

type fakePublisher struct {
	mu   sync.Mutex
	got  []app.OutboxMessage
	fail bool
}

func (p *fakePublisher) Publish(_ context.Context, msgs []app.OutboxMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("broker down")
	}
	p.got = append(p.got, msgs...)
	return nil
}

func newStore(n int) *fakeStore {
	s := &fakeStore{sent: map[int64]bool{}}
	for i := 1; i <= n; i++ {
		s.msgs = append(s.msgs, app.OutboxMessage{ID: int64(i), Topic: "t", Key: "k"})
	}
	return s
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRelay_PublishesInBatches(t *testing.T) {
	store, pub := newStore(25), &fakePublisher{}
	r := NewRelay(store, noTx{}, pub, time.Millisecond, 10, quiet)

	for {
		n, err := r.publishBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}

	if len(pub.got) != 25 || len(store.sent) != 25 {
		t.Fatalf("published %d, marked %d; want 25", len(pub.got), len(store.sent))
	}
}

func TestRelay_KeepsMessagesWhenBrokerIsDown(t *testing.T) {
	store, pub := newStore(3), &fakePublisher{fail: true}
	r := NewRelay(store, noTx{}, pub, time.Millisecond, 10, quiet)

	if _, err := r.publishBatch(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if len(store.sent) != 0 {
		t.Fatal("messages must stay unsent")
	}
}

func TestRelay_RunStopsOnCancel(t *testing.T) {
	r := NewRelay(newStore(0), noTx{}, &fakePublisher{}, time.Millisecond, 10, quiet)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
