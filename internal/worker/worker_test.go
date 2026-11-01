package worker_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Big-Lapa/bet-settlement-go/internal/worker"
)

func TestProcess_AllItems(t *testing.T) {
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	var sum atomic.Int64

	err := worker.Process(context.Background(), items, 8, func(_ context.Context, n int) error {
		sum.Add(int64(n))
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	if sum.Load() != 999*1000/2 {
		t.Fatalf("sum = %d", sum.Load())
	}
}

func TestProcess_RespectsConcurrencyLimit(t *testing.T) {
	var running, peak atomic.Int64
	items := make([]int, 50)

	_ = worker.Process(context.Background(), items, 4, func(context.Context, int) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		running.Add(-1)
		return nil
	})

	if peak.Load() > 4 {
		t.Fatalf("peak concurrency %d > 4", peak.Load())
	}
}

func TestProcess_StopsOnFirstError(t *testing.T) {
	boom := errors.New("boom")
	var calls atomic.Int64
	items := make([]int, 10_000)

	err := worker.Process(context.Background(), items, 4, func(ctx context.Context, _ int) error {
		if calls.Add(1) == 10 {
			return boom
		}
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if calls.Load() == int64(len(items)) {
		t.Fatal("pool did not stop early")
	}
}

func TestProcess_ParentCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := worker.Process(ctx, []int{1, 2, 3}, 2, func(context.Context, int) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestRetry(t *testing.T) {
	cfg := worker.RetryConfig{Attempts: 3, Base: time.Millisecond, Max: 5 * time.Millisecond}
	transient := errors.New("transient")

	tests := []struct {
		name      string
		failTimes int
		permanent bool
		wantCalls int64
		wantErr   bool
	}{
		{"succeeds first time", 0, false, 1, false},
		{"succeeds after retries", 2, false, 3, false},
		{"gives up", 5, false, 3, true},
		{"permanent stops at once", 5, true, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int64
			err := worker.Retry(context.Background(), cfg, func(context.Context) error {
				if calls.Add(1) <= int64(tt.failTimes) {
					if tt.permanent {
						return worker.Permanent(transient)
					}
					return transient
				}
				return nil
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v", err)
			}
			if calls.Load() != tt.wantCalls {
				t.Fatalf("calls = %d, want %d", calls.Load(), tt.wantCalls)
			}
		})
	}
}

func BenchmarkProcess(b *testing.B) {
	items := make([]int, 10_000)
	for _, workers := range []int{1, 4, 16} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) {
			for b.Loop() {
				_ = worker.Process(context.Background(), items, workers, func(context.Context, int) error { return nil })
			}
		})
	}
}
