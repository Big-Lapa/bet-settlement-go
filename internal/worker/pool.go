// Package worker contains small generic concurrency helpers.
package worker

import (
	"context"
	"sync"
)

// Process runs fn for every item using at most `workers` goroutines.
//
// It is written with plain channels on purpose, to show the classic pattern:
//   - a producer goroutine feeds a jobs channel and closes it when done;
//   - N workers range over the channel;
//   - the first error cancels the shared context, so other workers stop early;
//   - sync.WaitGroup waits for all workers before returning.
//
// In production code golang.org/x/sync/errgroup with SetLimit does the same
// in a few lines (see cmd/settlement/main.go for errgroup usage).
func Process[T any](ctx context.Context, items []T, workers int, fn func(context.Context, T) error) error {
	if workers < 1 {
		workers = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan T)
	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
	)
	fail := func(err error) {
		once.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if err := fn(ctx, item); err != nil {
					fail(err)
				}
			}
		}()
	}

	// Producer: stop feeding as soon as the context is cancelled.
feed:
	for _, item := range items {
		select {
		case jobs <- item:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	return ctx.Err() // non-nil only if the parent context was cancelled
}
