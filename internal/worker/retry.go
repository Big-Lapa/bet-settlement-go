package worker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// permanentError marks an error that must not be retried.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent wraps err so Retry gives up immediately.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool { return errors.As(err, new(permanentError)) }

// RetryConfig controls Retry.
type RetryConfig struct {
	Attempts int
	Base     time.Duration // first delay; doubled after every attempt
	Max      time.Duration // upper bound for one delay
}

// Retry calls fn until it succeeds, returns a permanent error, the attempts
// run out, or ctx is cancelled. Delays grow exponentially.
func Retry(ctx context.Context, cfg RetryConfig, fn func(context.Context) error) error {
	delay := cfg.Base
	var err error
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		if err = fn(ctx); err == nil || IsPermanent(err) {
			return err
		}
		if attempt == cfg.Attempts {
			break
		}
		// time.NewTimer + Stop instead of time.After: no timer leak on early exit.
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		delay = min(delay*2, cfg.Max)
	}
	return fmt.Errorf("after %d attempts: %w", cfg.Attempts, err)
}
