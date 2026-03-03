// Package retry provides retry logic with exponential backoff and jitter.
//
// WHY retry in a Kafka consumer?
// Kafka delivers messages "at least once". Your handler will encounter transient
// failures: database timeouts, downstream service blips, rate limits. Without retry
// logic, every transient failure either causes message loss (if you mark it and move on)
// or infinite blocking (if you don't mark it and keep retrying forever).
//
// The pattern:
//   Message received → try handler → fail → wait (backoff) → retry → fail → DLQ
//
// Key concepts:
//   Exponential backoff: each retry waits longer (1s → 2s → 4s → 8s...)
//   Jitter: add randomness to avoid thundering herd (all retries firing simultaneously)
//   Max retries: cap the total number of attempts to prevent infinite loops
//   Non-retryable errors: some failures (bad data, validation) shouldn't be retried
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Config defines the retry behavior.
type Config struct {
	// MaxAttempts is the total number of tries (1 = no retry, 5 = initial + 4 retries).
	MaxAttempts int

	// InitialDelay is how long to wait before the first retry.
	InitialDelay time.Duration

	// MaxDelay caps the backoff to prevent absurdly long waits.
	MaxDelay time.Duration

	// Multiplier controls how fast the delay grows: delay *= Multiplier each attempt.
	Multiplier float64

	// JitterFactor adds randomness to backoff: ±(JitterFactor * delay).
	// 0.0 = no jitter; 0.2 = ±20% randomness.
	JitterFactor float64
}

// DefaultConfig returns sensible retry settings suitable for most Kafka consumers.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		JitterFactor: 0.2,
	}
}

// AggressiveConfig retries quickly—useful when downstream recovery is fast.
func AggressiveConfig() Config {
	return Config{
		MaxAttempts:  10,
		InitialDelay: 50 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Multiplier:   1.5,
		JitterFactor: 0.1,
	}
}

// ─── Error Types ─────────────────────────────────────────────────────────────

// NonRetryableError signals that an error should NOT trigger a retry.
// Use this for permanent failures: malformed payloads, validation errors,
// unknown event types. Retrying these is wasteful—send straight to DLQ.
type NonRetryableError struct {
	cause error
}

func (e *NonRetryableError) Error() string { return fmt.Sprintf("non-retryable: %s", e.cause) }
func (e *NonRetryableError) Unwrap() error  { return e.cause }

// Permanent wraps an error so the retry loop skips retrying and fails immediately.
func Permanent(err error) error {
	return &NonRetryableError{cause: err}
}

// IsPermanent reports whether an error (or its chain) is non-retryable.
func IsPermanent(err error) bool {
	var nre *NonRetryableError
	return errors.As(err, &nre)
}

// ─── Retry Logic ─────────────────────────────────────────────────────────────

// Do executes fn, retrying on failure according to cfg.
//
// It respects context cancellation: if ctx is cancelled between attempts,
// Do returns immediately with a context error.
//
// Example:
//
//	err := retry.Do(ctx, retry.DefaultConfig(), func() error {
//	    return database.Save(event)
//	})
func Do(ctx context.Context, cfg Config, fn func() error) error {
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return fmt.Errorf("retry cancelled at attempt %d/%d: %w",
				attempt, cfg.MaxAttempts, ctx.Err())
		}

		lastErr = fn()
		if lastErr == nil {
			return nil // Success
		}

		// Permanent errors skip retry entirely.
		if IsPermanent(lastErr) {
			return lastErr
		}

		if attempt == cfg.MaxAttempts {
			break // Don't sleep after the final attempt
		}

		delay := computeDelay(cfg, attempt)

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled during backoff: %w", ctx.Err())
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return fmt.Errorf("all %d attempts failed, last error: %w", cfg.MaxAttempts, lastErr)
}

// DoWithResult is like Do but returns a result value alongside the error.
func DoWithResult[T any](ctx context.Context, cfg Config, fn func() (T, error)) (T, error) {
	var (
		result  T
		lastErr error
	)

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return result, fmt.Errorf("retry cancelled: %w", ctx.Err())
		}

		result, lastErr = fn()
		if lastErr == nil {
			return result, nil
		}

		if IsPermanent(lastErr) {
			return result, lastErr
		}

		if attempt == cfg.MaxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return result, fmt.Errorf("retry cancelled during backoff: %w", ctx.Err())
		case <-time.After(computeDelay(cfg, attempt)):
		}
	}

	return result, fmt.Errorf("all %d attempts failed: %w", cfg.MaxAttempts, lastErr)
}

// computeDelay calculates the backoff delay for a given attempt.
//   delay = min(InitialDelay * Multiplier^(attempt-1), MaxDelay) ± jitter
func computeDelay(cfg Config, attempt int) time.Duration {
	base := float64(cfg.InitialDelay) * math.Pow(cfg.Multiplier, float64(attempt-1))

	if base > float64(cfg.MaxDelay) {
		base = float64(cfg.MaxDelay)
	}

	// Jitter: adds or subtracts up to JitterFactor * base.
	// This prevents multiple failed consumers from retrying simultaneously.
	if cfg.JitterFactor > 0 {
		jitter := base * cfg.JitterFactor * (2*rand.Float64() - 1)
		base += jitter
	}

	if base < 0 {
		base = 0
	}

	return time.Duration(base)
}
