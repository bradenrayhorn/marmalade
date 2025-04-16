package s3

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

type retriableError struct {
	err error
}

func (e retriableError) Error() string {
	return fmt.Sprintf("retry: %v", e.err)
}

func (e retriableError) Unwrap() error {
	return e.err
}

func withRetries[T any](do func() (T, error)) (T, error) {
	maxTries := 10
	i := 0

	var result T
	var err error
	var retriable retriableError
	for i < maxTries {
		result, err = do()

		// if there is no error just return
		if err == nil {
			return result, nil
		}

		// if the error is not retriable then return
		if !errors.As(err, &retriable) {
			return result, err
		}

		backoff := time.Duration(100 * (1 << i))
		jitter := time.Duration(rand.Int64N(int64(100)))

		// don't sleep in tests to keep them fast
		if !testing.Testing() {
			time.Sleep(backoff + jitter)
		}
		i++
	}

	return result, fmt.Errorf("retries exceeded: %w", retriable.Unwrap())
}
