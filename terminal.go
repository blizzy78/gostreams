package gostreams

import (
	"context"
	"errors"
	"sync"
)

// ConsumerFunc consumes element elem.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type ConsumerFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64)

// CollectorFunc folds element elem into an internal accumulator and returns the result so far.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type CollectorFunc[T any, R any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) R

// ErrShortCircuit is a generic error used to short-circuit a stream by canceling its context.
var ErrShortCircuit = errors.New("short circuit")

// Reduce calls coll for each element produced by prod, and returns the final result.
// If the stream's context is canceled, it returns the result so far, and the cause of the cancellation.
// If the cause of cancellation was ErrShortCircuit, it returns a nil error instead.
func Reduce[T any, R any](ctx context.Context, prod ProducerFunc[T], coll CollectorFunc[T, R]) (R, error) {
	var result R

	err := Each(ctx, prod, func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) {
		result = coll(ctx, cancel, elem, index)
	})

	return result, err
}

// ReduceSlice returns a slice of all elements produced by prod.
// If the stream's context is canceled, it returns the collected elements so far, and the cause of the cancellation.
// If the cause of cancellation was ErrShortCircuit, it returns a nil error instead.
func ReduceSlice[T any](ctx context.Context, prod ProducerFunc[T]) ([]T, error) {
	return Reduce(ctx, prod, CollectSlice[T]())
}

// Each calls each for each element produced by prod.
// If the stream's context is canceled, it returns the cause of the cancellation.
// If the cause of cancellation was ErrShortCircuit, it returns a nil error instead.
func Each[T any](ctx context.Context, prod ProducerFunc[T], each ConsumerFunc[T]) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ch := prod(ctx, cancel)

	index := uint64(0)

	for elem := range ch {
		each(ctx, cancel, elem, index)

		if contextDone(ctx) {
			break
		}

		index++
	}

	err := context.Cause(ctx)
	if errors.Is(err, ErrShortCircuit) {
		err = nil
	}

	return err
}

// EachConcurrent concurrently calls each for each element produced by prod.
// If the stream's context is canceled, it returns the cause of the cancellation.
// If the cause of cancellation was ErrShortCircuit, it returns a nil error instead.
func EachConcurrent[T any](ctx context.Context, prod ProducerFunc[T], each ConsumerFunc[T]) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ch := prod(ctx, cancel)

	index := uint64(0)

	grp := sync.WaitGroup{}

	for elem := range ch {
		grp.Add(1)

		go func(elem T, index uint64) {
			defer grp.Done()

			each(ctx, cancel, elem, index)
		}(elem, index)

		index++
	}

	grp.Wait()

	err := context.Cause(ctx)
	if errors.Is(err, ErrShortCircuit) {
		err = nil
	}

	return err
}

// AnyMatch returns true as soon as pred returns true for an element produced by prod, that is, an element matches.
// If an element matches, it cancels the stream's context using ErrShortCircuit, and returns a nil error.
// If the stream's context is canceled with any other error, it returns an undefined result, and the cause of the cancellation.
func AnyMatch[T any](ctx context.Context, prod ProducerFunc[T], pred PredicateFunc[T]) (bool, error) {
	anyMatch := false

	err := Each(ctx, prod, func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) {
		if !pred(ctx, cancel, elem, index) {
			return
		}

		anyMatch = true

		cancel(ErrShortCircuit)
	})

	return anyMatch, err
}

// AllMatch returns true if pred returns true for all elements produced by prod, that is, all elements match.
// If any element doesn't match, it cancels the stream's context using ErrShortCircuit, and returns a nil error.
// If the stream's context is canceled with any other error, it returns an undefined result, and the cause of the cancellation.
func AllMatch[T any](ctx context.Context, prod ProducerFunc[T], pred PredicateFunc[T]) (bool, error) {
	allMatch := true

	err := Each(ctx, prod, func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) {
		if pred(ctx, cancel, elem, index) {
			return
		}

		allMatch = false

		cancel(ErrShortCircuit)
	})

	return allMatch, err
}

// Count returns the number of elements produced by prod.
// If the stream's context is canceled, it returns an undefined result, and the cause of the cancellation.
func Count[T any](ctx context.Context, prod ProducerFunc[T]) (uint64, error) {
	count := uint64(0)

	err := Each(ctx, prod, func(_ context.Context, _ context.CancelCauseFunc, _ T, _ uint64) {
		count++
	})

	return count, err
}
