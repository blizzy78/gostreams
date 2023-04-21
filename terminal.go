package gostreams

import (
	"context"
	"errors"
	"sync"
)

// ConsumerFunc consumes element elem.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type ConsumerFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64)

// AccumulatorFunc folds element elem into the accumulator acc, returning acc, or a new accumulator.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type AccumulatorFunc[T any, A any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64, acc A) A

// ErrShortCircuit is a generic error used to short-circuit a stream by canceling its context.
var ErrShortCircuit = errors.New("short circuit")

// Reduce calls reduce for each element produced by prod, folding it into accumulator acc, returning the final accumulator.
// If prod or reduce cancel the stream's context, it returns the accumulator so far, and the cause of the cancelation.
func Reduce[T any, A any](ctx context.Context, prod ProducerFunc[T], acc A, reduce AccumulatorFunc[T, A]) (A, error) {
	err := Each(ctx, prod, func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) {
		acc = reduce(ctx, cancel, elem, index, acc)
	})

	return acc, err
}

// Each calls each for each element produced by prod.
// If prod or each cancel the stream's context, it returns cause of the cancelation.
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
// If prod or each cancel the stream's context, it returns cause of the cancelation.
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
// If an element matches, it cancels the stream's context using ErrShortCircuit.
// If prod or pred cancel the stream's context, it returns an undefined result, and the cause of the cancelation.
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
// If any element does not match, it cancels the stream's context using ErrShortCircuit.
// If prod or pred cancel the stream's context, it returns an undefined result, and the cause of the cancelation.
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
// If prod cancels the stream's context, it returns an undefined result, and the cause of the cancelation.
func Count[T any](ctx context.Context, prod ProducerFunc[T]) (uint64, error) {
	count := uint64(0)

	err := Each(ctx, prod, func(_ context.Context, _ context.CancelCauseFunc, _ T, _ uint64) {
		count++
	})

	return count, err
}
