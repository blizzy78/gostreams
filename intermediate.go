package gostreams

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/exp/slices"
)

// Function returns the result of applying an operation to elem.
type Function[T any, U any] func(elem T) U

// MapperFunc maps element elem to type U.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type MapperFunc[T any, U any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) U

// PredicateFunc returns true if elem matches a predicate.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type PredicateFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) bool

// LessFunc returns true if element a is "less" than element b.
type LessFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc, a T, b T) bool

// SeenFunc is a function that returns true if elem has been seen before.
type SeenFunc[T any] func(elem T) bool

// ErrLimitReached is the error used to short-circuit a stream by canceling its context to indicate that
// the maximum number of elements given to Limit has been reached.
var ErrLimitReached = errors.New("limit reached")

// FuncMapper returns a mapper that calls mapp for each element.
func FuncMapper[T any, U any](mapp Function[T, U]) MapperFunc[T, U] {
	return func(_ context.Context, _ context.CancelCauseFunc, elem T, _ uint64) U {
		return mapp(elem)
	}
}

// Map returns a producer that calls mapp for each element produced by prod, mapping it to type U.
func Map[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, U]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		outCh := make(chan U)

		go func() {
			defer close(outCh)

			index := uint64(0)

			for elem := range prod(ctx, cancel) {
				outElem := mapp(ctx, cancel, elem, index)

				if contextDone(ctx) {
					return
				}

				select {
				case outCh <- outElem:
					index++

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// MapConcurrent returns a producer that concurrently calls mapp for each element produced by prod,
// mapping it to type U. It produces elements in undefined order.
func MapConcurrent[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, U]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		outCh := make(chan U)

		index := uint64(0)

		grp := sync.WaitGroup{}

		for elem := range prod(ctx, cancel) {
			grp.Add(1)

			go func(elem T, index uint64) {
				defer grp.Done()

				outElem := mapp(ctx, cancel, elem, index)

				if contextDone(ctx) {
					return
				}

				select {
				case outCh <- outElem:

				case <-ctx.Done():
				}
			}(elem, index)

			index++
		}

		go func() {
			defer close(outCh)

			grp.Wait()
		}()

		return outCh
	}
}

// FlatMap returns a producer that calls mapp for each element produced by prod, mapping it to an intermediate producer
// that produces elements of type U. The new producer produces all elements produced by the intermediate producers, in order.
func FlatMap[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, ProducerFunc[U]]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		prods := []ProducerFunc[U]{}
		index := uint64(0)

		for elem := range prod(ctx, cancel) {
			prods = append(prods, mapp(ctx, cancel, elem, index))
			index++
		}

		prod := Join(prods...)

		outCh := make(chan U)

		go func() {
			defer close(outCh)

			for elem := range prod(ctx, cancel) {
				select {
				case outCh <- elem:

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// FlatMapConcurrent returns a producer that calls mapp for each element produced by prod, mapping it to an intermediate producer
// that produces elements of type U. The new producer produces all elements produced by the intermediate producers, in undefined order.
func FlatMapConcurrent[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, ProducerFunc[U]]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		prods := []ProducerFunc[U]{}
		index := uint64(0)

		for elem := range prod(ctx, cancel) {
			prods = append(prods, mapp(ctx, cancel, elem, index))
			index++
		}

		prod := JoinConcurrent(prods...)

		outCh := make(chan U)

		go func() {
			defer close(outCh)

			for elem := range prod(ctx, cancel) {
				select {
				case outCh <- elem:

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// Filter returns a producer that calls filter for each element produced by prod, and only produces elements for which
// filter returns true.
func Filter[T any](prod ProducerFunc[T], filter PredicateFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			index := uint64(0)
			for elem := range prod(ctx, cancel) {
				filterResult := filter(ctx, cancel, elem, index)

				if contextDone(ctx) {
					return
				}

				index++

				if !filterResult {
					continue
				}

				select {
				case outCh <- elem:

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// FilterConcurrent returns a producer that calls filter for each element produced by prod, and only produces elements for which
// filter returns true. It produces elements in undefined order.
func FilterConcurrent[T any](prod ProducerFunc[T], filter PredicateFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		index := uint64(0)

		grp := sync.WaitGroup{}

		for elem := range prod(ctx, cancel) {
			grp.Add(1)

			go func(elem T, index uint64) {
				defer grp.Done()

				filterResult := filter(ctx, cancel, elem, index)

				if contextDone(ctx) {
					return
				}

				if !filterResult {
					return
				}

				select {
				case outCh <- elem:

				case <-ctx.Done():
				}
			}(elem, index)

			index++
		}

		go func() {
			defer close(outCh)

			grp.Wait()
		}()

		return outCh
	}
}

// Peek returns a producer that calls peek for each element produced by prod, in order, and produces the same elements.
func Peek[T any](prod ProducerFunc[T], peek ConsumerFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			index := uint64(0)
			for elem := range prod(ctx, cancel) {
				peek(ctx, cancel, elem, index)

				if contextDone(ctx) {
					return
				}

				select {
				case outCh <- elem:
					index++

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// Limit returns a producer that produces the same elements as prod, in order, up to max elements.
// Once the limit has been reached, it cancels prod's context with ErrLimitReached (but not the
// entire stream's context).
func Limit[T any](prod ProducerFunc[T], max uint64) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			prodCtx, cancelProd := context.WithCancelCause(ctx)
			defer cancelProd(nil)

			ch := prod(prodCtx, cancel)

			if max == 0 {
				cancelProd(ErrLimitReached)
				return
			}

			done := uint64(0)

			for elem := range ch {
				select {
				case outCh <- elem:
					done++
					if done == max {
						cancelProd(ErrLimitReached)
						return
					}

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// Skip returns a producer that produces the same elements as prod, in order, skipping the first num elements.
func Skip[T any](prod ProducerFunc[T], num uint64) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			done := uint64(0)
			for elem := range prod(ctx, cancel) {
				done++
				if done <= num {
					continue
				}

				select {
				case outCh <- elem:

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// Sort returns a producer that consumes elements from prod, sorts them using sort, and produces them in sorted order.
func Sort[T any](prod ProducerFunc[T], sort LessFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		result := []T{}
		for elem := range prod(ctx, cancel) {
			result = append(result, elem)
		}

		slices.SortFunc(result, func(a T, b T) bool {
			return sort(ctx, cancel, a, b)
		})

		outCh := make(chan T)

		go func() {
			defer close(outCh)

			for _, elem := range result {
				select {
				case outCh <- elem:

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// Distinct returns a producer that produces those elements produced by prod which are distinct.
func Distinct[T comparable](prod ProducerFunc[T]) ProducerFunc[T] {
	return DistinctSeen(prod, seenMap[T]())
}

// DistinctSeen returns a producer that produces those elements produced by prod for which seen returns false.
func DistinctSeen[T any](prod ProducerFunc[T], seen SeenFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			for elem := range prod(ctx, cancel) {
				if seen(elem) {
					continue
				}

				select {
				case outCh <- elem:

				case <-ctx.Done():
					return
				}
			}
		}()

		return outCh
	}
}

// seenMap returns a SeenFunc that uses a map to keep track of seen elements.
func seenMap[T comparable]() SeenFunc[T] {
	seen := map[T]struct{}{}

	return func(elem T) bool {
		if _, ok := seen[elem]; ok {
			return true
		}

		seen[elem] = struct{}{}

		return false
	}
}

// Identity returns a mapper that returns the same element it receives.
func Identity[T any]() MapperFunc[T, T] {
	return func(_ context.Context, _ context.CancelCauseFunc, elem T, _ uint64) T {
		return elem
	}
}
