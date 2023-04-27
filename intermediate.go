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

// PredicateFunc returns true elem matches a predicate.
// The index is the 0-based index of elem, in the order produced by the upstream producer.
type PredicateFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) bool

// LessFunc returns true if element a is "less" than element b.
type LessFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc, a T, b T) bool

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
		ch := prod(ctx, cancel)

		outCh := make(chan U)

		go func() {
			defer close(outCh)

			index := uint64(0)

			for elem := range ch {
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

// MapConcurrent returns a producer that concurrently calls mapp for each element produced by prod, mapping it to type U.
// The order of elements produced by the new producer is undefined.
func MapConcurrent[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, U]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		ch := prod(ctx, cancel)

		outCh := make(chan U)

		index := uint64(0)

		grp := sync.WaitGroup{}

		for elem := range ch {
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
// that produces elements of type U.
// The new producer produces all elements produced by the intermediate producers, in order.
func FlatMap[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, ProducerFunc[U]]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		ch := prod(ctx, cancel)

		index := uint64(0)

		prods := []ProducerFunc[U]{}

		for elem := range ch {
			prods = append(prods, mapp(ctx, cancel, elem, index))
			index++
		}

		prod := Join(prods...)

		prodCh := prod(ctx, cancel)

		outCh := make(chan U)

		go func() {
			defer close(outCh)

			for elem := range prodCh {
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
// that produces elements of type U.
// The new producer produces all elements produced by the intermediate producers, in undefined order.
func FlatMapConcurrent[T any, U any](prod ProducerFunc[T], mapp MapperFunc[T, ProducerFunc[U]]) ProducerFunc[U] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan U {
		ch := prod(ctx, cancel)

		index := uint64(0)

		prods := []ProducerFunc[U]{}

		for elem := range ch {
			prods = append(prods, mapp(ctx, cancel, elem, index))
			index++
		}

		prod := JoinConcurrent(prods...)

		prodCh := prod(ctx, cancel)

		outCh := make(chan U)

		go func() {
			defer close(outCh)

			for elem := range prodCh {
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
		ch := prod(ctx, cancel)

		outCh := make(chan T)

		go func() {
			defer close(outCh)

			index := uint64(0)

			for elem := range ch {
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
// filter returns true.
// The order of elements produced by the new producer is undefined.
func FilterConcurrent[T any](prod ProducerFunc[T], filter PredicateFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		ch := prod(ctx, cancel)

		outCh := make(chan T)

		index := uint64(0)

		grp := sync.WaitGroup{}

		for elem := range ch {
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
		ch := prod(ctx, cancel)

		outCh := make(chan T)

		go func() {
			defer close(outCh)

			index := uint64(0)

			for elem := range ch {
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
func Limit[T any](prod ProducerFunc[T], max uint64) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		prodCtx, cancelProd := context.WithCancelCause(ctx)

		ch := prod(prodCtx, cancel)

		outCh := make(chan T)

		go func() {
			defer cancelProd(nil)

			defer close(outCh)

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
		ch := prod(ctx, cancel)

		outCh := make(chan T)

		go func() {
			defer close(outCh)

			done := uint64(0)

			for elem := range ch {
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
		ch := prod(ctx, cancel)

		result := []T{}

		for elem := range ch {
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

// Identity returns a mapper that returns the same element it receives.
func Identity[T any]() MapperFunc[T, T] {
	return func(_ context.Context, _ context.CancelCauseFunc, elem T, _ uint64) T {
		return elem
	}
}
