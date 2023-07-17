package gostreams

import (
	"context"
	"sync"
)

// ProducerFunc returns a channel of elements for a stream.
type ProducerFunc[T any] func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T

// Produce returns a producer that produces the elements of the given slices, in order.
func Produce[T any](slices ...[]T) ProducerFunc[T] {
	return func(ctx context.Context, _ context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			for _, slice := range slices {
				for _, elem := range slice {
					select {
					case outCh <- elem:

					case <-ctx.Done():
						return
					}
				}
			}
		}()

		return outCh
	}
}

// ProduceChannel returns a producer that produces the elements received through the given channels, in order.
func ProduceChannel[T any](channels ...<-chan T) ProducerFunc[T] {
	return func(ctx context.Context, _ context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		go func() {
			defer close(outCh)

			for _, ch := range channels {
				for elem := range ch {
					select {
					case outCh <- elem:

					case <-ctx.Done():
						return
					}
				}
			}
		}()

		return outCh
	}
}

// ProduceChannelConcurrent returns a producer that produces the elements received through the given channels,
// in undefined order. It consumes the channels concurrently.
func ProduceChannelConcurrent[T any](channels ...<-chan T) ProducerFunc[T] {
	return func(ctx context.Context, _ context.CancelCauseFunc) <-chan T {
		outCh := make(chan T)

		grp := sync.WaitGroup{}
		grp.Add(len(channels))

		for _, ch := range channels {
			go func(ch <-chan T) {
				defer grp.Done()

				for elem := range ch {
					select {
					case outCh <- elem:

					case <-ctx.Done():
						return
					}
				}
			}(ch)
		}

		go func() {
			defer close(outCh)

			grp.Wait()
		}()

		return outCh
	}
}

// Split returns producers that produce the elements produced by prod split between them, in order.
// The elements may not be split evenly between the producers. The new producers consume prod concurrently.
func Split[T any](ctx context.Context, prod ProducerFunc[T]) (ProducerFunc[T], ProducerFunc[T]) {
	outCh1 := make(chan T)
	outCh2 := make(chan T)

	prod1 := ProduceChannel(outCh1)
	prod2 := ProduceChannel(outCh2)

	go func() {
		defer close(outCh1)
		defer close(outCh2)

		ctx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)

		for elem := range prod(ctx, cancel) {
			select {
			case outCh1 <- elem:
			case outCh2 <- elem:

			case <-ctx.Done():
				return
			}
		}
	}()

	return prod1, prod2
}

// Join returns a producer that produces the elements produced by the given producers, in order.
func Join[T any](producers ...ProducerFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		channels := make([]<-chan T, len(producers))
		for i, prod := range producers {
			channels[i] = prod(ctx, cancel)
		}

		return ProduceChannel(channels...)(ctx, cancel)
	}
}

// JoinConcurrent returns a producer that produces the elements produced by the given producers, in undefined order.
// It consumes the producers concurrently.
func JoinConcurrent[T any](producers ...ProducerFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		channels := make([]<-chan T, len(producers))
		for i, prod := range producers {
			channels[i] = prod(ctx, cancel)
		}

		return ProduceChannelConcurrent(channels...)(ctx, cancel)
	}
}

// Tee returns producers that produce all elements produced by prod, in order.
// The new producers consume prod concurrently.
// The new producers must be consumed concurrently to avoid a deadlock (use JoinConcurrent instead of Join).
func Tee[T any](ctx context.Context, prod ProducerFunc[T]) (ProducerFunc[T], ProducerFunc[T]) {
	outCh1 := make(chan T)
	outCh2 := make(chan T)

	prod1 := ProduceChannel(outCh1)
	prod2 := ProduceChannel(outCh2)

	go func() {
		defer close(outCh1)
		defer close(outCh2)

		ctx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)

		for elem := range prod(ctx, cancel) {
			select {
			case outCh1 <- elem:

			case <-ctx.Done():
				return
			}

			select {
			case outCh2 <- elem:

			case <-ctx.Done():
				return
			}
		}
	}()

	return prod1, prod2
}
