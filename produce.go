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
