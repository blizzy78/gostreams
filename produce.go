package gostreams

import (
	"context"
	"sync"
	"sync/atomic"
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
	prod, _ := produceChannel(channels...)
	return prod
}

// produceChannel returns a producer that produces the elements received through the given channels, in order.
// The returned signal channel will be closed once the new producer is finished.
// The new producer must not be called more than once, doing so will panic.
func produceChannel[T any](channels ...<-chan T) (ProducerFunc[T], <-chan struct{}) {
	finished := make(chan struct{})

	started := atomic.Bool{}

	return func(ctx context.Context, _ context.CancelCauseFunc) <-chan T {
		if started.Swap(true) {
			panic("producer called multiple times")
		}

		outCh := make(chan T)

		go func() {
			defer close(finished)

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
	}, finished
}

// ProduceChannelConcurrent returns a producer that produces the elements received through the given channels, in undefined order.
// The channels are consumed concurrently.
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

// Split returns producers that produce the elements produced by prod, in undefined order.
// prod is consumed concurrently by the new producers.
// The new producers are not guaranteed to consume elements evenly.
func Split[T any](ctx context.Context, prod ProducerFunc[T]) (ProducerFunc[T], ProducerFunc[T], context.Context) {
	outCh1 := make(chan T)
	outCh2 := make(chan T)

	prod1, finished1 := produceChannel(outCh1)
	prod2, finished2 := produceChannel(outCh2)

	go func() {
		ctx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)

		defer func() {
			<-finished1
			<-finished2
		}()

		defer close(outCh1)
		defer close(outCh2)

		for elem := range prod(ctx, cancel) {
			select {
			case outCh1 <- elem:
			case outCh2 <- elem:

			case <-ctx.Done():
				return
			}
		}
	}()

	return prod1, prod2, ctx
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
// The producers are consumed concurrently.
func JoinConcurrent[T any](producers ...ProducerFunc[T]) ProducerFunc[T] {
	return func(ctx context.Context, cancel context.CancelCauseFunc) <-chan T {
		channels := make([]<-chan T, len(producers))
		for i, prod := range producers {
			channels[i] = prod(ctx, cancel)
		}

		return ProduceChannelConcurrent(channels...)(ctx, cancel)
	}
}
