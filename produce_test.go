package gostreams

import (
	"context"
	"testing"

	"github.com/matryer/is"
	"golang.org/x/exp/slices"
)

func TestProduce(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ints := []int{}
	for i := range Produce([]int{1, 2}, []int{3, 4, 5})(ctx, cancel) {
		ints = append(ints, i)
	}

	is.Equal(ints, []int{1, 2, 3, 4, 5})
}

func TestProduceChannel(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	intsCh1 := Produce([]int{1, 2})(ctx, cancel)
	intsCh2 := Produce([]int{3, 4, 5})(ctx, cancel)

	ints := []int{}
	for i := range ProduceChannel(intsCh1, intsCh2)(ctx, cancel) {
		ints = append(ints, i)
	}

	is.Equal(ints, []int{1, 2, 3, 4, 5})
}

func TestProduceChannelConcurrent(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	intsCh1 := Produce([]int{1, 2})(ctx, cancel)
	intsCh2 := Produce([]int{3, 4, 5})(ctx, cancel)

	ints := []int{}
	for i := range ProduceChannelConcurrent(intsCh1, intsCh2)(ctx, cancel) {
		ints = append(ints, i)
	}

	slices.Sort(ints)

	is.Equal(ints, []int{1, 2, 3, 4, 5})
}

func TestJoin(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ints1 := Produce([]int{1, 2})
	ints2 := Produce([]int{3, 4, 5})

	ints := []int{}
	for i := range Join(ints1, ints2)(ctx, cancel) {
		ints = append(ints, i)
	}

	is.Equal(ints, []int{1, 2, 3, 4, 5})
}

func TestJoinConcurrent(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ints1 := Produce([]int{1, 2})
	ints2 := Produce([]int{3, 4, 5})

	ints := []int{}
	for i := range JoinConcurrent(ints1, ints2)(ctx, cancel) {
		ints = append(ints, i)
	}

	slices.Sort(ints)

	is.Equal(ints, []int{1, 2, 3, 4, 5})
}

func TestSplit(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ints := Produce([]int{1, 2, 3, 4, 5})

	ints1, ints2 := Split(ctx, ints)

	result := []int{}

	ch1 := ints1(ctx, cancel)
	ch2 := ints2(ctx, cancel)

	for ch1 != nil || ch2 != nil {
		select {
		case i, ok := <-ch1: //nolint:varnamelen // i is okay
			if !ok {
				ch1 = nil
				continue
			}

			result = append(result, i)

		case i, ok := <-ch2: //nolint:varnamelen // i is okay
			if !ok {
				ch2 = nil
				continue
			}

			result = append(result, i)
		}
	}

	slices.Sort(result)

	is.Equal(result, []int{1, 2, 3, 4, 5})
}

func TestTee(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	ints := Produce([]int{1, 2, 3, 4, 5})

	ints1, ints2 := Tee(ctx, ints)

	result1 := []int{}
	result2 := []int{}

	ch1 := ints1(ctx, cancel)
	ch2 := ints2(ctx, cancel)

	for ch1 != nil || ch2 != nil {
		select {
		case i, ok := <-ch1: //nolint:varnamelen // i is okay
			if !ok {
				ch1 = nil
				continue
			}

			result1 = append(result1, i)

		case i, ok := <-ch2: //nolint:varnamelen // i is okay
			if !ok {
				ch2 = nil
				continue
			}

			result2 = append(result2, i)
		}
	}

	is.Equal(result1, []int{1, 2, 3, 4, 5})
	is.Equal(result2, []int{1, 2, 3, 4, 5})
}
