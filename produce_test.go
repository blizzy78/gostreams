package gostreams

import (
	"context"
	"slices"
	"testing"

	"github.com/matryer/is"
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
