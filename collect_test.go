package gostreams

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/matryer/is"
)

func TestCollectSlice(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	collect := CollectSlice[int]()

	ints := []int{}
	ints = collect(ctx, cancel, 1, 0, ints)
	ints = collect(ctx, cancel, 2, 1, ints)
	ints = collect(ctx, cancel, 3, 2, ints)

	is.Equal(ints, []int{1, 2, 3})
}

func TestCollectMap(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	collect := CollectMap(Identity[int](), itoa)

	mapp := map[int]string{}
	mapp = collect(ctx, cancel, 1, 0, mapp)
	mapp = collect(ctx, cancel, 2, 1, mapp)
	mapp = collect(ctx, cancel, 3, 2, mapp)

	is.Equal(mapp, map[int]string{
		1: "1",
		2: "2",
		3: "3",
	})
}

func TestCollectMap_DuplicateKey(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	collect := CollectMap(Identity[int](), itoa)

	mapp := map[int]string{}
	mapp = collect(ctx, cancel, 1, 0, mapp)
	mapp = collect(ctx, cancel, 2, 1, mapp)
	mapp = collect(ctx, cancel, 3, 2, mapp)
	mapp = collect(ctx, cancel, 3, 3, mapp)

	is.Equal(mapp, map[int]string{
		1: "1",
		2: "2",
		3: "3",
	})
}

func TestCollectMapNoDuplicateKeys(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	collect := CollectMapNoDuplicateKeys(itoa, Identity[int]())

	mapp := map[string]int{}
	mapp = collect(ctx, cancel, 1, 0, mapp)
	mapp = collect(ctx, cancel, 2, 1, mapp)
	mapp = collect(ctx, cancel, 3, 2, mapp)
	mapp = collect(ctx, cancel, 3, 3, mapp)

	is.Equal(mapp, map[string]int{
		"1": 1,
		"2": 2,
		"3": 3,
	})

	is.True(errors.Is(ctx.Err(), context.Canceled))

	var err *DuplicateKeyError[int, string]

	is.True(errors.As(context.Cause(ctx), &err))

	is.Equal(err.Element, 3)
	is.Equal(err.Key, "3")
}

func TestCollectGroup(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	collect := CollectGroup(evenOddStr, Identity[int]())

	mapp := map[string][]int{}
	mapp = collect(ctx, cancel, 1, 0, mapp)
	mapp = collect(ctx, cancel, 2, 1, mapp)
	mapp = collect(ctx, cancel, 3, 2, mapp)
	mapp = collect(ctx, cancel, 4, 3, mapp)
	mapp = collect(ctx, cancel, 5, 4, mapp)

	is.Equal(mapp, map[string][]int{
		"odd":  {1, 3, 5},
		"even": {2, 4},
	})
}

func TestCollectPartition(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	collect := CollectPartition(even, Identity[int]())

	mapp := map[bool][]int{}
	mapp = collect(ctx, cancel, 1, 0, mapp)
	mapp = collect(ctx, cancel, 2, 1, mapp)
	mapp = collect(ctx, cancel, 3, 2, mapp)
	mapp = collect(ctx, cancel, 4, 3, mapp)
	mapp = collect(ctx, cancel, 5, 4, mapp)

	is.Equal(mapp, map[bool][]int{
		false: {1, 3, 5},
		true:  {2, 4},
	})
}

func itoa(_ context.Context, _ context.CancelCauseFunc, elem int, _ uint64) string {
	return strconv.Itoa(elem)
}

func even(_ context.Context, _ context.CancelCauseFunc, elem int, _ uint64) bool {
	return elem%2 == 0
}

func evenOddStr(_ context.Context, _ context.CancelCauseFunc, elem int, _ uint64) string {
	if elem%2 != 0 {
		return "odd"
	}

	return "even"
}
