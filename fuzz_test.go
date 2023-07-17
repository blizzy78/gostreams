package gostreams

import (
	"context"
	"testing"

	"github.com/matryer/is"
	"golang.org/x/exp/slices"
)

func TestReadProducerSlice(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	fuzzProd, fuzzInput, ok := readProducerSlice(t, fuzzInput, nil)
	is.Equal(fuzzProd.order, orderStable)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
}

func TestReadProducerChannel(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	fuzzProd, fuzzInput, ok := readProducerChannel(t, fuzzInput, nil)
	is.Equal(fuzzProd.order, orderStable)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
}

func TestReadProducerChannelConcurrent(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	fuzzProd, fuzzInput, ok := readProducerChannelConcurrent(t, fuzzInput, nil)
	is.Equal(fuzzProd.order, orderUnstable)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	slices.Sort(elems)

	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
}

func TestReadProducerLimit(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2, 3, 4, 5})}
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerLimit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 2, 3})
}

func TestReadProducerLimit_UpstreamIsShorter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2})}
		},

		expected: []byte{1, 2},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerLimit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 2})
}

func TestReadProducerSkip(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2, 3, 4, 5})}
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerSkip(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{4, 5})
}

func TestReadProducerSkip_UpstreamIsShorter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2})}
		},

		expected: []byte{1, 2},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerSkip(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.True(len(elems) == 0)
}

func TestReadProducerSort(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{3, 1, 5, 2, 4})}
		},

		expected: []byte{3, 1, 5, 2, 4},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerSort(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, orderStable)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 2, 3, 4, 5})
}

func TestReadProducerMap(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{10, 20, 30, 40, 50})}
		},

		expected: []byte{10, 20, 30, 40, 50},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerMap(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{5, 10, 15, 20, 25})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{5, 10, 15, 20, 25})
}

func TestReadProducerMapConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{10, 20, 30, 40, 50})}
		},

		expected: []byte{10, 20, 30, 40, 50},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerMapConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{3, 6, 10, 13, 16})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	slices.Sort(elems)
	is.Equal(elems, []byte{3, 6, 10, 13, 16})
}

func TestReadProducerSplit(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2, 3, 4, 5})}
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerSplit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, orderUnstable)
	is.Equal(fuzzProd.multiple, multipleYes)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, Join(prods...))
	slices.Sort(elems)

	is.Equal(elems, []byte{1, 2, 3, 4, 5})
}

func TestReadProducerJoin(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleYes,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{
				Produce([]byte{1, 2, 3, 4, 5}),
				Produce([]byte{6, 7, 8, 9, 10}),
			}
		},

		expected: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerJoin(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func TestReadProducerJoinConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleYes,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{
				Produce([]byte{1, 2, 3, 4, 5}),
				Produce([]byte{6, 7, 8, 9, 10}),
			}
		},

		expected: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerJoinConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, orderUnstable)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	slices.Sort(elems)

	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func TestReadProducerTee(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2, 3, 4, 5})}
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerTee(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, orderUnstable)
	is.Equal(fuzzProd.multiple, multipleYes)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, JoinConcurrent(prods...))
	slices.Sort(elems)

	is.Equal(elems, []byte{1, 1, 2, 2, 3, 3, 4, 4, 5, 5})
}

func TestReadProducerFilter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2, 3, 4, 5})}
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerFilter(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{2, 4})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{2, 4})
}

func TestReadProducerFilterConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 2, 3, 4, 5})}
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerFilterConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{2, 4})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	slices.Sort(elems)
	is.Equal(elems, []byte{2, 4})
}

func TestReadProducerDistinct(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{1, 3, 2, 4, 1, 3, 4, 3, 2, 5, 5})}
		},

		expected: []byte{1, 3, 2, 4, 1, 3, 4, 3, 2, 5, 5},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerDistinct(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 3, 2, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{1, 3, 2, 4, 5})
}

func TestReadProducerFlatMap(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{10, 20, 30, 40, 50})}
		},

		expected: []byte{10, 20, 30, 40, 50},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerFlatMap(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{10, 2, 20, 5, 30, 7, 40, 10, 50, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	is.Equal(elems, []byte{10, 2, 20, 5, 30, 7, 40, 10, 50, 12})
}

func TestReadProducerFlatMapConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce([]byte{10, 20, 30, 40, 50})}
		},

		expected: []byte{10, 20, 30, 40, 50},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerFlatMapConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{10, 2, 20, 5, 30, 7, 40, 10, 50, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	ctx := context.Background()
	prods := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods[0])
	slices.Sort(elems)
	is.Equal(elems, []byte{2, 5, 7, 10, 10, 12, 20, 30, 40, 50})
}

func TestReadSlices(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	slices, fuzzInput, ok := readSlices(t, fuzzInput)
	is.Equal(slices, [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11, 12}})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)
}

func TestReadSlice(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{5, 1, 2, 3, 4, 5}

	input, fuzzInput, ok := readSlice(t, fuzzInput)
	is.Equal(input, []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)
}

func TestReadByte(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{123}

	b, fuzzInput, ok := readByte(t, fuzzInput)
	is.Equal(b, byte(123))
	is.Equal(fuzzInput, []byte{})
	is.True(ok)
}
