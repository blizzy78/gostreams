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
	is.Equal(fuzzProd.order, OrderStable)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
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
	is.Equal(fuzzProd.order, OrderStable)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
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
	is.Equal(fuzzProd.order, OrderUnstable)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	slices.Sort(elems)

	expected := make([]byte, len(fuzzProd.expected))
	copy(expected, fuzzProd.expected)
	slices.Sort(expected)

	is.Equal(elems, expected)
}

func TestReadProducerLimit(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce([]byte{1, 2, 3, 4, 5})
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerLimit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, OrderLikeParent)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	is.Equal(elems, []byte{1, 2, 3})
}

func TestReadProducerLimit_UpstreamIsShorter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce([]byte{1, 2})
		},

		expected: []byte{1, 2},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerLimit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, OrderLikeParent)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	is.Equal(elems, []byte{1, 2})
}

func TestReadProducerSkip(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce([]byte{1, 2, 3, 4, 5})
		},

		expected: []byte{1, 2, 3, 4, 5},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerSkip(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, OrderLikeParent)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	is.Equal(elems, []byte{4, 5})
}

func TestReadProducerSkip_UpstreamIsShorter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce([]byte{1, 2})
		},

		expected: []byte{1, 2},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, ok := readProducerSkip(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, OrderLikeParent)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	isEqualSlices(t, is, elems, []byte{})
}

func TestReadProducerSort(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce([]byte{3, 1, 5, 2, 4})
		},

		expected: []byte{3, 1, 5, 2, 4},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerSort(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, OrderStable)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	is.Equal(elems, []byte{1, 2, 3, 4, 5})
}

func TestReadProducerMap(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce([]byte{10, 20, 30, 40, 50})
		},

		expected: []byte{10, 20, 30, 40, 50},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, ok := readProducerMap(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.order, OrderLikeParent)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected, []byte{5, 10, 15, 20, 25})
	is.Equal(fuzzInput, []byte{})
	is.True(ok)

	elems, _ := ReduceSlice(context.Background(), fuzzProd.create())
	is.Equal(elems, []byte{5, 10, 15, 20, 25})
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
