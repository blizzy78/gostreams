package gostreams

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/matryer/is"
)

func TestReadProducerSlice(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	fuzzProd, fuzzInput, err := readProducerSlice(t, fuzzInput, nil)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
}

func TestReadProducerChannel(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	fuzzProd, fuzzInput, err := readProducerChannel(t, fuzzInput, nil)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
}

func TestReadProducerChannelConcurrent(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	fuzzProd, fuzzInput, err := readProducerChannelConcurrent(t, fuzzInput, nil)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	slices.Sort(elems)

	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
}

func TestReadProducerLimit(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, err := readProducerLimit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2, 3})
}

func TestReadProducerLimit_UpstreamIsShorter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2}), nil
		},

		expected: func() []byte {
			return []byte{1, 2}
		},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, err := readProducerLimit(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 2})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2})
}

func TestReadProducerSkip(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, err := readProducerSkip(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{4, 5})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{4, 5})
}

func TestReadProducerSkip_UpstreamIsShorter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2}), nil
		},

		expected: func() []byte {
			return []byte{1, 2}
		},
	}

	fuzzInput := []byte{3}

	fuzzProd, fuzzInput, err := readProducerSkip(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.True(len(elems) == 0)
}

func TestReadProducerSort(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{3, 1, 5, 2, 4}), nil
		},

		expected: func() []byte {
			return []byte{3, 1, 5, 2, 4}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerSort(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2, 3, 4, 5})
}

func TestReadProducerMap(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{10, 20, 30, 40, 50}), nil
		},

		expected: func() []byte {
			return []byte{10, 20, 30, 40, 50}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerMap(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{5, 10, 15, 20, 25})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{5, 10, 15, 20, 25})
}

func TestReadProducerMapConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{10, 20, 30, 40, 50}), nil
		},

		expected: func() []byte {
			return []byte{10, 20, 30, 40, 50}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerMapConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{3, 6, 10, 13, 16})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	slices.Sort(elems)
	is.Equal(elems, []byte{3, 6, 10, 13, 16})
}

func TestReadProducerJoin(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}, []byte{6, 7, 8, 9, 10}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerJoin(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func TestReadProducerJoinConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}, []byte{6, 7, 8, 9, 10}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerJoinConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	slices.Sort(elems)

	is.Equal(elems, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
}

func TestReadProducerFilter(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerFilter(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{2, 4})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{2, 4})
}

func TestReadProducerFilterConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerFilterConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{2, 4})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	slices.Sort(elems)
	is.Equal(elems, []byte{2, 4})
}

func TestReadProducerDistinct(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 3, 2, 4, 1, 3, 4, 3, 2, 5, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 3, 2, 4, 1, 3, 4, 3, 2, 5, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerDistinct(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 3, 2, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 3, 2, 4, 5})
}

func TestReadProducerFlatMap(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{10, 20, 30, 40, 50}), nil
		},

		expected: func() []byte {
			return []byte{10, 20, 30, 40, 50}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerFlatMap(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{10, 2, 20, 5, 30, 7, 40, 10, 50, 12})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{10, 2, 20, 5, 30, 7, 40, 10, 50, 12})
}

func TestReadProducerFlatMapConcurrent(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{10, 20, 30, 40, 50}), nil
		},

		expected: func() []byte {
			return []byte{10, 20, 30, 40, 50}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerFlatMapConcurrent(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{10, 2, 20, 5, 30, 7, 40, 10, 50, 12})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	slices.Sort(elems)
	is.Equal(elems, []byte{2, 5, 7, 10, 10, 12, 20, 30, 40, 50})
}

func TestReadProducerPeek(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerPeek(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzProd.expected(), []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, _ := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2, 3, 4, 5})
}

func TestReadProducerCancel(t *testing.T) {
	is := is.New(t)

	upstream := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzProd, fuzzInput, err := readProducerCancel(t, fuzzInput, &upstream)
	is.Equal(fuzzProd.upstream, &upstream)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	prods, _ := fuzzProd.create(ctx)
	elems, err := ReduceSlice(ctx, prods)
	is.Equal(elems, []byte{1, 2})
	is.True(errors.Is(err, errTestCancel))
}

func TestReadConsumerReduceSlice(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}, []byte{6, 7, 8, 9, 10}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerReduceSlice(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerCollectMap(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerCollectMap(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerCollectMapNoDuplicateKeys(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerCollectMapNoDuplicateKeys(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerCollectGroup(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{10, 11, 12, 13, 14}), nil
		},

		expected: func() []byte {
			return []byte{10, 11, 12, 13, 14}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerCollectGroup(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerCollectPartition(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{10, 11, 12, 13, 14}), nil
		},

		expected: func() []byte {
			return []byte{10, 11, 12, 13, 14}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerCollectPartition(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerAnyMatch(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5, 100, 101, 102, 103, 104}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5, 100, 101, 102, 103, 104}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerAnyMatch(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerAllMatch(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5, 100, 101, 102, 103, 104}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5, 100, 101, 102, 103, 104}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerAllMatch(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerCount(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerCount(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerFirst(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{1, 2, 3, 4, 5}), nil
		},

		expected: func() []byte {
			return []byte{1, 2, 3, 4, 5}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerFirst(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadConsumerFirst_NoElements(t *testing.T) {
	is := is.New(t)

	fuzzProd := fuzzProducer{
		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce([]byte{}), nil
		},

		expected: func() []byte {
			return []byte{}
		},
	}

	fuzzInput := []byte{}

	fuzzCons, err := readConsumerFirst(t, &fuzzProd, fuzzInput)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)

	ctx := context.Background()
	is.NoErr(fuzzCons.test(ctx))
}

func TestReadSlices(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{3,
		3, 1, 2, 3,
		4, 4, 5, 6, 7,
		5, 8, 9, 10, 11, 12,
	}

	slices, fuzzInput, err := readSlices(t, fuzzInput)
	is.Equal(slices, [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11, 12}})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)
}

func TestReadSlice(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{5, 1, 2, 3, 4, 5}

	input, fuzzInput, err := readSlice(t, fuzzInput)
	is.Equal(input, []byte{1, 2, 3, 4, 5})
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)
}

func TestPeekInt(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{123}

	i, err := peekInt(t, fuzzInput)
	is.Equal(i, 123)
	is.NoErr(err)
}

func TestReadInt(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{123}

	i, fuzzInput, err := readInt(t, fuzzInput)
	is.Equal(i, 123)
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)
}

func TestReadByte(t *testing.T) {
	is := is.New(t)

	fuzzInput := []byte{123}

	b, fuzzInput, err := readByte(t, fuzzInput)
	is.Equal(b, byte(123))
	is.Equal(fuzzInput, []byte{})
	is.NoErr(err)
}
