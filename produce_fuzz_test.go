package gostreams

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

const (
	SliceProducerType = iota + 1
	ChannelProducerType
	ChannelConcurrentProducerType
	LimitProducerType
	SkipProducerType
	SortProducerType
	MapProducerType
	MapConcurrentProducerType
	JoinProducerType
	JoinConcurrentProducerType
	FilterProducerType
	FilterConcurrentProducerType
	DistinctProducerType
	FlatMapProducerType
	FlatMapConcurrentProducerType
	PeekProducerType
	CancelProducerType

	NoMoreProducers = 255
)

const (
	orderUnstableFlag = producerFlag(1 << iota)
	orderStableFlag

	memoryIntensiveFlag

	cancelFlag
)

type fuzzProducer struct {
	describe    func() string
	upstream    *fuzzProducer
	flags       producerFlag
	create      func(context.Context) (ProducerFunc[byte], error)
	expected    func() []byte
	acceptedErr error
}

type producerFlag uint8

var producerTypeToFunc = map[int]func(*testing.T, []byte, *fuzzProducer) (*fuzzProducer, []byte, error){
	SliceProducerType:             readProducerSlice,
	ChannelProducerType:           readProducerChannel,
	ChannelConcurrentProducerType: readProducerChannelConcurrent,
	LimitProducerType:             readProducerLimit,
	SkipProducerType:              readProducerSkip,
	SortProducerType:              readProducerSort,
	MapProducerType:               readProducerMap,
	MapConcurrentProducerType:     readProducerMapConcurrent,
	JoinProducerType:              readProducerJoin,
	JoinConcurrentProducerType:    readProducerJoinConcurrent,
	FilterProducerType:            readProducerFilter,
	FilterConcurrentProducerType:  readProducerFilterConcurrent,
	DistinctProducerType:          readProducerDistinct,
	FlatMapProducerType:           readProducerFlatMap,
	FlatMapConcurrentProducerType: readProducerFlatMapConcurrent,
	PeekProducerType:              readProducerPeek,
	CancelProducerType:            readProducerCancel,
}

var errTestCancel = errors.New("test cancel")

func readProducer(t *testing.T, fuzzInput []byte) (*fuzzProducer, []byte, error) {
	t.Helper()

	var fuzzProd *fuzzProducer

	for {
		var err error

		fuzzProd, fuzzInput, err = readProducerWithUpstream(t, fuzzInput, fuzzProd)
		if err != nil {
			return nil, nil, err
		}

		if !acceptProducerPipeline(t, fuzzProd) {
			return nil, nil, errFuzzInput
		}

		next, err := peekInt(t, fuzzInput)
		if err != nil {
			return nil, nil, err
		}

		if next == NoMoreProducers {
			_, fuzzInput, err = readInt(t, fuzzInput)
			if err != nil {
				return nil, nil, err
			}

			break
		}
	}

	return fuzzProd, fuzzInput, nil
}

func acceptProducerPipeline(t *testing.T, fuzzProd *fuzzProducer) bool {
	t.Helper()

	if fuzzProd.length() > maxPipelineLength {
		return false
	}

	if fuzzProd.memoryIntensives() > maxMemoryIntensives {
		return false
	}

	if fuzzProd.cancels() > 1 {
		return false
	}

	return true
}

func readProducerWithUpstream(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	typ, fuzzInput, err := readInt(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	f, ok := producerTypeToFunc[typ]
	if !ok {
		return nil, fuzzInput, errFuzzInput
	}

	return f(t, fuzzInput, upstream)
}

func readProducerSlice(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream != nil {
		return nil, nil, errFuzzInput
	}

	slices, fuzzInput, err := readSlices(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	return &fuzzProducer{
		describe: func() string {
			total := 0
			for _, s := range slices {
				total += len(s)
			}

			return fmt.Sprintf("produceSlice(%d slices, %d total)", len(slices), total)
		},

		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			return Produce(slices...), nil
		},

		expected: func() []byte {
			expected := []byte{}
			for _, s := range slices {
				expected = append(expected, s...)
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerChannel(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream != nil {
		return nil, nil, errFuzzInput
	}

	slices, fuzzInput, err := readSlices(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	return &fuzzProducer{
		describe: func() string {
			total := 0
			for _, s := range slices {
				total += len(s)
			}

			return fmt.Sprintf("produceChannel(%d slices, %d total)", len(slices), total)
		},

		flags: orderStableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			channels := make([]<-chan byte, len(slices))

			for idx, slice := range slices {
				ch := make(chan byte)

				go func(ch chan byte, slice []byte) {
					defer close(ch)

					for _, b := range slice {
						ch <- b
					}
				}(ch, slice)

				channels[idx] = ch
			}

			return ProduceChannel(channels...), nil
		},

		expected: func() []byte {
			expected := []byte{}
			for _, s := range slices {
				expected = append(expected, s...)
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerChannelConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream != nil {
		return nil, nil, errFuzzInput
	}

	slices, fuzzInput, err := readSlices(t, fuzzInput)
	if err != nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			total := 0
			for _, s := range slices {
				total += len(s)
			}

			return fmt.Sprintf("produceChannelConcurrent(%d slices, %d total)", len(slices), total)
		},

		flags: orderUnstableFlag,

		create: func(_ context.Context) (ProducerFunc[byte], error) {
			channels := make([]<-chan byte, len(slices))

			for idx, slice := range slices {
				ch := make(chan byte)

				go func(ch chan byte, slice []byte) {
					defer close(ch)

					for _, b := range slice {
						ch <- b
					}
				}(ch, slice)

				channels[idx] = ch
			}

			return ProduceChannelConcurrent(channels...), nil
		},

		expected: func() []byte {
			expected := []byte{}
			for _, s := range slices {
				expected = append(expected, s...)
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerLimit(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	if !upstream.stableOrder() {
		return nil, nil, errFuzzInput
	}

	max, fuzzInput, err := readInt(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> limit(%d)", upstream.describe(), max)
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return Limit(upstreamProd, uint64(max)), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			end := max
			if len(upstreamExpected) < end {
				end = len(upstreamExpected)
			}

			return upstreamExpected[:end]
		},
	}, fuzzInput, nil
}

func readProducerSkip(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	if !upstream.stableOrder() {
		return nil, nil, errFuzzInput
	}

	num, fuzzInput, err := readInt(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> skip(%d)", upstream.describe(), num)
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return Skip(upstreamProd, uint64(num)), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			start := num
			if len(upstreamExpected) < start {
				start = len(upstreamExpected)
			}

			return upstreamExpected[start:]
		},
	}, fuzzInput, nil
}

func readProducerSort(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> sort"
		},

		upstream: upstream,
		flags:    orderStableFlag,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			cmp := func(_ context.Context, _ context.CancelCauseFunc, a byte, b byte) int {
				return int(a) - int(b)
			}

			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return Sort(upstreamProd, cmp), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			expected := make([]byte, len(upstreamExpected))
			copy(expected, upstreamExpected)
			slices.Sort(expected)

			return expected
		},
	}, fuzzInput, nil
}

func readProducerMap(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> map"
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			mapp := FuncMapper(func(elem byte) byte {
				return elem / 2
			})

			return Map(upstreamProd, mapp), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			expected := make([]byte, len(upstreamExpected))
			for i, b := range upstreamExpected {
				expected[i] = b / 2
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerMapConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> mapConcurrent"
		},

		upstream: upstream,
		flags:    orderUnstableFlag,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			mapp := FuncMapper(func(elem byte) byte {
				return elem / 3
			})

			return MapConcurrent(upstreamProd, mapp), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			expected := make([]byte, len(upstreamExpected))
			for i, b := range upstreamExpected {
				expected[i] = b / 3
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerJoin(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> join"
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return Join(upstreamProd), nil
		},

		expected: upstream.expected,
	}, fuzzInput, nil
}

func readProducerJoinConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> joinConcurrent"
		},

		upstream: upstream,
		flags:    orderUnstableFlag,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return JoinConcurrent(upstreamProd), nil
		},

		expected: upstream.expected,
	}, fuzzInput, nil
}

func readProducerFilter(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> filter"
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			even := FuncPredicate(func(elem byte) bool {
				return elem%2 == 0
			})

			return Filter(upstreamProd, even), nil
		},

		expected: func() []byte {
			expected := []byte{}
			for _, b := range upstream.expected() {
				if b%2 != 0 {
					continue
				}

				expected = append(expected, b)
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerFilterConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> filterConcurrent"
		},

		upstream: upstream,
		flags:    orderUnstableFlag,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			even := FuncPredicate(func(elem byte) bool {
				return elem%2 == 0
			})

			return FilterConcurrent(upstreamProd, even), nil
		},

		expected: func() []byte {
			expected := []byte{}
			for _, b := range upstream.expected() {
				if b%2 != 0 {
					continue
				}

				expected = append(expected, b)
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerDistinct(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> distinct"
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return Distinct(upstreamProd), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			expected := make([]byte, 0, len(upstreamExpected))

			for _, byt := range upstreamExpected {
				if slices.Contains(expected, byt) {
					continue
				}

				expected = append(expected, byt)
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerFlatMap(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> flatMap"
		},

		upstream: upstream,
		flags:    memoryIntensiveFlag,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			mapp := FuncMapper(func(elem byte) ProducerFunc[byte] {
				return Produce([]byte{elem, elem / 4})
			})

			return FlatMap(upstreamProd, mapp), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			expected := make([]byte, len(upstreamExpected)*2)
			for i, b := range upstreamExpected {
				expected[i*2] = b
				expected[i*2+1] = b / 4
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerFlatMapConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> flatMap"
		},

		upstream: upstream,
		flags:    orderUnstableFlag | memoryIntensiveFlag,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			mapp := FuncMapper(func(elem byte) ProducerFunc[byte] {
				return Produce([]byte{elem, elem / 4})
			})

			return FlatMapConcurrent(upstreamProd, mapp), nil
		},

		expected: func() []byte {
			upstreamExpected := upstream.expected()

			expected := make([]byte, len(upstreamExpected)*2)
			for i, b := range upstreamExpected {
				expected[i*2] = b
				expected[i*2+1] = b / 4
			}

			return expected
		},
	}, fuzzInput, nil
}

func readProducerPeek(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> peek"
		},

		upstream: upstream,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			return Peek(upstreamProd, func(_ context.Context, _ context.CancelCauseFunc, _ byte, _ uint64) {}), nil
		},

		expected: upstream.expected,
	}, fuzzInput, nil
}

func readProducerCancel(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	if !upstream.stableOrder() {
		return nil, nil, errFuzzInput
	}

	max := 0

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> cancel(%d)", upstream.describe(), max)
		},

		upstream:    upstream,
		flags:       cancelFlag,
		acceptedErr: errTestCancel,

		create: func(ctx context.Context) (ProducerFunc[byte], error) {
			upstreamExpected := upstream.expected()

			if len(upstreamExpected) < 2 {
				return nil, errFuzzInput
			}

			max = len(upstreamExpected) / 2

			upstreamProd, err := upstream.create(ctx)
			if err != nil {
				return nil, err
			}

			num := uint64(0)

			return Peek(upstreamProd, func(_ context.Context, cancel context.CancelCauseFunc, elem byte, index uint64) {
				if num < uint64(max) {
					num++
					return
				}

				cancel(errTestCancel)
			}), nil
		},

		expected: func() []byte {
			return upstream.expected()[:max]
		},
	}, fuzzInput, nil
}

func (p *fuzzProducer) stableOrder() bool {
	for p := p; p != nil; p = p.upstream {
		if p.flags&orderStableFlag != 0 {
			return true
		}

		if p.flags&orderUnstableFlag != 0 {
			return false
		}
	}

	panic("cannot determine order")
}

func (p *fuzzProducer) length() int {
	length := 0
	for p := p; p != nil; p = p.upstream {
		length++
	}

	return length
}

func (p *fuzzProducer) memoryIntensives() int {
	num := 0

	for p := p; p != nil; p = p.upstream {
		if p.flags&memoryIntensiveFlag == 0 {
			continue
		}

		num++
	}

	return num
}

func (p *fuzzProducer) cancels() int {
	num := 0

	for p := p; p != nil; p = p.upstream {
		if p.flags&cancelFlag == 0 {
			continue
		}

		num++
	}

	return num
}

func (p *fuzzProducer) acceptedError() error {
	for p := p; p != nil; p = p.upstream {
		if p.acceptedErr == nil {
			continue
		}

		return p.acceptedErr
	}

	return nil
}
