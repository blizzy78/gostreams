package gostreams

// nice -n 19 go test '-run=^$' -fuzz=FuzzAll -cover

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"golang.org/x/exp/slices"
)

// reduce these to lower memory usage
const (
	maxPipelineLength   = 20
	maxSlices           = 5
	maxSliceLength      = 10
	maxMemoryIntensives = 2
)

const (
	SliceProducerType = byte(iota + 1)
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

	NoMoreProducers = byte(255)
)

const (
	ReduceSliceConsumerType = byte(iota + 1)
	CollectMapConsumerType
	CollectMapNoDuplicateKeysConsumerType
	CollectGroupConsumerType
	CollectPartitionConsumerType
	AnyMatchConsumerType
	AllMatchConsumerType
	CountConsumerType
)

const (
	orderUnstableFlag = flag(1 << iota)
	orderStableFlag

	memoryIntensiveFlag

	cancelFlag
)

type fuzzConsumer struct {
	describe func() string
	test     func(context.Context) error
}

type fuzzProducer struct {
	describe    func() string
	upstream    *fuzzProducer
	flags       flag
	create      func(context.Context) ProducerFunc[byte]
	expected    []byte
	acceptedErr error
}

type flag uint8

type unexpectedResultError[T any] struct {
	actual   T
	expected T
}

var producerTypeToFunc = map[byte]func(*testing.T, []byte, *fuzzProducer) (*fuzzProducer, []byte, error){
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

var consumerTypeToFunc = map[byte]func(*testing.T, *fuzzProducer, []byte) (*fuzzConsumer, []byte, error){
	ReduceSliceConsumerType:               readConsumerReduceSlice,
	CollectMapConsumerType:                readConsumerCollectMap,
	CollectMapNoDuplicateKeysConsumerType: readConsumerCollectMapNoDuplicateKeys,
	CollectGroupConsumerType:              readConsumerCollectGroup,
	CollectPartitionConsumerType:          readConsumerCollectPartition,
	AnyMatchConsumerType:                  readConsumerAnyMatch,
	AllMatchConsumerType:                  readConsumerAllMatch,
	CountConsumerType:                     readConsumerCount,
}

var (
	errFuzzInput  = errors.New("invalid fuzz testing input")
	errTestCancel = errors.New("test cancel")
)

func FuzzAll(f *testing.F) {
	f.Fuzz(func(t *testing.T, fuzzInput []byte) {
		origFuzzInput := fuzzInput

		fuzzProd, fuzzInput, err := readProducer(t, fuzzInput)
		if err != nil {
			if errors.Is(err, errFuzzInput) {
				t.SkipNow()
				return
			}

			t.Fatalf("%+v: %s", origFuzzInput, err.Error())
			return
		}

		fuzzCons, fuzzInput, err := readConsumer(t, fuzzProd, fuzzInput)
		if err != nil {
			if errors.Is(err, errFuzzInput) {
				t.SkipNow()
				return
			}

			t.Fatalf("%+v: %s", origFuzzInput, err.Error())
			return
		}

		// reject extra input
		if len(fuzzInput) != 0 {
			t.SkipNow()
			return
		}

		if err := fuzzCons.test(context.Background()); err != nil {
			t.Fatalf("%+v: %s -> %s: %s", origFuzzInput, fuzzProd.describe(), fuzzCons.describe(), err.Error())
		}
	})
}

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

		next, err := peekByte(t, fuzzInput)
		if err != nil {
			return nil, nil, err
		}

		if next == NoMoreProducers {
			_, fuzzInput, err = readByte(t, fuzzInput)
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

	typ, fuzzInput, err := readByte(t, fuzzInput)
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

	expected := []byte{}
	for _, s := range slices {
		expected = append(expected, s...)
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

		create: func(_ context.Context) ProducerFunc[byte] {
			return Produce(slices...)
		},

		expected: expected,
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

	expected := []byte{}
	for _, s := range slices {
		expected = append(expected, s...)
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

		create: func(_ context.Context) ProducerFunc[byte] {
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

			return ProduceChannel(channels...)
		},

		expected: expected,
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

	expected := []byte{}
	for _, s := range slices {
		expected = append(expected, s...)
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

		create: func(_ context.Context) ProducerFunc[byte] {
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

			return ProduceChannelConcurrent(channels...)
		},

		expected: expected,
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

	max, fuzzInput, err := readByte(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	end := int(max)
	if len(upstream.expected) < end {
		end = len(upstream.expected)
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> limit(%d)", upstream.describe(), int(max))
		},

		upstream: upstream,

		create: func(ctx context.Context) ProducerFunc[byte] {
			return Limit(upstream.create(ctx), uint64(max))
		},

		expected: upstream.expected[:end],
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

	num, fuzzInput, err := readByte(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	start := int(num)
	if len(upstream.expected) < start {
		start = len(upstream.expected)
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> skip(%d)", upstream.describe(), int(num))
		},

		upstream: upstream,

		create: func(ctx context.Context) ProducerFunc[byte] {
			return Skip(upstream.create(ctx), uint64(num))
		},

		expected: upstream.expected[start:],
	}, fuzzInput, nil
}

func readProducerSort(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := make([]byte, len(upstream.expected))
	copy(expected, upstream.expected)
	slices.Sort(expected)

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> sort"
		},

		upstream: upstream,
		flags:    orderStableFlag,

		create: func(ctx context.Context) ProducerFunc[byte] {
			less := func(_ context.Context, _ context.CancelCauseFunc, a byte, b byte) bool {
				return a < b
			}

			return Sort(upstream.create(ctx), less)
		},

		expected: expected,
	}, fuzzInput, nil
}

func readProducerMap(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := make([]byte, len(upstream.expected))
	for i, b := range upstream.expected {
		expected[i] = b / 2
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> map"
		},

		upstream: upstream,

		create: func(ctx context.Context) ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) byte {
				return elem / 2
			}

			return Map(upstream.create(ctx), mapp)
		},

		expected: expected,
	}, fuzzInput, nil
}

func readProducerMapConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := make([]byte, len(upstream.expected))
	for i, b := range upstream.expected {
		expected[i] = b / 3
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> mapConcurrent"
		},

		upstream: upstream,
		flags:    orderUnstableFlag,

		create: func(ctx context.Context) ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) byte {
				return elem / 3
			}

			return MapConcurrent(upstream.create(ctx), mapp)
		},

		expected: expected,
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

		create: func(ctx context.Context) ProducerFunc[byte] {
			return Join(upstream.create(ctx))
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

		create: func(ctx context.Context) ProducerFunc[byte] {
			return JoinConcurrent(upstream.create(ctx))
		},

		expected: upstream.expected,
	}, fuzzInput, nil
}

func readProducerFilter(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := []byte{}
	for _, b := range upstream.expected {
		if b%2 != 0 {
			continue
		}

		expected = append(expected, b)
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> filter"
		},

		upstream: upstream,

		create: func(ctx context.Context) ProducerFunc[byte] {
			even := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) bool {
				return elem%2 == 0
			}

			return Filter(upstream.create(ctx), even)
		},

		expected: expected,
	}, fuzzInput, nil
}

func readProducerFilterConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := []byte{}
	for _, b := range upstream.expected {
		if b%2 != 0 {
			continue
		}

		expected = append(expected, b)
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> filterConcurrent"
		},

		upstream: upstream,
		flags:    orderUnstableFlag,

		create: func(ctx context.Context) ProducerFunc[byte] {
			even := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) bool {
				return elem%2 == 0
			}

			return FilterConcurrent(upstream.create(ctx), even)
		},

		expected: expected,
	}, fuzzInput, nil
}

func readProducerDistinct(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := make([]byte, 0, len(upstream.expected))

	for _, byt := range upstream.expected {
		if slices.Contains(expected, byt) {
			continue
		}

		expected = append(expected, byt)
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> distinct"
		},

		upstream: upstream,

		create: func(ctx context.Context) ProducerFunc[byte] {
			return Distinct(upstream.create(ctx))
		},

		expected: expected,
	}, fuzzInput, nil
}

func readProducerFlatMap(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := make([]byte, len(upstream.expected)*2)
	for i, b := range upstream.expected {
		expected[i*2] = b
		expected[i*2+1] = b / 4
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> flatMap"
		},

		upstream: upstream,
		flags:    memoryIntensiveFlag,

		create: func(ctx context.Context) ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) ProducerFunc[byte] {
				return Produce([]byte{elem, elem / 4})
			}

			return FlatMap(upstream.create(ctx), mapp)
		},

		expected: expected,
	}, fuzzInput, nil
}

func readProducerFlatMapConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, error) {
	t.Helper()

	if upstream == nil {
		return nil, nil, errFuzzInput
	}

	expected := make([]byte, len(upstream.expected)*2)
	for i, b := range upstream.expected {
		expected[i*2] = b
		expected[i*2+1] = b / 4
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> flatMap"
		},

		upstream: upstream,
		flags:    orderUnstableFlag | memoryIntensiveFlag,

		create: func(ctx context.Context) ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) ProducerFunc[byte] {
				return Produce([]byte{elem, elem / 4})
			}

			return FlatMapConcurrent(upstream.create(ctx), mapp)
		},

		expected: expected,
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

		create: func(ctx context.Context) ProducerFunc[byte] {
			return Peek(upstream.create(ctx), func(_ context.Context, _ context.CancelCauseFunc, _ byte, _ uint64) {})
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

	if len(upstream.expected) < 2 {
		return nil, nil, errFuzzInput
	}

	max := len(upstream.expected) / 2

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> cancel(%d)", upstream.describe(), max)
		},

		upstream:    upstream,
		flags:       cancelFlag,
		acceptedErr: errTestCancel,

		create: func(ctx context.Context) ProducerFunc[byte] {
			num := uint64(0)

			return Peek(upstream.create(ctx), func(_ context.Context, cancel context.CancelCauseFunc, elem byte, index uint64) {
				if num < uint64(max) {
					num++
					return
				}

				cancel(errTestCancel)
			})
		},

		expected: upstream.expected[:max],
	}, fuzzInput, nil
}

func readConsumer(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) {
	t.Helper()

	typ, fuzzInput, err := readByte(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	f, ok := consumerTypeToFunc[typ]
	if !ok {
		return nil, nil, errFuzzInput
	}

	return f(t, fuzzProd, fuzzInput)
}

func readConsumerReduceSlice(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "reduceSlice"
		},

		test: func(ctx context.Context) error {
			expected := fuzzProd.expected

			if !fuzzProd.stableOrder() {
				expected = make([]byte, len(fuzzProd.expected))
				copy(expected, fuzzProd.expected)
				slices.Sort(expected)
			}

			prod := fuzzProd.create(ctx)

			elems, err := ReduceSlice(ctx, prod)
			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			if !fuzzProd.stableOrder() {
				slices.Sort(elems)
			}

			if !equalSlices(t, elems, expected) {
				return &unexpectedResultError[[]byte]{
					actual:   elems,
					expected: expected,
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerCollectMap(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:gocognit // map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectMap"
		},

		test: func(ctx context.Context) error {
			expected := map[byte]byte{}
			for _, b := range fuzzProd.expected {
				expected[b*2] = b * 3
			}

			prod := fuzzProd.create(ctx)

			result, err := Reduce(
				ctx, prod,
				CollectMap(
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem * 2
					},
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem * 3
					},
				),
			)

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			if len(result) != len(expected) {
				return &unexpectedResultError[map[byte]byte]{
					actual:   result,
					expected: expected,
				}
			}

			for k, rv := range result {
				if ev, ok := expected[k]; !ok || rv != ev {
					return &unexpectedResultError[map[byte]byte]{
						actual:   result,
						expected: expected,
					}
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerCollectMapNoDuplicateKeys(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:gocognit,cyclop // must match signature; map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectMapNoDuplicateKeys"
		},

		test: func(ctx context.Context) error {
			expected := map[byte]byte{}

			expectDuplicateKeyErr := false

			for _, b := range fuzzProd.expected { //nolint:varnamelen // b is fine here
				if _, ok := expected[b*2]; ok {
					expectDuplicateKeyErr = true
					break
				}

				expected[b*2] = b * 3
			}

			prod := fuzzProd.create(ctx)

			result, err := Reduce(
				ctx, prod,
				CollectMapNoDuplicateKeys(
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem * 2
					},
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem * 3
					},
				),
			)

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				if expectDuplicateKeyErr {
					var dupKeyError *DuplicateKeyError[byte, byte]
					if errors.As(err, &dupKeyError) {
						return nil
					}
				}

				return err
			}

			if len(result) != len(expected) {
				return &unexpectedResultError[map[byte]byte]{
					actual:   result,
					expected: expected,
				}
			}

			for k, rv := range result {
				if ev, ok := expected[k]; !ok || rv != ev {
					return &unexpectedResultError[map[byte]byte]{
						actual:   result,
						expected: expected,
					}
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerCollectGroup(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:gocognit // map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectGroup"
		},

		test: func(ctx context.Context) error {
			expected := map[byte][]byte{}
			for _, b := range fuzzProd.expected {
				expected[b%10] = append(expected[b%10], b)
			}

			prod := fuzzProd.create(ctx)

			result, err := Reduce(
				ctx, prod,
				CollectGroup(
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem % 10
					},
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem
					},
				),
			)

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			if len(result) != len(expected) {
				return &unexpectedResultError[map[byte][]byte]{
					actual:   result,
					expected: expected,
				}
			}

			for k, rv := range result {
				if ev, ok := expected[k]; !ok || !slices.Equal(rv, ev) {
					return &unexpectedResultError[map[byte][]byte]{
						actual:   result,
						expected: expected,
					}
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerCollectPartition(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:gocognit // map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectPartition"
		},

		test: func(ctx context.Context) error {
			expected := map[bool][]byte{}
			for _, b := range fuzzProd.expected {
				expected[b%2 == 0] = append(expected[b%2 == 0], b)
			}

			prod := fuzzProd.create(ctx)

			result, err := Reduce(
				ctx, prod,
				CollectPartition(
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) bool {
						return elem%2 == 0
					},
					func(_ context.Context, _ context.CancelCauseFunc, elem byte, index uint64) byte {
						return elem
					},
				),
			)

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			if len(result) != len(expected) {
				return &unexpectedResultError[map[bool][]byte]{
					actual:   result,
					expected: expected,
				}
			}

			for k, rv := range result {
				if ev, ok := expected[k]; !ok || !slices.Equal(rv, ev) {
					return &unexpectedResultError[map[bool][]byte]{
						actual:   result,
						expected: expected,
					}
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerAnyMatch(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "anyMatch"
		},

		test: func(ctx context.Context) error {
			prod := fuzzProd.create(ctx)

			match, err := AnyMatch(ctx, prod, func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) bool {
				return elem >= 100
			})

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			expected := false
			for _, b := range fuzzProd.expected {
				if b < 100 {
					continue
				}

				expected = true
				break
			}

			if match != expected {
				return &unexpectedResultError[bool]{
					actual:   match,
					expected: expected,
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerAllMatch(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "allMatch"
		},

		test: func(ctx context.Context) error {
			prod := fuzzProd.create(ctx)

			allMatch, err := AllMatch(ctx, prod, func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) bool {
				return elem >= 100
			})

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			expected := true
			for _, b := range fuzzProd.expected {
				if b < 100 {
					expected = false
					break
				}
			}

			if allMatch != expected {
				return &unexpectedResultError[bool]{
					actual:   allMatch,
					expected: expected,
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readConsumerCount(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, []byte, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "allMatch"
		},

		test: func(ctx context.Context) error {
			prod := fuzzProd.create(ctx)

			count, err := Count(ctx, prod)
			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			if int(count) != len(fuzzProd.expected) {
				return &unexpectedResultError[int]{
					actual:   int(count),
					expected: len(fuzzProd.expected),
				}
			}

			return nil
		},
	}, fuzzInput, nil
}

func readSlices(t *testing.T, fuzzInput []byte) ([][]byte, []byte, error) {
	t.Helper()

	num, fuzzInput, err := readByte(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	if int(num) > maxSlices {
		return nil, nil, errFuzzInput
	}

	slices := make([][]byte, int(num))

	for idx := 0; idx < int(num); idx++ {
		var (
			slice []byte
			err   error
		)

		slice, fuzzInput, err = readSlice(t, fuzzInput)
		if err != nil {
			return nil, nil, err
		}

		slices[idx] = slice
	}

	return slices, fuzzInput, nil
}

func readSlice(t *testing.T, fuzzInput []byte) ([]byte, []byte, error) {
	t.Helper()

	length, fuzzInput, err := readByte(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	if len(fuzzInput) < int(length) {
		return nil, nil, errFuzzInput
	}

	return fuzzInput[:int(length)], fuzzInput[int(length):], nil
}

func peekByte(t *testing.T, fuzzInput []byte) (byte, error) {
	t.Helper()

	b, _, err := readByte(t, fuzzInput)
	if err != nil {
		return 0, err
	}

	return b, nil
}

func readByte(t *testing.T, fuzzInput []byte) (byte, []byte, error) {
	t.Helper()

	if len(fuzzInput) == 0 {
		return 0, nil, errFuzzInput
	}

	return fuzzInput[0], fuzzInput[1:], nil
}

func equalSlices[T comparable](t *testing.T, first []T, second []T) bool {
	t.Helper()

	if first == nil {
		first = []T{}
	}

	if second == nil {
		second = []T{}
	}

	if len(first) != len(second) {
		return false
	}

	for idx := range first {
		if first[idx] != second[idx] {
			return false
		}
	}

	return true
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

func (e *unexpectedResultError[T]) Error() string {
	return fmt.Sprintf("%+v: unexpected result: expected %+v", e.actual, e.expected)
}
