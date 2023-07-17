package gostreams

// nice -n 19 go test '-run=^$' -fuzz=FuzzAll -cover

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
	SplitProducerType
	JoinProducerType
	JoinConcurrentProducerType
	TeeProducerType
	FilterProducerType
	FilterConcurrentProducerType
	DistinctProducerType
	FlatMapProducerType
	FlatMapConcurrentProducerType
)

const (
	orderLikeUpstream = order(iota)
	orderUnstable
	orderStable
)

const (
	multipleLikeUpstream = multiple(iota)
	multipleNo
	multipleYes
)

const (
	joinLikeUpstream = join(iota)
	joinAny
	joinConcurrent
)

type fuzzProducer struct {
	describe        func() string
	upstream        *fuzzProducer
	order           order
	multiple        multiple
	join            join
	memoryIntensive bool
	create          func(context.Context) []ProducerFunc[byte]
	expected        []byte
}

type order int

type multiple int

type join int

type unexpectedResultError struct {
	actual   []byte
	expected []byte
}

var producerTypeToFunc = map[byte]func(*testing.T, []byte, *fuzzProducer) (*fuzzProducer, []byte, bool){
	SliceProducerType:             readProducerSlice,
	ChannelProducerType:           readProducerChannel,
	ChannelConcurrentProducerType: readProducerChannelConcurrent,
	LimitProducerType:             readProducerLimit,
	SkipProducerType:              readProducerSkip,
	SortProducerType:              readProducerSort,
	MapProducerType:               readProducerMap,
	MapConcurrentProducerType:     readProducerMapConcurrent,
	SplitProducerType:             readProducerSplit,
	JoinProducerType:              readProducerJoin,
	JoinConcurrentProducerType:    readProducerJoinConcurrent,
	TeeProducerType:               readProducerTee,
	FilterProducerType:            readProducerFilter,
	FilterConcurrentProducerType:  readProducerFilterConcurrent,
	DistinctProducerType:          readProducerDistinct,
	FlatMapProducerType:           readProducerFlatMap,
	FlatMapConcurrentProducerType: readProducerFlatMapConcurrent,
}

func FuzzAll(f *testing.F) {
	f.Fuzz(func(t *testing.T, fuzzInput []byte) {
		tmp := make([]byte, len(fuzzInput))
		copy(tmp, fuzzInput)
		fuzzInput = tmp

		origFuzzInput := fuzzInput

		fuzzProd, fuzzInput, ok := readProducer(t, fuzzInput)
		if !ok {
			t.SkipNow()
			return
		}

		// reject extra input
		if len(fuzzInput) != 0 {
			t.SkipNow()
			return
		}

		ctx := context.Background()

		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		if err := testProducer(ctx, t, fuzzProd); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				t.Logf("%+v: %s: took too long", origFuzzInput, fuzzProd.describe())
				t.SkipNow()
				return
			}

			t.Fatalf("%+v: %s: %s", origFuzzInput, fuzzProd.describe(), err.Error())
		}
	})
}

func testProducer(ctx context.Context, t *testing.T, fuzzProd *fuzzProducer) error { //nolint:thelper // not a helper function
	prod := fuzzProd.create(ctx)[0]

	elems, err := ReduceSlice(ctx, prod)
	if err != nil {
		return err
	}

	expected := fuzzProd.expected

	if !fuzzProd.stableOrder() {
		slices.Sort(elems)

		expected = make([]byte, len(fuzzProd.expected))
		copy(expected, fuzzProd.expected)
		slices.Sort(expected)
	}

	if !equalSlices(t, elems, expected) {
		return &unexpectedResultError{
			actual:   elems,
			expected: expected,
		}
	}

	return nil
}

func readProducer(t *testing.T, fuzzInput []byte) (*fuzzProducer, []byte, bool) {
	t.Helper()

	var (
		fuzzProd *fuzzProducer
		ok       bool
	)

	fuzzProd, fuzzInput, ok = readProducerWithUpstream(t, fuzzInput, nil)
	if !ok {
		return nil, nil, false
	}

	for {
		if fuzzProd.length() > maxPipelineLength {
			return nil, nil, false
		}

		if fuzzProd.memoryIntensives() > maxMemoryIntensives {
			return nil, nil, false
		}

		newFuzzProd, newFuzzInput, ok := readProducerWithUpstream(t, fuzzInput, fuzzProd)
		if !ok {
			break
		}

		fuzzProd = newFuzzProd
		fuzzInput = newFuzzInput
	}

	if fuzzProd.multipleYes() {
		return nil, nil, false
	}

	return fuzzProd, fuzzInput, true
}

func readProducerWithUpstream(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if len(fuzzInput) == 0 {
		return nil, nil, false
	}

	typ := fuzzInput[0]
	fuzzInput = fuzzInput[1:]

	f, ok := producerTypeToFunc[typ]
	if !ok {
		return nil, nil, false
	}

	return f(t, fuzzInput, upstream)
}

func readProducerSlice(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream != nil {
		return nil, nil, false
	}

	slices, fuzzInput, ok := readSlices(t, fuzzInput)
	if !ok {
		return nil, nil, false
	}

	expected := []byte{}
	for _, s := range slices {
		expected = append(expected, s...)
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("produceSlice(%d slices)", len(slices))
		},

		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
			return []ProducerFunc[byte]{Produce(slices...)}
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerChannel(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream != nil {
		return nil, nil, false
	}

	slices, fuzzInput, ok := readSlices(t, fuzzInput)
	if !ok {
		return nil, nil, false
	}

	expected := []byte{}
	for _, s := range slices {
		expected = append(expected, s...)
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("produceChannel(%d slices)", len(slices))
		},

		order:    orderStable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
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

			return []ProducerFunc[byte]{ProduceChannel(channels...)}
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerChannelConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream != nil {
		return nil, nil, false
	}

	slices, fuzzInput, ok := readSlices(t, fuzzInput)
	if !ok {
		return nil, nil, false
	}

	expected := []byte{}
	for _, s := range slices {
		expected = append(expected, s...)
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("produceChannelConcurrent(%d slices)", len(slices))
		},

		order:    orderUnstable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(_ context.Context) []ProducerFunc[byte] {
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

			return []ProducerFunc[byte]{ProduceChannelConcurrent(channels...)}
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerLimit(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	if !upstream.stableOrder() {
		return nil, nil, false
	}

	max, fuzzInput, ok := readByte(t, fuzzInput)
	if !ok {
		return nil, nil, false
	}

	expected := upstream.expected
	if int(max) < len(expected) {
		expected = expected[:int(max)]
	}

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> limit(%d)", upstream.describe(), int(max))
		},

		upstream: upstream,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Limit(u, uint64(max))
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerSkip(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	if !upstream.stableOrder() {
		return nil, nil, false
	}

	num, fuzzInput, ok := readByte(t, fuzzInput)
	if !ok {
		return nil, nil, false
	}

	expected := upstream.expected
	start := int(num)

	if len(expected) < start {
		start = len(expected)
	}

	expected = expected[start:]

	return &fuzzProducer{
		describe: func() string {
			return fmt.Sprintf("%s -> skip(%d)", upstream.describe(), int(num))
		},

		upstream: upstream,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Skip(u, uint64(num))
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerSort(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	expected := make([]byte, len(upstream.expected))
	copy(expected, upstream.expected)
	slices.Sort(expected)

	order := orderStable
	if upstream.multipleYes() {
		order = orderLikeUpstream
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> sort"
		},

		upstream: upstream,
		order:    order,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			less := func(_ context.Context, _ context.CancelCauseFunc, a byte, b byte) bool {
				return a < b
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Sort(u, less)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerMap(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
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

		create: func(ctx context.Context) []ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) byte {
				return elem / 2
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Map(u, mapp)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerMapConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
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
		order:    orderUnstable,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) byte {
				return elem / 3
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = MapConcurrent(u, mapp)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerSplit(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> split"
		},

		upstream: upstream,
		order:    orderUnstable,
		multiple: multipleYes,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams)*2)
			for i, u := range upstreams {
				prod1, prod2 := Split(ctx, u)
				prods[i*2] = prod1
				prods[i*2+1] = prod2
			}

			return prods
		},

		expected: upstream.expected,
	}, fuzzInput, true
}

func readProducerJoin(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	if upstream.joinConcurrent() {
		return nil, nil, false
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> join"
		},

		upstream: upstream,
		multiple: multipleNo,
		join:     joinAny,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			prods := upstream.create(ctx)
			return []ProducerFunc[byte]{Join(prods...)}
		},

		expected: upstream.expected,
	}, fuzzInput, true
}

func readProducerJoinConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> joinConcurrent"
		},

		upstream: upstream,
		order:    orderUnstable,
		multiple: multipleNo,
		join:     joinAny,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			prods := upstream.create(ctx)
			return []ProducerFunc[byte]{JoinConcurrent(prods...)}
		},

		expected: upstream.expected,
	}, fuzzInput, true
}

func readProducerTee(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	expected := upstream.expected
	expected = append(expected, upstream.expected...)

	return &fuzzProducer{
		describe: func() string {
			return upstream.describe() + " -> tee"
		},

		upstream:        upstream,
		order:           orderUnstable,
		multiple:        multipleYes,
		join:            joinConcurrent,
		memoryIntensive: true,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams)*2)
			for i, u := range upstreams {
				prod1, prod2 := Tee(ctx, u)
				prods[i*2] = prod1
				prods[i*2+1] = prod2
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerFilter(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
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

		create: func(ctx context.Context) []ProducerFunc[byte] {
			even := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) bool {
				return elem%2 == 0
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Filter(u, even)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerFilterConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
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
		order:    orderUnstable,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			even := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) bool {
				return elem%2 == 0
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Filter(u, even)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerDistinct(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
	}

	// current framework not suitable for multiple upstream producers
	if upstream.multipleYes() {
		return nil, nil, false
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

		create: func(ctx context.Context) []ProducerFunc[byte] {
			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = Distinct(u)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerFlatMap(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
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

		upstream:        upstream,
		memoryIntensive: true,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) ProducerFunc[byte] {
				return Produce([]byte{elem, elem / 4})
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = FlatMap(u, mapp)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readProducerFlatMapConcurrent(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if upstream == nil {
		return nil, nil, false
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

		upstream:        upstream,
		order:           orderUnstable,
		memoryIntensive: true,

		create: func(ctx context.Context) []ProducerFunc[byte] {
			mapp := func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) ProducerFunc[byte] {
				return Produce([]byte{elem, elem / 4})
			}

			upstreams := upstream.create(ctx)

			prods := make([]ProducerFunc[byte], len(upstreams))
			for i, u := range upstreams {
				prods[i] = FlatMapConcurrent(u, mapp)
			}

			return prods
		},

		expected: expected,
	}, fuzzInput, true
}

func readSlices(t *testing.T, fuzzInput []byte) ([][]byte, []byte, bool) {
	t.Helper()

	if len(fuzzInput) == 0 {
		return nil, nil, false
	}

	num := int(fuzzInput[0])
	if num > maxSlices {
		return nil, nil, false
	}

	fuzzInput = fuzzInput[1:]

	slices := make([][]byte, num)

	for idx := 0; idx < num; idx++ {
		var (
			slice []byte
			ok    bool
		)

		slice, fuzzInput, ok = readSlice(t, fuzzInput)
		if !ok {
			return nil, nil, false
		}

		slices[idx] = slice
	}

	return slices, fuzzInput, true
}

func readSlice(t *testing.T, fuzzInput []byte) ([]byte, []byte, bool) {
	t.Helper()

	if len(fuzzInput) == 0 {
		return nil, nil, false
	}

	length := int(fuzzInput[0])
	if length > maxSliceLength {
		return nil, nil, false
	}

	fuzzInput = fuzzInput[1:]

	if len(fuzzInput) < length {
		return nil, nil, false
	}

	input := fuzzInput[:length]

	return input, fuzzInput[length:], true
}

func readByte(t *testing.T, fuzzInput []byte) (byte, []byte, bool) {
	t.Helper()

	if len(fuzzInput) == 0 {
		return 0, nil, false
	}

	return fuzzInput[0], fuzzInput[1:], true
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
		switch p.order {
		case orderUnstable:
			return false
		case orderStable:
			return true
		}
	}

	panic("cannot determine order")
}

func (p *fuzzProducer) multipleYes() bool {
	for p := p; p != nil; p = p.upstream {
		switch p.multiple {
		case multipleNo:
			return false
		case multipleYes:
			return true
		}
	}

	panic("cannot determine multiple")
}

func (p *fuzzProducer) joinConcurrent() bool {
	for p := p; p != nil; p = p.upstream {
		switch p.join {
		case joinAny:
			return false
		case joinConcurrent:
			return true
		}
	}

	panic("cannot determine join")
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
		if !p.memoryIntensive {
			continue
		}

		num++
	}

	return num
}

func (e *unexpectedResultError) Error() string {
	return fmt.Sprintf("%+v: unexpected result: expected %+v", e.actual, e.expected)
}
