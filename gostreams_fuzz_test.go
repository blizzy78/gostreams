package gostreams

import (
	"context"
	"testing"

	"github.com/matryer/is"
	"golang.org/x/exp/slices"
)

const (
	SliceProducerType = byte(iota + 1)
	ChannelProducerType
	ChannelConcurrentProducerType

	LimitProducerType
	SkipProducerType
	SortProducerType
	MapProducerType
)

const (
	OrderLikeParent = order(iota)
	OrderUnstable
	OrderStable
)

const maxChainLength = 20

type fuzzProducer struct {
	upstream *fuzzProducer
	order    order
	create   func() ProducerFunc[byte]
	expected []byte
}

type order int

func FuzzAll(f *testing.F) {
	f.Fuzz(func(t *testing.T, fuzzInput []byte) {
		fuzzProd, _, ok := readProducer(t, fuzzInput)
		if !ok {
			return
		}

		length := 0
		for fp := fuzzProd; fp != nil; fp = fp.upstream {
			length++
		}

		if length > maxChainLength {
			return
		}

		is := is.New(t)

		prod := fuzzProd.create()

		elems, err := ReduceSlice(context.Background(), prod)
		expected := fuzzProd.expected

		if !fuzzProd.stableOrder() {
			slices.Sort(elems)

			expected = make([]byte, len(fuzzProd.expected))
			copy(expected, fuzzProd.expected)
			slices.Sort(expected)
		}

		isEqualSlices(t, is, elems, expected)
		is.NoErr(err)
	})
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
		newFuzzProd, newFuzzInput, ok := readProducerWithUpstream(t, fuzzInput, fuzzProd)
		if !ok {
			return fuzzProd, fuzzInput, true
		}

		fuzzProd = newFuzzProd
		fuzzInput = newFuzzInput
	}
}

func readProducerWithUpstream(t *testing.T, fuzzInput []byte, upstream *fuzzProducer) (*fuzzProducer, []byte, bool) {
	t.Helper()

	if len(fuzzInput) == 0 {
		return nil, nil, false
	}

	typ := fuzzInput[0]
	fuzzInput = fuzzInput[1:]

	switch typ {
	case SliceProducerType:
		return readProducerSlice(t, fuzzInput, upstream)

	case ChannelProducerType:
		return readProducerChannel(t, fuzzInput, upstream)

	case ChannelConcurrentProducerType:
		return readProducerChannelConcurrent(t, fuzzInput, upstream)

	case LimitProducerType:
		return readProducerLimit(t, fuzzInput, upstream)

	case SkipProducerType:
		return readProducerSkip(t, fuzzInput, upstream)

	case SortProducerType:
		return readProducerSort(t, fuzzInput, upstream)

	case MapProducerType:
		return readProducerMap(t, fuzzInput, upstream)

	default:
		return nil, nil, false
	}
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
		order: OrderStable,

		create: func() ProducerFunc[byte] {
			return Produce(slices...)
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
		order: OrderStable,

		create: func() ProducerFunc[byte] {
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
		order: OrderUnstable,

		create: func() ProducerFunc[byte] {
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
		order:    OrderLikeParent,
		upstream: upstream,

		create: func() ProducerFunc[byte] {
			return Limit(upstream.create(), uint64(max))
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
		order:    OrderLikeParent,
		upstream: upstream,

		create: func() ProducerFunc[byte] {
			return Skip(upstream.create(), uint64(num))
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

	return &fuzzProducer{
		order:    OrderStable,
		upstream: upstream,

		create: func() ProducerFunc[byte] {
			return Sort(upstream.create(), func(_ context.Context, _ context.CancelCauseFunc, a byte, b byte) bool {
				return a < b
			})
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
		order:    OrderLikeParent,
		upstream: upstream,

		create: func() ProducerFunc[byte] {
			return Map[byte, byte](upstream.create(), func(_ context.Context, _ context.CancelCauseFunc, elem byte, _ uint64) byte {
				return elem / 2
			})
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

func isEqualSlices[T any](t *testing.T, is *is.I, first []T, second []T) {
	t.Helper()

	if first == nil {
		first = []T{}
	}

	if second == nil {
		second = []T{}
	}

	is.Equal(first, second)
}

func (p *fuzzProducer) stableOrder() bool {
	for prod := p; prod != nil; prod = prod.upstream {
		switch prod.order {
		case OrderUnstable:
			return false
		case OrderStable:
			return true
		}
	}

	panic("cannot determine order")
}
