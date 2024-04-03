package gostreams

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

const (
	ReduceSliceConsumerType = iota + 1
	CollectMapConsumerType
	CollectMapNoDuplicateKeysConsumerType
	CollectGroupConsumerType
	CollectPartitionConsumerType
	AnyMatchConsumerType
	AllMatchConsumerType
	CountConsumerType
	FirstConsumerType
)

type fuzzConsumer struct {
	describe func() string
	test     func(ctx context.Context) error
}

type unexpectedResultError[T any] struct {
	actual   T
	expected T
}

var consumerTypeToFunc = map[int]func(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, error){
	ReduceSliceConsumerType:               readConsumerReduceSlice,
	CollectMapConsumerType:                readConsumerCollectMap,
	CollectMapNoDuplicateKeysConsumerType: readConsumerCollectMapNoDuplicateKeys,
	CollectGroupConsumerType:              readConsumerCollectGroup,
	CollectPartitionConsumerType:          readConsumerCollectPartition,
	AnyMatchConsumerType:                  readConsumerAnyMatch,
	AllMatchConsumerType:                  readConsumerAllMatch,
	CountConsumerType:                     readConsumerCount,
	FirstConsumerType:                     readConsumerFirst,
}

func readConsumer(t *testing.T, fuzzProd *fuzzProducer, fuzzInput []byte) (*fuzzConsumer, error) {
	t.Helper()

	typ, fuzzInput, err := readInt(t, fuzzInput)
	if err != nil {
		return nil, err
	}

	f, ok := consumerTypeToFunc[typ]
	if !ok {
		return nil, errFuzzInput
	}

	return f(t, fuzzProd, fuzzInput)
}

func readConsumerReduceSlice(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "reduceSlice"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			expected := fuzzProd.expected()

			if !fuzzProd.stableOrder() {
				tmp := make([]byte, len(expected))
				copy(tmp, expected)
				expected = tmp

				slices.Sort(expected)
			}

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
	}, nil
}

func readConsumerCollectMap(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:gocognit // map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectMap"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			expected := map[byte]byte{}
			for _, b := range fuzzProd.expected() {
				expected[b*2] = b * 3
			}

			result, err := Reduce(
				ctx, prod,
				CollectMap(
					FuncMapper(func(elem byte) byte {
						return elem * 2
					}),
					FuncMapper(func(elem byte) byte {
						return elem * 3
					}),
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
	}, nil
}

func readConsumerCollectMapNoDuplicateKeys(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:gocognit,cyclop // must match signature; map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectMapNoDuplicateKeys"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			expected := map[byte]byte{}

			for _, b := range fuzzProd.expected() {
				if _, ok := expected[b*2]; ok {
					return errFuzzInput
				}

				expected[b*2] = b * 3
			}

			result, err := Reduce(
				ctx, prod,
				CollectMapNoDuplicateKeys(
					FuncMapper(func(elem byte) byte {
						return elem * 2
					}),
					FuncMapper(func(elem byte) byte {
						return elem * 3
					}),
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
	}, nil
}

func readConsumerCollectGroup(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:gocognit // map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectGroup"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			expected := map[byte][]byte{}
			for _, b := range fuzzProd.expected() {
				expected[b%10] = append(expected[b%10], b)
			}

			result, err := Reduce(
				ctx, prod,
				CollectGroup(
					FuncMapper(func(elem byte) byte {
						return elem % 10
					}),
					FuncMapper(func(elem byte) byte {
						return elem
					}),
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
	}, nil
}

func readConsumerCollectPartition(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:gocognit // map collector is a bit more involved
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "collectPartition"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			expected := map[bool][]byte{}
			for _, b := range fuzzProd.expected() {
				expected[b%2 == 0] = append(expected[b%2 == 0], b)
			}

			result, err := Reduce(
				ctx, prod,
				CollectPartition(
					FuncPredicate(func(elem byte) bool {
						return elem%2 == 0
					}),
					FuncMapper(func(elem byte) byte {
						return elem
					}),
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
	}, nil
}

func readConsumerAnyMatch(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "anyMatch"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			match, err := AnyMatch(ctx, prod, FuncPredicate(func(elem byte) bool {
				return elem >= 100
			}))

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			expected := false
			for _, b := range fuzzProd.expected() {
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
	}, nil
}

func readConsumerAllMatch(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "allMatch"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			allMatch, err := AllMatch(ctx, prod, FuncPredicate(func(elem byte) bool {
				return elem >= 100
			}))

			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			expected := true
			for _, b := range fuzzProd.expected() {
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
	}, nil
}

func readConsumerCount(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) { //nolint:unparam // must match signature
	t.Helper()

	return &fuzzConsumer{
		describe: func() string {
			return "allMatch"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			count, err := Count(ctx, prod)
			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			expected := fuzzProd.expected()
			if int(count) != len(expected) {
				return &unexpectedResultError[int]{
					actual:   int(count),
					expected: len(expected),
				}
			}

			return nil
		},
	}, nil
}

func readConsumerFirst(t *testing.T, fuzzProd *fuzzProducer, _ []byte) (*fuzzConsumer, error) {
	t.Helper()

	if !fuzzProd.stableOrder() {
		return nil, errFuzzInput
	}

	return &fuzzConsumer{
		describe: func() string {
			return "first"
		},

		test: func(ctx context.Context) error {
			prod, err := fuzzProd.create(ctx)
			if err != nil {
				return err
			}

			result, ok, err := First(ctx, prod)
			if err != nil {
				if errors.Is(err, fuzzProd.acceptedError()) {
					return nil
				}

				return err
			}

			expected := fuzzProd.expected()
			expectedOk := len(expected) != 0

			if ok != expectedOk {
				return &unexpectedResultError[bool]{
					actual:   ok,
					expected: expectedOk,
				}
			}

			if ok && result != expected[0] {
				return &unexpectedResultError[byte]{
					actual:   result,
					expected: expected[0],
				}
			}

			return nil
		},
	}, nil
}

func (e *unexpectedResultError[T]) Error() string {
	return fmt.Sprintf("%+v: unexpected result: expected %+v", e.actual, e.expected)
}
