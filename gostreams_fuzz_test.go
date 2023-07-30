package gostreams

// nice -n 19 go test '-run=^$' -fuzz=FuzzAll -cover

import (
	"context"
	"errors"
	"testing"
)

// reduce these to lower memory usage
const (
	maxPipelineLength   = 20
	maxSlices           = 5
	maxSliceLength      = 10
	maxMemoryIntensives = 2
)

var errFuzzInput = errors.New("invalid fuzz testing input")

func FuzzAll(f *testing.F) {
	f.Fuzz(func(t *testing.T, fuzzInput []byte) {
		origFuzzInput := fuzzInput

		fuzzCons, fuzzProd, err := readConsumerAndProducer(t, fuzzInput)
		if err != nil {
			if errors.Is(err, errFuzzInput) {
				t.SkipNow()
				return
			}

			t.Fatalf("%+v: %s", origFuzzInput, err.Error())
			return
		}

		if err := fuzzCons.test(context.Background()); err != nil {
			if errors.Is(err, errFuzzInput) {
				t.SkipNow()
				return
			}

			t.Fatalf("%+v: %s -> %s: %s", origFuzzInput, fuzzProd.describe(), fuzzCons.describe(), err.Error())
		}
	})
}

func readConsumerAndProducer(t *testing.T, fuzzInput []byte) (*fuzzConsumer, *fuzzProducer, error) {
	t.Helper()

	var (
		fuzzProd *fuzzProducer
		err      error
	)

	fuzzProd, fuzzInput, err = readProducer(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	fuzzCons, err := readConsumer(t, fuzzProd, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	return fuzzCons, fuzzProd, nil
}

func readSlices(t *testing.T, fuzzInput []byte) ([][]byte, []byte, error) {
	t.Helper()

	num, fuzzInput, err := readInt(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	if num > maxSlices {
		return nil, nil, errFuzzInput
	}

	slices := make([][]byte, num)

	for idx := 0; idx < num; idx++ {
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

	length, fuzzInput, err := readInt(t, fuzzInput)
	if err != nil {
		return nil, nil, err
	}

	if len(fuzzInput) < length {
		return nil, nil, errFuzzInput
	}

	return fuzzInput[:length], fuzzInput[length:], nil
}

func peekInt(t *testing.T, fuzzInput []byte) (int, error) {
	t.Helper()

	b, _, err := readByte(t, fuzzInput)
	if err != nil {
		return 0, err
	}

	return int(b), nil
}

func readInt(t *testing.T, fuzzInput []byte) (int, []byte, error) {
	t.Helper()

	byt, fuzzInput, err := readByte(t, fuzzInput)
	if err != nil {
		return 0, nil, err
	}

	return int(byt), fuzzInput, nil
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

	if first == nil && second == nil {
		return true
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
