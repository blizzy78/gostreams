package gostreams

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/matryer/is"
)

func TestReduce(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ints := Produce([]int{1, 2, 3, 4, 5})

	summer := func(_ context.Context, _ context.CancelCauseFunc, elem int, index uint64, acc int) int {
		is.Equal(index, uint64(elem-1))

		return acc + elem
	}

	result, _ := Reduce(ctx, ints, 0, summer)

	is.Equal(result, 15)
}

func TestReduce_Cancel(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ints := Produce([]int{1, 2, 3, 4, 5})

	summer := func(_ context.Context, cancel context.CancelCauseFunc, elem int, index uint64, acc int) int {
		is.True(elem <= 3)
		is.Equal(index, uint64(elem-1))

		if elem == 3 {
			cancel(nil)
			return acc
		}

		return acc + elem
	}

	result, err := Reduce(ctx, ints, 0, summer)

	is.Equal(result, 3)
	is.True(errors.Is(err, context.Canceled))
}

func TestReduce_CollectMapNoDuplicateKeys(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ints := Produce([]int{1, 2, 3, 3, 4, 5})

	result, err := Reduce(ctx, ints, map[string]int{}, CollectMapNoDuplicateKeys(itoa, Identity[int]()))

	is.Equal(result, map[string]int{
		"1": 1,
		"2": 2,
		"3": 3,
	})

	var cause *DuplicateKeyError[int, string]

	is.True(errors.As(err, &cause))
	is.Equal(cause.Element, 3)
	is.Equal(cause.Key, "3")
}

func TestEach(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ints := Produce([]int{1, 2, 3, 4, 5})

	sum := 0

	summer := func(_ context.Context, _ context.CancelCauseFunc, elem int, index uint64) {
		is.Equal(index, uint64(elem-1))

		sum += elem
	}

	_ = Each(ctx, ints, summer)

	is.Equal(sum, 15)
}

func TestEach_Cancel(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ints := Produce([]int{1, 2, 3, 4, 5})

	sum := 0

	summer := func(_ context.Context, cancel context.CancelCauseFunc, elem int, index uint64) {
		is.True(elem <= 3)
		is.Equal(index, uint64(elem-1))

		if elem == 3 {
			cancel(nil)
			return
		}

		sum += elem
	}

	err := Each(ctx, ints, summer)

	is.Equal(sum, 3)
	is.True(errors.Is(err, context.Canceled))
}

func TestEachConcurrent(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	ints := Produce([]int{1, 2, 3, 4, 5})

	sum := atomic.Int32{}

	summer := func(_ context.Context, _ context.CancelCauseFunc, elem int, index uint64) {
		is.Equal(index, uint64(elem-1))

		sum.Add(int32(elem))
	}

	_ = EachConcurrent(ctx, ints, summer)

	is.Equal(int(sum.Load()), 15)
}

func TestAnyMatch(t *testing.T) {
	tests := []struct {
		given                []int
		want                 bool
		wantProducerCanceled bool
	}{
		{
			given:                []int{1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			want:                 false,
			wantProducerCanceled: false,
		},
		{
			given:                []int{1, 2, 100, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			want:                 true,
			wantProducerCanceled: true,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			is := is.New(t)

			ctx := context.Background()

			producerCanceled := make(chan bool)

			ints := func(ctx context.Context, _ context.CancelCauseFunc) <-chan int {
				outCh := make(chan int)

				go func() {
					canceled := false

					defer func() {
						producerCanceled <- canceled
					}()

					defer close(outCh)

					for _, i := range test.given {
						select {
						case outCh <- i:

						case <-ctx.Done():
							canceled = true
							return
						}
					}
				}()

				return outCh
			}

			expectedIndex := uint64(0)

			greaterThan10 := func(_ context.Context, _ context.CancelCauseFunc, elem int, index uint64) bool {
				is.Equal(index, expectedIndex)
				expectedIndex++

				return elem > 10
			}

			result, _ := AnyMatch(ctx, ints, greaterThan10)

			is.Equal(result, test.want)
			is.Equal(<-producerCanceled, test.wantProducerCanceled)
		})
	}
}

func TestAllMatch(t *testing.T) {
	tests := []struct {
		given                []int
		want                 bool
		wantProducerCanceled bool
	}{
		{
			given:                []int{1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			want:                 true,
			wantProducerCanceled: false,
		},
		{
			given:                []int{1, 2, 100, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5},
			want:                 false,
			wantProducerCanceled: true,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			is := is.New(t)

			ctx := context.Background()

			producerCanceled := make(chan bool)

			ints := func(ctx context.Context, _ context.CancelCauseFunc) <-chan int {
				outCh := make(chan int)

				go func() {
					canceled := false

					defer func() {
						producerCanceled <- canceled
					}()

					defer close(outCh)

					for _, i := range test.given {
						select {
						case outCh <- i:

						case <-ctx.Done():
							canceled = true
							return
						}
					}
				}()

				return outCh
			}

			expectedIndex := uint64(0)

			lessThan10 := func(_ context.Context, _ context.CancelCauseFunc, elem int, index uint64) bool {
				is.Equal(index, expectedIndex)
				expectedIndex++

				return elem < 10
			}

			result, _ := AllMatch(ctx, ints, lessThan10)

			is.Equal(result, test.want)
			is.Equal(<-producerCanceled, test.wantProducerCanceled)
		})
	}
}

func TestCount(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	strs := Produce([]string{"foo", "bar", "baz"})

	result, _ := Count(ctx, strs)

	is.Equal(result, uint64(3))
}
