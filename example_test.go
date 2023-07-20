package gostreams

import (
	"context"
	"fmt"
	"strconv"
)

func Example() {
	// construct a producer from a slice
	ints := Produce([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

	// filter for even elements
	// since we only need the elements themselves, we can use FuncPredicate
	ints = Filter(ints, FuncPredicate(func(elem int) bool {
		return elem%2 == 0
	}))

	// map elements by doubling them
	// since we only need the elements themselves, we can use FuncMapper
	ints = Map(ints, FuncMapper(func(elem int) int {
		return elem * 2
	}))

	// map elements by converting them to strings
	intStrs := Map(ints, FuncMapper(strconv.Itoa))

	// perform a reduction to collect the strings into a slice
	strs, _ := ReduceSlice(context.Background(), intStrs)

	fmt.Printf("%+v\n", strs)
	// Output: [4 8 12 16 20]
}
