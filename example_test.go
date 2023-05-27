package gostreams

import (
	"context"
	"fmt"
	"strconv"
)

func Example() {
	// construct a producer from a slice
	ints := Produce([]int{1, 2, 3, 4, 5})

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
	// Output: [2 4 6 8 10]
}
