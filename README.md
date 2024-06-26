[![GoDoc](https://pkg.go.dev/badge/github.com/blizzy78/gostreams)](https://pkg.go.dev/github.com/blizzy78/gostreams)


gostreams
=========

A Go package that provides a set of operations on streams of elements.

```go
import "github.com/blizzy78/gostreams"
```


Code example
------------

```go
// construct a producer from a slice
ints := gostreams.Produce([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

// filter for even elements
// since we only need the elements themselves, we can use FuncPredicate
ints = gostreams.Filter(ints, gostreams.FuncPredicate(func(elem int) bool {
	return elem%2 == 0
}))

// map elements by doubling them
// since we only need the elements themselves, we can use FuncMapper
ints = gostreams.Map(ints, gostreams.FuncMapper(func(elem int) int {
	return elem * 2
}))

// map elements by converting them to strings
intStrs := gostreams.Map(ints, gostreams.FuncMapper(strconv.Itoa))

// perform a reduction to collect the strings into a slice
strs, _ := gostreams.ReduceSlice(context.Background(), intStrs)

fmt.Printf("%+v\n", strs)

// Output: [4 8 12 16 20]
```


License
-------

This package is licensed under the MIT license.
