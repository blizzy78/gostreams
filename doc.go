// Package gostreams provides a set of operations on streams of elements.
// Streams form a pipeline of operations that elements are being passed through.
//
// Streams are constructed by creating an initial ProducerFunc, which can produce elements from slices,
// channels, or any arbitrary source.
//
// Elements may then be operated upon using mapping, filtering, and sorting operations
// (which are intermediate ProducerFuncs). Some of these operations can work on elements concurrently
// to increase throughput.
//
// Finally, the elements are consumed by ConsumerFuncs, such as collecting them into slices or maps,
// grouping/partitioning them, checking for matching elements, or simply iterating over them.
//
// Stream operations will receive a context.CancelCauseFunc. Calling the cancel function will
// cancel the entire stream, thus short-circuiting processing elements. Depending on the intermediate
// operations and the final consumer, the result of the consumer may be undefined.
// Producer implementations must be prepared to be canceled at any time by checking the provided context.Context.
//
// Streams are always lazy, meaning that producers will produce a new element only after a
// downstream producer or consumer has consumed the previous element.
package gostreams
