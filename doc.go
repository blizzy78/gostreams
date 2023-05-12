// Package gostreams provides a set of operations on streams of elements. Streams form a pipeline
// of operations that elements are passing through.
//
// Streams consist of an initial ProducerFunc, which can produce elements from slices, channels,
// or any arbitrary source.
//
// Intermediate ProducerFuncs may apply mapping, filtering, and sorting operations to the elements.
// Some of these operations can work on elements concurrently to increase throughput.
//
// Finally, ConsumerFuncs consume the elements, and perform a final operation on them, such as
// collecting them into slices or maps, grouping/partitioning them, checking for matching elements,
// or simply iterating over them.
//
// Stream operations receive a context.CancelCauseFunc. Calling the cancel function cancels the entire
// stream, short-circuiting the processing of elements. Depending on the intermediate operations and
// the final consumer, the result of the consumer may be undefined. Producer implementations must be
// prepared to handle cancellation at any time by checking the provided context.Context.
//
// Streams are always lazy, meaning that producers will produce a new element only after a downstream
// producer or consumer has consumed the previous element.
package gostreams
