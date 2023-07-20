package gostreams

import "context"

// DuplicateKeyError is the error used to short-circuit a stream by canceling its context to indicate that
// a key couldn't be added to a map because it already exists.
type DuplicateKeyError[T any, K comparable] struct {
	// Element is the upstream producer's element that caused the error.
	Element T

	// Key is the key that was already in the map.
	Key K
}

// CollectSlice returns an accumulator that collects elements into a slice.
func CollectSlice[T any]() AccumulatorFunc[T, []T] {
	var result []T

	return func(_ context.Context, _ context.CancelCauseFunc, elem T, _ uint64) []T {
		result = append(result, elem)
		return result
	}
}

// CollectMap returns an accumulator that collects elements into a map, using key to map elements to keys,
// and value to map elements to values. If a key is already in the map, it overwrites the map entry.
func CollectMap[T any, K comparable, V any](key MapperFunc[T, K], value MapperFunc[T, V]) AccumulatorFunc[T, map[K]V] {
	result := map[K]V{}

	return func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) map[K]V {
		result[key(ctx, cancel, elem, index)] = value(ctx, cancel, elem, index)
		return result
	}
}

// CollectMapNoDuplicateKeys returns an accumulator that collects elements into a map, using key to map
// elements to keys, and value to map elements to values. If a key is already in the map, it cancels the stream's context
// with a DuplicateKeyError.
func CollectMapNoDuplicateKeys[T any, K comparable, V any](key MapperFunc[T, K], value MapperFunc[T, V]) AccumulatorFunc[T, map[K]V] {
	result := map[K]V{}

	return func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) map[K]V {
		key := key(ctx, cancel, elem, index)

		if _, ok := result[key]; ok {
			cancel(&DuplicateKeyError[T, K]{
				Element: elem,
				Key:     key,
			})

			return result
		}

		result[key] = value(ctx, cancel, elem, index)

		return result
	}
}

// CollectGroup returns an accumulator that collects elements into a group map, according to key.
// It uses value to map elements to values.
func CollectGroup[T any, K comparable, V any](key MapperFunc[T, K], value MapperFunc[T, V]) AccumulatorFunc[T, map[K][]V] {
	result := map[K][]V{}

	return func(ctx context.Context, cancel context.CancelCauseFunc, elem T, index uint64) map[K][]V {
		key := key(ctx, cancel, elem, index)
		result[key] = append(result[key], value(ctx, cancel, elem, index))

		return result
	}
}

// CollectPartition returns an accumulator that collects elements into a partition map, according to pred.
// It uses value to map elements to values.
func CollectPartition[T any, V any](pred PredicateFunc[T], value MapperFunc[T, V]) AccumulatorFunc[T, map[bool][]V] {
	return CollectGroup(MapperFunc[T, bool](pred), value)
}

// Error implements error.
func (e *DuplicateKeyError[T, K]) Error() string {
	return "duplicate key"
}
