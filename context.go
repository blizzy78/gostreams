package gostreams

import "context"

// contextDone returns true if ctx.Done is closed.
func contextDone(ctx context.Context) bool {
	return ctx.Err() != nil
}
