package contextinfo

import (
	"context"
	"time"
)

// Details captures the cancellation and deadline state of a context at a
// specific point in time. It is intended for diagnostic logging only.
type Details struct {
	Err         string
	Cause       string
	HasDeadline bool
	Deadline    string
	Remaining   string
}

// Snapshot returns a log-friendly snapshot of ctx without retaining it.
func Snapshot(ctx context.Context) Details {
	if ctx == nil {
		return Details{}
	}

	details := Details{
		Err:   errorText(ctx.Err()),
		Cause: errorText(context.Cause(ctx)),
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return details
	}

	details.HasDeadline = true
	details.Deadline = deadline.UTC().Format(time.RFC3339Nano)
	details.Remaining = time.Until(deadline).String()
	return details
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
