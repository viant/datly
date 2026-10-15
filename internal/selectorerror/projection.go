// Package selectorerror retains native projection-stage origin within Datly.
// It does not confer transport ownership; the reader verifies its own root and input.
package selectorerror

import (
	"github.com/viant/datly/data"
	dsql "github.com/viant/datly/sql"
)

// Projection is created only at a native projection failure. Build strips a
// direct marker not created by that exact invocation before returning it.
type Projection struct {
	View  *data.View
	Cause *dsql.UnknownProjectionColumnError
}

func (e *Projection) Error() string {
	if e == nil || e.Cause == nil {
		return ""
	}
	return e.Cause.Error()
}
func (e *Projection) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
