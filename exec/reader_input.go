package exec

import (
	"context"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

const ReaderInputPreparerKey xhandler.ValueKey = "reader_input_preparer"

// ReaderInput carries invocation values only. Bindly projections stay owned by
// the canonical registry contract and its DI adapter.
type ReaderInput struct {
	Input      any
	Binder     xhandler.Binder
	Parameters sqlx.ParameterResolver
}

type ReaderInputPreparer func(context.Context, ...string) (*ReaderInput, error)
