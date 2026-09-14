package reader

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/sql/reader/readmeta"
	"github.com/viant/sqlx"
	sqlxio "github.com/viant/sqlx/io"
	xhandler "github.com/viant/xdatly/handler"
)

// ReadResult carries ordinary typed data plus optional evidence about actual
// mapped columns at final result ordinals. It does not change row shape.
type ReadResult struct {
	Data       any
	Projection *readmeta.Result
}

// ReadResult opts one invocation into column provenance. Ordinary Read remains
// allocation-free with respect to this optional evidence graph.
func (e *Execution) ReadResult(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*ReadResult, error) {
	if e == nil || e.service == nil {
		return nil, fmt.Errorf("reader execution is not initialized")
	}
	session := e.session()
	session.Parameters = resolver
	session.applyReadOptions(ctx)
	session.CollectProjection = true
	data, err := e.service.Read(ctx, session, input, binder)
	if err != nil {
		return nil, err
	}
	return &ReadResult{Data: data, Projection: session.Projection}, nil
}

type columnEvidence struct {
	rowType reflect.Type
	fields  *readmeta.Fields
}

func (e *columnEvidence) observe(columns []sqlxio.Column) error {
	if e.rowType == nil {
		return fmt.Errorf("read evidence requires a prepared row schema")
	}
	matched, err := sqlxio.NewMatcher(nil).Match(e.rowType, columns)
	if err != nil && !sqlxio.IsMatchedError(err) {
		return err
	}
	var indexes [][]int
	for _, field := range matched {
		if index := field.FieldIndex(); len(index) > 0 {
			indexes = append(indexes, index)
		}
	}
	e.fields = readmeta.NewFields(e.rowType, indexes)
	return nil
}
