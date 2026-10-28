package reader

import (
	"context"
	"fmt"
	"reflect"

	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	xh "github.com/viant/xdatly/handler"
)

// ReadRows keeps the consumer on this execution session, not in context, so
// binding, authorization and nested readers cannot inherit it.
func (e *Execution) ReadRows(ctx context.Context, input any, binder xh.Binder, resolver sqlx.ParameterResolver, consumer *xh.RowConsumer) (any, error) {
	if e == nil || e.service == nil {
		return nil, fmt.Errorf("reader execution is not initialized")
	}
	session := e.session()
	session.Parameters = resolver
	session.applyReadOptions(ctx)
	session.consumer = consumer
	if err := session.validateConsumer(); err != nil {
		return nil, err
	}
	return e.service.Read(ctx, session, input, binder)
}

func (s *Session) validateConsumer() error {
	if !s.consumer.Valid() {
		return fmt.Errorf("row consumer callback is required")
	}
	root := s.Artifact.Root
	if root == nil || root.Collector == nil || root.View == nil || root.Collector.Schema.RowType() == nil {
		return fmt.Errorf("row consumption requires a typed root")
	}
	if root.Partitioner != nil || len(root.Relations) > 0 || root.View.Spec.SelfReference != nil || root.View.Cache != nil || len(s.ReadCaches) > 0 || s.CacheOnly || s.RefreshCache || s.QueryScope != nil {
		return fmt.Errorf("row consumption requires a flat, unpartitioned, uncached reader")
	}
	if s.Artifact.DirectOutput && s.OutputType.Kind() != reflect.Slice {
		return fmt.Errorf("row consumption requires a collection result")
	}
	if !s.Artifact.DirectOutput {
		outputType := s.OutputType
		if outputType.Kind() == reflect.Ptr {
			outputType = outputType.Elem()
		}
		field, ok := outputType.FieldByName(s.Artifact.OutputViewField)
		if !ok || field.Type.Kind() != reflect.Slice {
			return fmt.Errorf("row consumption requires a collection result")
		}
	}
	rowType := root.Collector.Schema.RowType()
	if rowType.Kind() != reflect.Ptr {
		rowType = reflect.PointerTo(rowType)
	}
	if s.consumer.RowType() != rowType {
		return fmt.Errorf("row consumer type %v does not match generated row %v", s.consumer.RowType(), rowType)
	}
	if rowType.Implements(reflect.TypeFor[xh.OnRelationer]()) {
		return fmt.Errorf("row consumption does not support relation completion hooks")
	}
	return nil
}

func (s *Service) consumeRoot(ctx context.Context, session *Session, connection dsql.Connection, query *cache.ParmetrizedQuery, outputType reflect.Type, direct bool) (any, error) {
	// Streaming callbacks run with the cursor open. Transactional reader hooks
	// normally drain first to permit nested reads; that path remains unchanged.
	if connection.Tx != nil {
		return nil, fmt.Errorf("row consumption does not support an active transaction")
	}
	root := session.Artifact.Root
	typ := root.Collector.Schema.RowType()
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	newRow := func() any { return reflect.New(typ).Interface() }
	options := newReaderOptions(session, root.View, nil)
	options.retry = sqlxread.RetryPolicy{}
	scan := options.rows(newRow, nil, query)
	visitor := newRowHookVisitor(ctx, root.View, nil, func(row any) error { return session.consumer.Consume(ctx, row) })
	visitor.decoder = scan.decoder
	count := 0
	var visitorError error
	err := scan.query(ctx, rowQuery{db: connection.DB, query: query, read: session.rootRead, visit: func(row any) error {
		if err := visitor.Visit(row); err != nil {
			visitorError = err
			return &rowConsumerFailure{cause: err}
		}
		count++
		return nil
	}})
	// SQLX interprets io.EOF as its internal early-stop signal. A consumer,
	// codec or hook EOF is still a failure of this invocation, not success.
	if visitorError != nil {
		return nil, visitorError
	}
	if err != nil {
		return nil, err
	}
	session.rootRead.done(count, nil)
	var output any
	if direct {
		output = reflect.MakeSlice(outputType, 0, 0).Interface()
	} else {
		output = reflect.New(outputType).Interface()
	}
	session.Data = output
	return output, nil
}

// Do not unwrap while SQLX owns the callback: its EOF sentinel means success.
// consumeRoot restores the original error after cursor cleanup and SQL metrics.
type rowConsumerFailure struct{ cause error }

func (e *rowConsumerFailure) Error() string { return e.cause.Error() }
