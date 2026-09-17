package reader

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	dexec "github.com/viant/datly/exec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/builder"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

type rootCacheMatcher struct {
	session   *Session
	input     reflect.Value
	binder    xhandler.Binder
	selectors invocationSelectors
	query     *cache.ParmetrizedQuery
}

func (m rootCacheMatcher) apply(ctx context.Context) error {
	root := m.session.Artifact.Root
	view := root.View
	if m.session.ReadCaches[view] == nil || view.Cache == nil || view.Cache.Warmup == nil {
		return nil
	}
	settings := view.Cache.Warmup
	prepared := &dexec.ReaderInput{Input: m.input.Interface(), Binder: m.binder, Parameters: m.session.Parameters}
	var values []any
	if settings.IndexColumn != "" {
		name := settings.IndexParameter
		if name == "" {
			name = settings.IndexColumn
		}
		if m.session.Parameters == nil {
			return nil
		}
		value, found, err := m.session.Parameters(name)
		if err != nil {
			return err
		}
		if !found || value == nil {
			return nil
		}
		actual := reflect.ValueOf(value)
		for actual.Kind() == reflect.Pointer || actual.Kind() == reflect.Interface {
			if actual.IsNil() {
				return nil
			}
			actual = actual.Elem()
		}
		if actual.Kind() == reflect.Slice && actual.Type().Elem().Kind() != reflect.Uint8 {
			for i := 0; i < actual.Len(); i++ {
				values = append(values, actual.Index(i).Interface())
			}
		} else {
			values = []any{actual.Interface()}
		}
		if len(values) == 0 {
			return nil
		}
		if m.binder == nil {
			return fmt.Errorf("indexed root cache requires canonical input preparation")
		}
		capability, found, err := m.binder.Lookup(ctx, dexec.ReaderInputPreparerKey)
		if err != nil {
			return err
		}
		preparer, ok := capability.(dexec.ReaderInputPreparer)
		if !found || !ok {
			return fmt.Errorf("indexed root cache requires canonical input preparation")
		}
		prepared, err = preparer(ctx, name)
		if err != nil {
			return err
		}
	}
	input := reflect.ValueOf(prepared.Input)
	if input.Kind() != reflect.Pointer || input.IsNil() || input.Elem().Type() != m.session.InputType {
		return fmt.Errorf("cache identity input must be *%s", m.session.InputType)
	}
	selector := m.selectors.forView(view).Clone()
	projection := viewProjection(view, selector)
	// Warmup's authored projection determines cache identity. Invocation fields
	// only describe the requested native replay; they never rename that entry.
	if selector == nil {
		selector = &xstate.Selector{}
	}
	selector.Fields = append([]string(nil), settings.FieldNames...)
	selector.Columns = nil
	connection, err := viewConnection(ctx, m.session, root)
	if err != nil {
		return err
	}
	identity, err := builder.NewBuilder().CacheSQL(ctx,
		builder.WithBuilderComponent(m.session.Component), builder.WithBuilderView(view), builder.WithBuilderCriteriaCompiler(root.Criteria),
		builder.WithBuilderSelector(selector), builder.WithBuilderProjection(viewProjection(view, selector)),
		builder.WithBuilderInput(input.Elem()), builder.WithBuilderParameterResolver(prepared.Parameters),
		builder.WithBuilderTemplate(root.Template), builder.WithBuilderBinder(prepared.Binder), builder.WithBuilderDialect(connection.Dialect))
	if err != nil {
		return err
	}
	stored, requested := root.cacheProjection(settings.FieldNames), root.cacheProjection(projection)
	if view.IsGroupable() {
		stored, err = (dsql.CacheProjection{SQL: identity.SQL, View: view}).Fields()
		if err != nil {
			return nil
		}
		requested, err = (dsql.CacheProjection{SQL: m.query.SQL, View: view}).Fields()
		if err != nil {
			return nil
		}
	}
	if _, compatible, _, err := (cache.Projection{Stored: stored}).Indexes(requested); err != nil {
		return err
	} else if !compatible {
		return nil
	}
	// Native indexed replay applies its window per index value. A root SQL
	// window is global, so multiple index values must use the ordinary query.
	if len(values) > 1 && (identity.Limit > 0 || identity.Offset > 0) {
		return nil
	}
	m.query.By = strings.TrimSpace(settings.IndexColumn)
	m.query.In = values
	m.query.IdentitySQL = identity.SQL
	m.query.IdentityArgs = identity.Args
	m.query.Limit = identity.Limit
	m.query.Offset = identity.Offset
	for _, field := range requested {
		// Native entry metadata retains the actual scan columns/types. Grouped
		// compatibility was checked above by the same native Projection owner.
		field.DimensionKey, field.MeasureKey = "", ""
		m.query.RequestedFields = append(m.query.RequestedFields, field)
	}
	return nil
}

// cacheProjection classifies this view's compiled column identities for native
// cache compatibility. SQL selection membership remains the builder's concern.
func (p *ViewPlan) cacheProjection(names []string) []cache.ProjectionField {
	var result []cache.ProjectionField
	appendColumn := func(name string) {
		for _, column := range p.View.Columns {
			if column == nil || !(strings.EqualFold(name, column.Name) || strings.EqualFold(name, column.Column)) {
				continue
			}
			field := cache.ProjectionField{Name: column.Name, ColumnName: column.Column, FieldName: column.Name}

			result = append(result, field)
			return
		}
		result = append(result, cache.ProjectionField{Name: name})
	}
	if len(names) == 0 {
		for _, column := range p.View.Columns {
			if column != nil {
				appendColumn(column.Name)
			}
		}
	} else {
		for _, name := range names {
			appendColumn(name)
		}
	}
	return result
}
