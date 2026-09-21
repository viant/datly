package reader

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
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
	if m.session.ReadCaches[view] == nil || view.Cache == nil {
		return nil
	}
	warmups, err := view.Cache.EffectiveWarmups()
	if err != nil {
		return err
	}
	settings, name, values, err := m.selectWarmup(warmups)
	if err != nil || settings == nil {
		return err
	}
	prepared := &dexec.ReaderInput{Input: m.input.Interface(), Binder: m.binder, Parameters: m.session.Parameters}
	if len(values) > 0 {
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

// selectWarmup picks the most restrictive supplied index: warmups whose index
// parameter carries request values are preferred; among those the highest
// explicit Priority wins and equal priorities use the later declaration so
// broad-to-specific declarations select the most specific supplied dimension.
// Without any supplied index, a non-indexed warmup (same priority rule) still
// drives cache identity and projection, preserving the singular behavior.
func (m rootCacheMatcher) selectWarmup(warmups []*spec.CacheWarmupSettings) (*spec.CacheWarmupSettings, string, []any, error) {
	type indexedCandidate struct {
		settings *spec.CacheWarmupSettings
		name     string
		values   []any
	}
	var supplied, zero *indexedCandidate
	var fallback *spec.CacheWarmupSettings
	for _, candidate := range warmups {
		if candidate == nil {
			continue
		}
		if strings.TrimSpace(candidate.IndexColumn) == "" {
			if fallback == nil || candidate.Priority >= fallback.Priority {
				fallback = candidate
			}
			continue
		}
		if m.session.Parameters == nil {
			continue
		}
		name := candidate.IndexParameter
		if name == "" {
			name = candidate.IndexColumn
		}
		value, found, err := m.session.Parameters(name)
		if err != nil {
			return nil, "", nil, err
		}
		if !found || value == nil {
			continue
		}
		values := warmupIndexValues(value)
		if len(values) == 0 {
			continue
		}
		// A bound zero value is not a supplied selection; it only wins when no
		// index carries an actual value, preserving the singular contract.
		slot := &supplied
		if allZeroWarmupValues(values) {
			slot = &zero
		}
		if *slot == nil || candidate.Priority >= (*slot).settings.Priority {
			*slot = &indexedCandidate{settings: candidate, name: name, values: values}
		}
	}
	if supplied == nil {
		supplied = zero
	}
	if supplied != nil {
		return supplied.settings, supplied.name, supplied.values, nil
	}
	return fallback, "", nil, nil
}

func allZeroWarmupValues(values []any) bool {
	for _, value := range values {
		if value == nil {
			continue
		}
		actual := reflect.ValueOf(value)
		if actual.IsValid() && !actual.IsZero() {
			return false
		}
	}
	return true
}

func warmupIndexValues(value any) []any {
	actual := reflect.ValueOf(value)
	for actual.Kind() == reflect.Pointer || actual.Kind() == reflect.Interface {
		if actual.IsNil() {
			return nil
		}
		actual = actual.Elem()
	}
	if actual.Kind() == reflect.Slice && actual.Type().Elem().Kind() != reflect.Uint8 {
		var values []any
		for i := 0; i < actual.Len(); i++ {
			values = append(values, actual.Index(i).Interface())
		}
		return values
	}
	return []any{actual.Interface()}
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
