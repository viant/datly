package builder

import (
	"fmt"

	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
)

// ShapeBound applies view selector policy to SQL whose authored placeholders
// are already bound. It preserves the existing argument order and is used by
// ordinary output relations that expand parent SQL before selector shaping.
func (b *Builder) ShapeBound(query *cache.ParmetrizedQuery, opts ...BuilderOption) (*cache.ParmetrizedQuery, error) {
	if query == nil {
		return nil, fmt.Errorf("bound query is required")
	}
	options := newBuilderOptions(opts...)
	options.sqlText = query.SQL
	if err := b.prepareCriteria(options); err != nil {
		return nil, err
	}
	controls, err := b.resolveControls(options, options.excludePagination)
	if err != nil {
		return nil, err
	}
	if err := b.validateProjection(options); err != nil {
		return nil, err
	}
	sourceSQL := options.sqlText
	positionalArgs := interfaceSlice(query.Args)
	resolver := options.parameterResolver
	if len(options.projection) > 0 {
		sourceSQL, resolver, err = nameProjectionBindings(sourceSQL, positionalArgs, resolver)
		if err != nil {
			return nil, err
		}
		positionalArgs = nil
	}
	projection, err := (dsql.SelectorProjection{SQL: sourceSQL, View: options.view}).Prepare(options.projection)
	if err != nil {
		return nil, err
	}
	controls, err = projection.OutputControls(controls)
	if err != nil {
		return nil, err
	}
	sqlText := projection.Source
	hadCriteriaToken := containsSelectorCriteriaToken(sqlText)
	boundSQL, args, err := bindSelectorCriteriaSQL(sqlText, resolver, options.selector, positionalArgs)
	if err != nil {
		return nil, err
	}
	boundSQL, args = options.appendSelectorCriteria(boundSQL, args, hadCriteriaToken)
	boundSQL, args, err = applyPartition(boundSQL, args, options.source, options.partition)
	if err != nil {
		return nil, err
	}
	boundSQL = dsql.PrepareExecutableSQL(projection.Render(boundSQL), controls)
	result := &cache.ParmetrizedQuery{
		By:        query.By,
		ByColumns: append([]string(nil), query.ByColumns...),
		SQL:       boundSQL,
		Ordered:   query.Ordered,
		Args:      interfaceSlice(args),
		In:        append([]interface{}(nil), query.In...),
		Offset:    query.Offset,
		Limit:     query.Limit,
		OnSkip:    query.OnSkip,
	}
	if len(query.InTuples) > 0 {
		result.InTuples = cloneInterfaceRows(query.InTuples)
	}
	applyMatcherWindow(result, controls)
	return result, nil
}
