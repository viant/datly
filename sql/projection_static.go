package sql

import (
	"fmt"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/sqlanalysis"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/metadata/info"
)

// StaticProjection owns complete, checked SQL output membership. Its retained
// analysis AST never becomes executable SQL and contains no request values.
// Construction is intentionally restricted to transparent outer-star readers.
type StaticProjection struct {
	source    string
	owner     *data.View
	analysis  *query.Select
	columns   []ProjectionColumn
	rendering data.View
	dialect   *info.Dialect
}

// StaticProjectionSelection is one invocation's immutable checked selection.
// A zero value carries no authority and is rejected by Prepare.
type StaticProjectionSelection struct {
	plan   *StaticProjection
	fields []string
	chosen []ProjectionColumn
}

func NewStaticProjection(source string, view *data.View, dialect *info.Dialect) (*StaticProjection, error) {
	if view == nil || view.IsGroupable() {
		return nil, fmt.Errorf("static projection requires a non-grouped reader view")
	}
	analysisSQL, parsed, err := sqlanalysis.Checked(source, nil)
	if err != nil {
		return nil, err
	}
	if parsed == nil || len(parsed.List) != 1 || projectionStar(parsed.List[0]) == nil || len(projectionStar(parsed.List[0]).Except) > 0 ||
		len(parsed.GroupBy) > 0 || parsed.Having != nil || parsed.Union != nil || len(parsed.WithSelects) > 0 || len(parsed.Joins) > 0 ||
		parsed.Window != nil || parsed.QualifyClause != nil || strings.EqualFold(parsed.Kind, "DISTINCT") {
		return nil, fmt.Errorf("static projection requires a transparent outer star")
	}
	// Physical wildcards and opaque sources cannot borrow authority from Go scan
	// metadata. This boundary requires a complete explicit derived SELECT.
	raw, ok := parsed.From.X.(*expr.Raw)
	if !ok || raw.X == nil {
		return nil, fmt.Errorf("static projection requires a checked derived source")
	}
	nested, ok := raw.X.(*query.Select)
	if !ok || len(nested.List) == 0 || len(nested.GroupBy) > 0 || nested.Having != nil || nested.Union != nil || nested.Window != nil || len(nested.WithSelects) > 0 || groupedProjectionNeedsRewrite(nested.List) {
		return nil, fmt.Errorf("static projection requires complete non-grouped derived outputs")
	}
	for _, item := range nested.List {
		if item == nil || item.Expr == nil || projectionStar(item) != nil {
			return nil, fmt.Errorf("static projection cannot infer physical wildcard outputs")
		}
	}
	parts, ok := newSelectProjectionSource(analysisSQL)
	if !ok {
		return nil, fmt.Errorf("static projection has no SELECT source")
	}
	membership := SelectorProjection{SQL: analysisSQL, View: view, Dialect: dialect}
	columns, star, err := membership.columnsFromParsed(parsed, parts, true, true)
	if err != nil {
		return nil, err
	}
	if !star || len(columns) == 0 {
		return nil, fmt.Errorf("static projection has no complete outputs")
	}
	rendering := data.View{}
	allowNulls := view.NullsAllowed()
	rendering.Spec.AllowNulls = &allowNulls
	for i := range columns {
		if columns[i].metadata != nil {
			copyColumn := *columns[i].metadata
			columns[i].metadata = &copyColumn
		}
		rendering.Columns = append(rendering.Columns, columns[i].outputColumn(view))
	}
	return &StaticProjection{source: source, owner: view, analysis: parsed, columns: columns, rendering: rendering, dialect: dialect}, nil
}

// Select validates the final requested Fields once through the normal SQL
// output/mapping owner. It neither reads Page nor evaluates SQL templates.
func (p *StaticProjection) Select(fields []string) (*StaticProjectionSelection, error) {
	if p == nil || p.analysis == nil || p.owner == nil {
		return nil, fmt.Errorf("static projection is not checked")
	}
	membership := SelectorProjection{}
	chosen, err := membership.selectColumns(p.columns, fields, true)
	if err != nil {
		if unknown, ok := err.(*UnknownProjectionColumnError); ok {
			for _, field := range fields {
				if canonicalProjectionName(field) == canonicalProjectionName(unknown.Column) {
					unknown.RequestedColumn = field
					break
				}
			}
		}
		return nil, err
	}
	return &StaticProjectionSelection{plan: p, fields: append([]string(nil), fields...), chosen: append([]ProjectionColumn(nil), chosen...)}, nil
}

// Prepare checks invocation ownership and attaches the normal evaluated source
// to this exact selection. It performs no SQL parse, membership lookup or
// template evaluation. Returned mutable projection state is invocation-local.
func (s *StaticProjectionSelection) Prepare(source string, view *data.View, evaluated string, dialects ...*info.Dialect) (*Projection, error) {
	if s == nil || s.plan == nil || s.plan.owner != view || s.plan.source != source {
		return nil, fmt.Errorf("static projection selection source/view mismatch")
	}
	dialect := s.plan.dialect
	if len(dialects) > 0 {
		dialect = dialects[0]
	}
	var names []string
	if len(normalizeProjectionSelection(s.fields)) > 0 {
		for _, column := range s.chosen {
			names = append(names, column.output)
		}
	}
	result, handled, err := renderStarProjection(evaluated, names, &s.plan.rendering, dialect, s.plan.columns)
	if err != nil {
		return nil, err
	}
	if !handled {
		result = &Projection{Source: evaluated, columns: append([]ProjectionColumn(nil), s.plan.columns...)}
	}
	return result, nil
}

// Validate binds a checked selection to the exact final Fields request before
// any template side effect. Requested spelling/order belong to that invocation.
func (s *StaticProjectionSelection) Validate(source string, view *data.View, fields []string) error {
	if s == nil || s.plan == nil || s.plan.owner != view || s.plan.source != source || len(s.fields) != len(fields) {
		return fmt.Errorf("static projection selection source/view/fields mismatch")
	}
	for i, field := range fields {
		if field != s.fields[i] {
			return fmt.Errorf("static projection selection fields mismatch")
		}
	}
	return nil
}

// Columns returns detached membership/selection slices for existing order policy.
// It does not repeat Fields validation or inspect executable SQL.
func (s *StaticProjectionSelection) Columns() (all, chosen []ProjectionColumn) {
	if s == nil || s.plan == nil {
		return nil, nil
	}
	return append([]ProjectionColumn(nil), s.plan.columns...), append([]ProjectionColumn(nil), s.chosen...)
}
