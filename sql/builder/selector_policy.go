package builder

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx/io/read/cache"
	xresponse "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

func applyReportOrdering(ctx context.Context, options *builderOptions) {
	if options.component == nil || options.view == nil ||
		!exec.AllowsReportOrdering(ctx, options.component.Key, options.view.Spec.CanonicalName()) {
		return
	}
	options.reportOrderFields = exec.ReportOrderingFields(ctx, options.component.Key, options.view.Spec.CanonicalName())
	policy := options.selectorPolicy
	if policy == nil && options.component.RootView != nil {
		policy = options.component.RootView.Selector
	}
	if policy != nil {
		// The compiled policy is shared by readers and reports. Only this local
		// copy may open the ordering gate; column restrictions remain intact.
		local := *policy
		local.AllowOrderBy = true
		options.selectorPolicy = &local
	}
}

func invalidOrdering(format string, args ...any) error {
	err := fmt.Errorf(format, args...)
	return &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: err.Error()}, Cause: err}
}

func (b *Builder) resolveControls(options *builderOptions, excludePagination bool) (*spec.ViewControls, error) {
	if options == nil {
		return nil, nil
	}
	policy := options.selectorPolicy
	if policy == nil && options.component != nil && options.component.RootView != nil {
		policy = options.component.RootView.Selector
	}
	source := *options
	if source.selector != nil && strings.TrimSpace(source.selector.OrderBy) != "" {
		if err := b.resolveSourceSQL(&source); err != nil {
			return nil, err
		}
		prepared, err := source.prepareProjectionSource()
		if err != nil {
			return nil, err
		}
		source.sqlText = prepared.sql
	}
	resolver := selectorResolver{sourceSQL: source.sqlText, policy: policy, sqlText: paginationInspectionSource(source.sqlText), view: source.view, projection: source.projection, reportOrderFields: options.reportOrderFields}
	orderingSQL, err := resolver.orderingSource(options.controls, options.selector)
	if err != nil {
		return nil, err
	}
	if orderingSQL != resolver.sqlText {
		// These invocation clauses are applied after source resolution. Their
		// namespace cannot be proven from the SQL wrapper alone.
		if options.partition != nil || options.forUpdate ||
			(!options.skipRelationFilter && (options.relation != nil || len(options.compositeColumns) > 0)) {
			return nil, invalidOrdering("source ordering cannot remove a declaration wrapper with deferred partition, relation or lock clauses")
		}
		// The local source changes only after a transparent wrapper proof.
		// Shared view/selector metadata and caller state remain untouched.
		options.sqlText = orderingSQL
		resolver.sqlText = orderingSQL
		resolver.sourceSQL = orderingSQL
		resolver.sourceOrdering = true
	}

	if excludePagination && len(options.projection) == 0 && options.selector != nil {
		resolver.ordinalReference = options.selector.Columns
	}
	return resolver.controls(options.controls, options.selector, excludePagination)
}

func (b *Builder) validateProjection(options *builderOptions) error {
	if options == nil || len(options.projection) == 0 {
		return nil
	}
	policy := options.selectorPolicy
	if policy == nil && options.component != nil && options.component.RootView != nil {
		policy = options.component.RootView.Selector
	}
	if policy != nil && !policy.AllowFields {
		return fmt.Errorf("selector projection is not allowed")
	}
	return nil
}

func applyMatcherWindow(query *cache.ParmetrizedQuery, controls *spec.ViewControls) {
	if query == nil || controls == nil {
		return
	}
	if controls.Offset != nil {
		query.Offset = *controls.Offset
	}
	if controls.Limit != nil {
		query.Limit = *controls.Limit
	}
}

type selectorResolver struct {
	sourceSQL         string
	sourceOrdering    bool
	reportOrderFields []string
	ordinalReference  []string
	projection        []string
	view              *data.View
	policy            *spec.Selector
	sqlText           string
}

func (r selectorResolver) controls(base *spec.ViewControls, input *xstate.Selector, excludePagination bool) (*spec.ViewControls, error) {
	if !excludePagination && input != nil && (input.Page < 0 || input.Limit < 0 || input.Offset < 0) {
		return nil, &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: "selector pagination values must be non-negative"}, Cause: fmt.Errorf("selector pagination values must be non-negative")}
	}
	result := base.Clone()
	if result == nil {
		result = &spec.ViewControls{}
	}
	if r.policy != nil {
		if result.OrderBy == "" {
			result.OrderBy = r.policy.DefaultOrder
		}
		if !excludePagination && result.Limit == nil && r.policy.DefaultLimit > 0 && !r.policy.NoLimit {
			limit := r.policy.DefaultLimit
			result.Limit = &limit
		}
	}
	if r.sourceOrdering && result.OrderBy != "" && (input == nil || strings.TrimSpace(input.OrderBy) == "") {
		resolved, err := r.orderBy(result.OrderBy)
		if err != nil {
			return nil, err
		}
		result.OrderBy = resolved
	}
	if input != nil {
		if strings.TrimSpace(input.OrderBy) != "" {
			if r.policy != nil && !r.policy.AllowOrderBy {
				return nil, invalidOrdering("selector order by is not allowed")
			}
			orderBy, err := r.orderBy(input.OrderBy)
			if err != nil {
				return nil, err
			}
			result.OrderBy = orderBy
		}
		if strings.TrimSpace(input.Criteria) != "" && r.policy != nil && !r.policy.AllowCriteria {
			return nil, fmt.Errorf("selector criteria is not allowed")
		}
		if !excludePagination {
			if input.Limit > 0 {
				if r.policy != nil && !r.policy.AllowLimit {
					return nil, fmt.Errorf("selector limit is not allowed")
				}
				limit := input.Limit
				if r.policy != nil && r.policy.DefaultLimit > 0 && !r.policy.NoLimit && limit > r.policy.DefaultLimit {
					limit = r.policy.DefaultLimit
				}
				result.Limit = &limit
			}
			if input.Offset > 0 {
				if r.policy != nil && !r.policy.AllowOffset {
					return nil, fmt.Errorf("selector offset is not allowed")
				}
				offset := input.Offset
				result.Offset = &offset
			} else if input.Page > 0 {
				if r.policy != nil && !r.policy.AllowPage {
					return nil, fmt.Errorf("selector page is not allowed")
				}
				if result.Limit == nil || *result.Limit <= 0 {
					if r.policy == nil || !r.policy.NoLimit {
						return nil, fmt.Errorf("selector page requires a positive limit")
					}
					// An explicitly unlimited view has no page window. Preserve
					// its complete result rather than inventing a limit or offset.
					result.Offset = nil
				} else {
					if input.Page-1 > int(^uint(0)>>1) / *result.Limit {
						return nil, &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: "selector page and limit overflow offset"}, Cause: fmt.Errorf("selector page and limit overflow offset")}
					}
					offset := *result.Limit * (input.Page - 1)
					result.Offset = &offset
				}
			}
		}
	}
	if excludePagination {
		result.Limit = nil
		result.Offset = nil
	} else if result.Offset != nil && *result.Offset > 0 && (result.Limit == nil || *result.Limit <= 0) {
		return nil, fmt.Errorf("selector offset requires a positive limit")
	}
	if result.IsZero() {
		return nil, nil
	}
	return result, nil
}

func (r selectorResolver) orderBy(source string) (string, error) {
	items, err := r.parseOrder(source)
	if err != nil {
		return "", invalidOrdering("%s", err)
	}

	sqlText := r.sqlText
	if r.view != nil && r.view.IsGroupable() && len(r.projection) > 0 {
		sqlText = dsql.GroupedProjectionCriteriaSource(sqlText, r.projection)
	}
	projection := dsql.SelectorProjection{SQL: sqlText, View: r.view}
	projected, err := projection.Columns(nil)
	if err != nil {
		return "", err
	}
	ordered, err := projection.Columns(r.projection)
	if err != nil {
		return "", err
	}
	ordinalColumns := ordered
	ordinalPrepared := len(r.ordinalReference) == 0
	result := make([]string, 0, len(items))
	for _, item := range items {
		name, positional, direction := item.name, item.positional, item.direction

		if positional {
			if !ordinalPrepared {
				ordinalColumns, err = projection.Columns(r.ordinalReference)
				if err != nil {
					return "", invalidOrdering("%s", err)
				}
				ordinalPrepared = true
			}
			position, _ := strconv.Atoi(name)
			if position < 1 || position > len(ordinalColumns) {
				return "", invalidOrdering("order by position %s is outside source projection", name)
			}
			if !r.orderPermitted(ordinalColumns[position-1]) {
				return "", invalidOrdering("order by position %s is not allowed", name)
			}
			if len(r.ordinalReference) > 0 {
				mapped := 0
				for i, column := range projected {
					if column.OutputName() == ordinalColumns[position-1].OutputName() {
						if mapped != 0 {
							return "", invalidOrdering("ambiguous order by position %s", name)
						}
						mapped = i + 1
					}
				}
				if mapped == 0 {
					return "", invalidOrdering("order by position %s is outside source projection", name)
				}
				name = strconv.Itoa(mapped)
			}
		} else {
			mapped, aliased, err := r.orderAlias(name)
			if err != nil {
				return "", invalidOrdering("%s", err)
			}
			var matched *dsql.ProjectionColumn
			for i := range projected {
				if projected[i].Matches(mapped) && (!aliased || projected[i].MatchesOutput(mapped)) {
					if matched != nil {
						return "", invalidOrdering("ambiguous order by field %q", name)
					}
					matched = &projected[i]
				}
			}
			if matched == nil {
				target, authorized, err := r.qualifiedOrderTarget(name)
				if err != nil {
					return "", invalidOrdering("%s", err)
				}
				if !authorized {
					return "", invalidOrdering("order by field %q is not in source projection", name)
				}
				if _, err = (dsql.SelectorProjection{SQL: r.sqlText}).SourceOrderingSQL(target); err != nil {
					return "", invalidOrdering("%s", err)
				}
				if direction != "" {
					target += " " + direction
				}
				result = append(result, target)
				continue
			}
			if !r.orderPermitted(*matched) {
				return "", invalidOrdering("order by field %q is not allowed", name)
			}
			name = matched.OrderExpression()
			if len(r.projection) > 0 && !sqltext.HasTopLevelClause(r.sqlText, "union") {
				retained := false
				for _, column := range ordered {
					if column.Matches(mapped) {
						name = column.OrderExpression()
						retained = true
						break
					}
				}
				if !retained {
					if r.view != nil && r.view.IsGroupable() {
						return "", invalidOrdering("order by field %q is not selected in grouped projection", name)
					}
					name = matched.SourceExpression()
				}
			}
		}
		if direction != "" {
			name += " " + direction
		}
		result = append(result, name)
	}
	return strings.Join(result, ", "), nil
}

func (r selectorResolver) orderPermitted(column dsql.ProjectionColumn) bool {
	if r.reportOrderFields != nil {
		selected := false
		for _, name := range r.reportOrderFields {
			if column.Matches(name) {
				selected = true
				break
			}
		}
		if !selected {
			return false
		}
	}
	if r.policy == nil || len(r.policy.Orderable) == 0 && len(r.policy.OrderAliases) == 0 {
		return true
	}
	for _, name := range r.policy.Orderable {
		if column.Matches(string(name)) {
			return true
		}
	}
	for _, name := range r.policy.OrderAliases {
		if column.MatchesOutput(string(name)) {
			return true
		}
	}
	return false
}

func (r selectorResolver) orderAlias(name string) (string, bool, error) {
	if r.policy == nil {
		return name, false, nil
	}
	mapped := ""
	for alias, target := range r.policy.OrderAliases {
		if !(dsql.ProjectionNames{alias}).Matches(name) {
			continue
		}
		if mapped != "" && mapped != string(target) {
			return "", true, fmt.Errorf("ambiguous order alias %q", name)
		}
		mapped = string(target)
	}
	if mapped == "" {
		return name, false, nil
	}
	return mapped, true, nil
}

type selectorOrderItem struct {
	name       string
	direction  string
	positional bool
}

func (r selectorResolver) parseOrder(source string) ([]selectorOrderItem, error) {
	rawItems := sqltext.SplitTopLevelCSV(normalizeOrderSyntax(source))
	var result []selectorOrderItem
	for _, raw := range rawItems {
		name := strings.TrimSpace(raw)
		direction := ""
		for _, candidate := range []string{"ASC", "DESC"} {
			at := sqltext.FindLastTopLevelKeyword(name, candidate, 0)
			if at >= 0 && strings.EqualFold(strings.TrimSpace(name[at:]), candidate) {
				direction = candidate
				name = strings.TrimSpace(name[:at])
				break
			}
		}
		if _, err := sqlparser.TableIdentifierParts(name); err == nil {
			result = append(result, selectorOrderItem{name: name, direction: direction})
			continue
		}
		parsed, err := sqlparser.ParseQuery("SELECT 1 FROM selector_order_source ORDER BY " + raw)
		if err != nil || parsed == nil || len(parsed.OrderBy) != 1 || parsed.Limit != nil || parsed.Offset != nil || parsed.Union != nil {
			return nil, fmt.Errorf("invalid selector order by %q", raw)
		}
		literal, ok := parsed.OrderBy[0].Expr.(*expr.Literal)
		if !ok || literal.Kind != "int" {
			return nil, fmt.Errorf("selector order by only supports output names or numeric positions")
		}
		if _, err := strconv.Atoi(literal.Value); err != nil {
			return nil, fmt.Errorf("invalid order by position %q", literal.Value)
		}
		result = append(result, selectorOrderItem{name: literal.Value, direction: direction, positional: true})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty order by")
	}
	return result, nil
}

func normalizeOrderSyntax(source string) string {
	items := sqltext.SplitTopLevelCSV(source)
	for i, item := range items {
		item = strings.TrimSpace(item)
		if strings.Count(item, ":") == 1 && !strings.ContainsAny(item, " \t") {
			name, direction, _ := strings.Cut(item, ":")
			item = strings.TrimSpace(name) + " " + strings.TrimSpace(direction)
		}
		items[i] = item
	}
	return strings.Join(items, ", ")
}

// qualifiedOrderTarget keeps source-only access opt-in through existing exact
// authored qualified targets. Terminal shorthand is local to that allowlist.
func (r selectorResolver) qualifiedOrderTarget(name string) (string, bool, error) {
	if r.policy == nil || r.reportOrderFields != nil {
		return "", false, nil
	}
	mapped, aliased, err := r.orderAlias(name)
	if err != nil {
		return "", false, err
	}
	target := ""
	accept := func(candidate string, aliasMatch bool) error {
		parts, e := sqlparser.TableIdentifierParts(candidate)
		if e != nil || len(parts) != 2 {
			return nil
		}
		matches := (dsql.ProjectionNames{candidate}).Matches(mapped)
		if !aliased {
			matches = matches || (dsql.ProjectionNames{parts[1]}).Matches(name)
		}
		if aliasMatch {
			matches = true
		}
		if !matches {
			return nil
		}
		if target != "" && !(dsql.ProjectionNames{target}).Matches(candidate) {
			return fmt.Errorf("ambiguous source order field %q", name)
		}
		target = candidate
		return nil
	}
	for _, column := range r.policy.Orderable {
		if err = accept(string(column), false); err != nil {
			return "", false, err
		}
	}
	for alias, column := range r.policy.OrderAliases {
		if err = accept(string(column), (dsql.ProjectionNames{alias}).Matches(name)); err != nil {
			return "", false, err
		}
	}
	return target, target != "", nil
}

// orderingSource unwraps at most one proven declaration wrapper when sorting
// on an explicitly authorized field absent from the public SELECT projection.
func (r *selectorResolver) orderingSource(base *spec.ViewControls, input *xstate.Selector) (string, error) {
	order := ""
	if base != nil {
		order = base.OrderBy
	}
	if order == "" && r.policy != nil {
		order = r.policy.DefaultOrder
	}
	if input != nil && strings.TrimSpace(input.OrderBy) != "" {
		order = input.OrderBy
	}
	if order == "" || r.policy == nil {
		return r.sqlText, nil
	}
	qualified := false
	for _, v := range r.policy.Orderable {
		if parts, e := sqlparser.TableIdentifierParts(string(v)); e == nil && len(parts) == 2 {
			qualified = true
		}
	}
	for _, v := range r.policy.OrderAliases {
		if parts, e := sqlparser.TableIdentifierParts(string(v)); e == nil && len(parts) == 2 {
			qualified = true
		}
	}
	if !qualified {
		return r.sqlText, nil
	}
	items, err := r.parseOrder(order)
	if err != nil {
		return "", invalidOrdering("%s", err)
	}
	columns, err := (dsql.SelectorProjection{SQL: r.sqlText, View: r.view}).Columns(nil)
	if err != nil {
		return r.sqlText, nil
	} // Ordinary resolution owns opaque metadata errors.
	result := r.sqlText
	for _, item := range items {
		if item.positional {
			continue
		}
		mapped, aliased, e := r.orderAlias(item.name)
		if e != nil {
			return "", invalidOrdering("%s", e)
		}
		projected := false
		for _, col := range columns {
			if col.Matches(mapped) && (!aliased || col.MatchesOutput(mapped)) {
				projected = true
			}
		}
		if projected {
			continue
		}
		target, authorized, e := r.qualifiedOrderTarget(item.name)
		if e != nil {
			return "", invalidOrdering("%s", e)
		}
		if !authorized {
			return "", invalidOrdering("order by field %q is not in source projection", item.name)
		}
		sourceSQL := r.sqlText
		if r.sourceSQL != "" {
			sourceSQL = r.sourceSQL
		}
		owner, e := (dsql.SelectorProjection{SQL: sourceSQL}).SourceOrderingSQL(target)
		if e != nil {
			return "", invalidOrdering("%s", e)
		}
		if owner != sourceSQL && input != nil && strings.TrimSpace(input.Criteria) != "" {
			return "", invalidOrdering("source ordering cannot remove a declaration wrapper with selector criteria")
		}
		if result != r.sqlText && owner != result {
			return "", invalidOrdering("source ordering has conflicting owners")
		}
		r.sourceOrdering = true
		result = owner
	}
	return result, nil
}
