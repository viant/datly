package builder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

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
	resolver := selectorResolver{policy: policy, sqlText: source.sqlText, view: source.view, projection: source.projection}
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
	projection []string
	view       *data.View
	policy     *spec.Selector
	sqlText    string
}

func (r selectorResolver) controls(base *spec.ViewControls, input *xstate.Selector, excludePagination bool) (*spec.ViewControls, error) {
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
	if input != nil {
		if strings.TrimSpace(input.OrderBy) != "" {
			if r.policy != nil && !r.policy.AllowOrderBy {
				return nil, fmt.Errorf("selector order by is not allowed")
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
					return nil, fmt.Errorf("selector page requires a positive limit")
				}
				offset := *result.Limit * (input.Page - 1)
				result.Offset = &offset
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
		return "", err
	}

	projection := dsql.SelectorProjection{SQL: r.sqlText, View: r.view}
	projected, err := projection.Columns(nil)
	if err != nil {
		return "", err
	}
	ordered, err := projection.Columns(r.projection)
	if err != nil {
		return "", err
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		name, positional, direction := item.name, item.positional, item.direction

		if positional {
			position, _ := strconv.Atoi(name)
			if position < 1 || position > len(ordered) {
				return "", fmt.Errorf("order by position %s is outside source projection", name)
			}
			if !r.orderPermitted(ordered[position-1]) {
				return "", fmt.Errorf("order by position %s is not allowed", name)
			}
		} else {
			mapped, aliased, err := r.orderAlias(name)
			if err != nil {
				return "", err
			}
			var matched *dsql.ProjectionColumn
			for i := range projected {
				if projected[i].Matches(mapped) && (!aliased || projected[i].MatchesOutput(mapped)) {
					if matched != nil {
						return "", fmt.Errorf("ambiguous order by field %q", name)
					}
					matched = &projected[i]
				}
			}
			if matched == nil {
				return "", fmt.Errorf("order by field %q is not in source projection", name)
			}
			if !r.orderPermitted(*matched) {
				return "", fmt.Errorf("order by field %q is not allowed", name)
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
