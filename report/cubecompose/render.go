package cubecompose

import (
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"strconv"
	"strings"
)

func (p *Plan) Render(frames []Frame) (string, []interface{}, error) {
	if len(frames) != p.FrameCount {
		return "", nil, fmt.Errorf("cube compose plan requires %d prepared cubes, got %d", p.FrameCount, len(frames))
	}
	for i, frame := range frames {
		if strings.TrimSpace(frame.SQL) == "" {
			return "", nil, fmt.Errorf("cube compose cube %d SQL was empty", i+1)
		}
	}
	r := &renderer{catalog: p.catalog, aliases: map[string]Column{}}
	for _, column := range p.Columns {
		r.aliases[normalize(column.Name)] = column
	}
	q := p.selectQuery
	projection, err := r.renderProjection(q.List)
	if err != nil {
		return "", nil, err
	}
	args := r.takeArgs()
	parts := []string{"SELECT " + projection}
	parts = append(parts, "FROM ("+frames[0].SQL+") AS t1")
	args = append(args, frames[0].Args...)
	for i, join := range q.Joins {
		frame := frames[i+1]
		joinKind, _ := normalizeJoin(join.Raw)
		on, err := r.renderNode(join.On.X)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, joinKind+" ("+frame.SQL+") AS "+cubeAlias(i+2)+" ON "+on)
		args = append(args, frame.Args...)
		args = append(args, r.takeArgs()...)
	}
	if q.Qualify != nil && q.Qualify.X != nil {
		value, err := r.renderNode(q.Qualify.X)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "WHERE "+value)
		args = append(args, r.takeArgs()...)
	}
	if len(q.GroupBy) > 0 {
		groupBy, err := r.renderList(q.GroupBy)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "GROUP BY "+groupBy)
		args = append(args, r.takeArgs()...)
	}
	if q.Having != nil && q.Having.X != nil {
		value, err := r.renderNode(q.Having.X)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "HAVING "+value)
		args = append(args, r.takeArgs()...)
	}
	if len(q.OrderBy) > 0 {
		orderBy, err := r.renderList(q.OrderBy)
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "ORDER BY "+orderBy)
		args = append(args, r.takeArgs()...)
	}
	parts = append(parts, "LIMIT "+strconv.Itoa(p.Limit))
	return strings.Join(parts, "\n"), args, nil
}

func (r *renderer) takeArgs() []interface{} {
	result := append([]interface{}{}, r.args...)
	r.args = nil
	return result
}

func (r *renderer) renderList(list query.List) (string, error) {
	items := make([]string, 0, len(list))
	for _, item := range list {
		value, err := r.renderNode(item.Expr)
		if err != nil {
			return "", err
		}
		if item.Alias != "" {
			value += " AS " + item.Alias
		}
		if item.Direction != "" {
			value += " " + strings.ToUpper(item.Direction)
		}
		items = append(items, value)
	}
	return strings.Join(items, ", "), nil
}

func (r *renderer) renderProjection(list query.List) (string, error) {
	items := make([]string, 0, len(list))
	for _, item := range list {
		value, err := r.renderNode(item.Expr)
		if err != nil {
			return "", err
		}
		alias := item.Alias
		if alias == "" {
			if selector, ok := item.Expr.(*expr.Selector); ok {
				parts := selectorParts(selector)
				field, found := r.catalog.Lookup(parts[1])
				if found && field.SQLName != field.Name {
					alias = field.Name
				}
			}
		}
		if alias != "" {
			value += " AS " + alias
		}
		items = append(items, value)
	}
	return strings.Join(items, ", "), nil
}

func (r *renderer) renderNode(n node.Node) (string, error) {
	switch actual := n.(type) {
	case []node.Node:
		items := make([]string, 0, len(actual))
		for _, item := range actual {
			value, err := r.renderNode(item)
			if err != nil {
				return "", err
			}
			items = append(items, value)
		}
		return strings.Join(items, ", "), nil
	case *expr.Selector:
		parts := selectorParts(actual)
		field, _ := r.catalog.Lookup(parts[1])
		return parts[0] + "." + field.SQLName, nil
	case *expr.Ident:
		if column, ok := r.aliases[normalize(actual.Name)]; ok {
			return column.Name, nil
		}
		return "", fmt.Errorf("unknown output alias %q", actual.Name)
	case *expr.Literal:
		if strings.EqualFold(actual.Kind, "null") {
			return "NULL", nil
		}
		value, err := literalValue(actual)
		if err != nil {
			return "", err
		}
		r.args = append(r.args, value)
		return "?", nil
	case *expr.Binary:
		left, err := r.renderNode(actual.X)
		if err != nil {
			return "", err
		}
		right, err := r.renderNode(actual.Y)
		if err != nil {
			return "", err
		}
		return left + " " + strings.ToUpper(strings.TrimSpace(actual.Op)) + " " + right, nil
	case *expr.Unary:
		value, err := r.renderNode(actual.X)
		operator := strings.ToUpper(strings.TrimSpace(actual.Op))
		if operator == "IS NULL" || operator == "IS NOT NULL" {
			return value + " " + operator, err
		}
		return operator + " " + value, err
	case *expr.Parenthesis:
		value, err := r.renderNode(actual.X)
		return "(" + value + ")", err
	case *expr.Qualify:
		return r.renderNode(actual.X)
	case *expr.Range:
		min, err := r.renderNode(actual.Min)
		if err != nil {
			return "", err
		}
		max, err := r.renderNode(actual.Max)
		return min + " AND " + max, err
	case *expr.Call:
		name := strings.ToUpper(sourceName(actual.X))
		args := make([]string, 0, len(actual.Args))
		for _, arg := range actual.Args {
			value, err := r.renderNode(arg)
			if err != nil {
				return "", err
			}
			args = append(args, value)
		}
		return name + "(" + strings.Join(args, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported rendered cube compose expression %T", n)
	}
}
