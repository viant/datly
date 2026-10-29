package cubecompose

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

// validateUnion admits one independently prepared frame per branch. Projection
// is deliberately limited to typed source fields and scalar literals: UNION ALL
// combines rows, and cannot recompute aggregates over grouped results.
func (p *Plan) validateUnion(maxLimit int) error {
	p.nullable = make([]bool, p.FrameCount)
	p.Fields = make([][]string, p.FrameCount)
	var last *query.Select
	for q := p.selectQuery; q != nil; {
		index := len(p.unionBranches)
		if index >= p.FrameCount {
			return fmt.Errorf("cube compose UNION ALL has too many branches")
		}
		if q.Kind != "" || len(q.WithSelects) != 0 || q.WithRecursive || len(q.Joins) != 0 || q.Qualify != nil || q.QualifyClause != nil || len(q.GroupBy) != 0 || q.Having != nil || q.ForUpdate {
			return fmt.Errorf("cube compose UNION ALL branch contains an unsupported clause")
		}
		if sourceFrame(q.From.X, p.FrameCount) != index+1 || !strings.EqualFold(q.From.Alias, cubeAlias(index+1)) || q.From.Comments != "" || q.From.Unparsed != "" || q.From.Raw.Raw != "" {
			return fmt.Errorf("cube compose UNION ALL branch %d must read only %s AS %s", index+1, cubeMacro(index+1), cubeAlias(index+1))
		}
		if len(q.List) == 0 {
			return fmt.Errorf("cube compose UNION ALL requires explicit columns")
		}
		if index > 0 && len(q.List) != len(p.Columns) {
			return fmt.Errorf("cube compose UNION ALL column counts differ")
		}
		names := map[string]bool{}
		fields := map[string]bool{}
		for column, item := range q.List {
			if item == nil || !safeIdentifier.MatchString(item.Alias) || names[normalize(item.Alias)] || item.Direction != "" || item.Comments != "" || item.Raw != "" {
				return fmt.Errorf("cube compose UNION ALL requires distinct explicit column aliases")
			}
			names[normalize(item.Alias)] = true
			var typ reflect.Type
			role := Computed
			switch value := item.Expr.(type) {
			case *expr.Selector:
				ref, err := p.resolveSelector(value)
				if err != nil {
					return err
				}
				if !strings.EqualFold(ref.alias, cubeAlias(index+1)) {
					return fmt.Errorf("cube compose UNION ALL branch references another frame")
				}
				typ, role = ref.field.Type, ref.field.Role
				if typ == nil {
					return fmt.Errorf("cube compose UNION ALL field requires a concrete type")
				}
				fields[ref.field.Name] = true
			case *expr.Literal:
				literal, err := literalValue(value)
				if err != nil {
					return err
				}
				typ = reflect.TypeOf(literal)
			default:
				return fmt.Errorf("cube compose UNION ALL permits only typed fields and scalar literals")
			}
			if index == 0 {
				p.Columns = append(p.Columns, Column{Name: item.Alias, Type: typeName(typ), Role: role, rType: typ})
				continue
			}
			prior := &p.Columns[column]
			if prior.Name != item.Alias {
				return fmt.Errorf("cube compose UNION ALL column aliases differ")
			}
			merged, err := unionType(prior.rType, typ)
			if err != nil {
				return fmt.Errorf("cube compose UNION ALL column %s: %w", item.Alias, err)
			}
			prior.rType, prior.Type = merged, typeName(merged)
			if prior.Role != role {
				prior.Role = Computed
			}
		}
		if len(fields) == 0 {
			return fmt.Errorf("cube compose UNION ALL branch requires a source field")
		}
		p.Fields[index] = sortedFields(fields)
		p.unionBranches = append(p.unionBranches, q)
		last = q
		if q.Union == nil {
			break
		}
		if !strings.EqualFold(strings.TrimSpace(q.Union.Kind), "all") || q.Union.X == nil {
			return fmt.Errorf("cube compose supports only UNION ALL")
		}
		if q.Limit != nil || q.Offset != nil || len(q.OrderBy) != 0 || q.Window != nil {
			return fmt.Errorf("cube compose UNION ALL paging belongs after the final branch")
		}
		q = q.Union.X
	}
	if len(p.unionBranches) != p.FrameCount {
		return fmt.Errorf("cube compose UNION ALL requires one branch per frame")
	}
	for _, column := range p.Columns {
		if column.rType == nil {
			return fmt.Errorf("cube compose UNION ALL column %s has only NULL values", column.Name)
		}
	}
	if last.Window != nil && (!strings.EqualFold(strings.TrimSpace(last.Window.Raw), "limit") || last.Window.X != nil || last.Window.Unparsed != "") {
		return fmt.Errorf("cube compose UNION ALL does not allow WINDOW clauses")
	}
	for _, item := range last.OrderBy {
		ident, ok := item.Expr.(*expr.Ident)
		if !ok || item.Alias != "" || item.Comments != "" || item.Direction != "" && !strings.EqualFold(item.Direction, "ASC") && !strings.EqualFold(item.Direction, "DESC") {
			return fmt.Errorf("cube compose UNION ALL ordering requires output aliases")
		}
		found := false
		for _, column := range p.Columns {
			if normalize(column.Name) == normalize(ident.Name) {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("cube compose UNION ALL order alias %q is unknown", ident.Name)
		}
	}
	p.unionOrder = last.OrderBy
	p.Limit = maxLimit
	if last.Limit != nil {
		v, e := strconv.Atoi(last.Limit.Value)
		if e != nil || v <= 0 || v > maxLimit {
			return fmt.Errorf("cube compose LIMIT must be between 1 and %d", maxLimit)
		}
		p.Limit = v
	}
	if last.Offset != nil {
		v, e := strconv.Atoi(last.Offset.Value)
		if e != nil || v < 0 || v > maxLimit || p.Limit > maxLimit-v {
			return fmt.Errorf("cube compose UNION ALL LIMIT plus OFFSET must not exceed %d", maxLimit)
		}
		if v > 0 && len(p.unionOrder) == 0 {
			return fmt.Errorf("cube compose UNION ALL OFFSET requires output ordering")
		}
		p.offset = v
	}
	return nil
}

func unionType(a, b reflect.Type) (reflect.Type, error) {
	if a == nil && b == nil {
		return nil, nil
	}
	if a == nil {
		if b.Kind() != reflect.Pointer {
			b = reflect.PointerTo(b)
		}
		return b, nil
	}
	if b == nil {
		if a.Kind() != reflect.Pointer {
			a = reflect.PointerTo(a)
		}
		return a, nil
	}
	left, right := a, b
	for left.Kind() == reflect.Pointer {
		left = left.Elem()
	}
	for right.Kind() == reflect.Pointer {
		right = right.Elem()
	}
	if left != right {
		return nil, fmt.Errorf("incompatible types %v and %v", a, b)
	}
	if a.Kind() == reflect.Pointer || b.Kind() == reflect.Pointer {
		return reflect.PointerTo(left), nil
	}
	return left, nil
}

func (p *Plan) renderUnion(frames []Frame) (string, []interface{}, error) {
	r := &renderer{catalog: p.catalog, aliases: map[string]Column{}}
	for _, column := range p.Columns {
		r.aliases[normalize(column.Name)] = column
	}
	var branches []string
	var args []interface{}
	for i, q := range p.unionBranches {
		projection, err := r.renderProjection(q.List)
		if err != nil {
			return "", nil, err
		}
		args = append(args, r.takeArgs()...)
		args = append(args, frames[i].Args...)
		branches = append(branches, "SELECT "+projection+" FROM ("+frames[i].SQL+") AS "+cubeAlias(i+1))
	}
	sql := strings.Join(branches, "\nUNION ALL\n")
	if len(p.unionOrder) > 0 {
		order, err := r.renderList(p.unionOrder)
		if err != nil {
			return "", nil, err
		}
		sql += "\nORDER BY " + order
	}
	sql += "\nLIMIT " + strconv.Itoa(p.Limit)
	if p.offset > 0 {
		sql += " OFFSET " + strconv.Itoa(p.offset)
	}
	return sql, args, nil
}
