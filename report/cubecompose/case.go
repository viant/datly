package cubecompose

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
)

// caseParts uses only the parser's traversable branches. Switch.Raw is never SQL authority.
func caseParts(s *expr.Switch) (node.Node, []*expr.Case, node.Node, error) {
	if s == nil || len(s.Cases) == 0 {
		return nil, nil, nil, fmt.Errorf("CASE requires structured branches")
	}
	var selector, otherwise node.Node
	branches := s.Cases
	if branches[0] != nil && branches[0].X.X != nil && branches[0].Y == nil {
		selector = branches[0].X.X
		branches = branches[1:]
	}
	if len(branches) > 0 && branches[len(branches)-1] != nil && branches[len(branches)-1].X.X == nil {
		otherwise = branches[len(branches)-1].Y
		branches = branches[:len(branches)-1]
		if otherwise == nil {
			return nil, nil, nil, fmt.Errorf("CASE ELSE is empty")
		}
	}
	if len(branches) == 0 {
		return nil, nil, nil, fmt.Errorf("CASE requires WHEN branches")
	}
	for _, b := range branches {
		if b == nil || b.X.X == nil || b.Y == nil {
			return nil, nil, nil, fmt.Errorf("CASE branch is malformed")
		}
	}
	return selector, branches, otherwise, nil
}

func (p *Plan) validateCase(s *expr.Switch, aliases map[string]Column) ([]fieldRef, error) {
	selector, branches, otherwise, err := caseParts(s)
	if err != nil {
		return nil, err
	}
	var refs []fieldRef
	nodes := []node.Node{selector, otherwise}
	for _, b := range branches {
		nodes = append(nodes, b.X.X, b.Y)
	}
	for _, n := range nodes {
		r, e := p.validateNode(n, aliases)
		if e != nil {
			return nil, e
		}
		refs = append(refs, r...)
	}
	if _, err = p.caseType(s, aliases); err != nil {
		return nil, err
	}
	return refs, nil
}

func (p *Plan) caseType(s *expr.Switch, aliases map[string]Column) (reflect.Type, error) {
	selector, branches, otherwise, err := caseParts(s)
	if err != nil {
		return nil, err
	}
	var selectorType reflect.Type
	if selector != nil {
		selectorType, err = p.scalarType(selector, aliases)
		if err != nil {
			return nil, err
		}
	}
	var result reflect.Type
	initialized := false
	for _, b := range branches {
		condition, e := p.scalarType(b.X.X, aliases)
		if e != nil {
			return nil, e
		}
		if selector == nil {
			if baseType(condition) != reflect.TypeFor[bool]() {
				return nil, fmt.Errorf("searched CASE WHEN requires a boolean condition")
			}
		} else if _, e = mergeScalarType(selectorType, condition); e != nil {
			return nil, fmt.Errorf("simple CASE comparison: %w", e)
		}
		value, e := p.scalarType(b.Y, aliases)
		if e != nil {
			return nil, e
		}
		if !initialized {
			result = value
			initialized = true
		} else {
			result, e = mergeScalarType(result, value)
			if e != nil {
				return nil, e
			}
		}
	}
	if otherwise == nil {
		if result == nil {
			return nil, fmt.Errorf("CASE with only NULL result arms requires a concrete result type")
		}
		return nullableType(result), nil
	}
	value, e := p.scalarType(otherwise, aliases)
	if e != nil {
		return nil, e
	}
	result, e = mergeScalarType(result, value)
	if e == nil && result == nil {
		e = fmt.Errorf("CASE with only NULL result arms requires a concrete result type")
	}
	return result, e
}
func baseType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}
func nullableType(t reflect.Type) reflect.Type {
	if t != nil && t.Kind() != reflect.Pointer {
		return reflect.PointerTo(t)
	}
	return t
}
func numericType(t reflect.Type) bool {
	t = baseType(t)
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}
func mergeScalarType(a, b reflect.Type) (reflect.Type, error) {
	if a == nil {
		return nullableType(b), nil
	}
	if b == nil {
		return nullableType(a), nil
	}
	left, right := baseType(a), baseType(b)
	result := left
	if left != right {
		if !numericType(left) || !numericType(right) {
			return nil, fmt.Errorf("cube compose has incompatible types %v and %v", a, b)
		}
		if (left.Kind() >= reflect.Uint && left.Kind() <= reflect.Uint64) != (right.Kind() >= reflect.Uint && right.Kind() <= reflect.Uint64) {
			return nil, fmt.Errorf("cube compose cannot safely merge signed or floating and unsigned integer types %v and %v", a, b)
		}
		result = reflect.TypeFor[int64]()
		if left.Kind() >= reflect.Uint && left.Kind() <= reflect.Uint64 {
			result = reflect.TypeFor[uint64]()
		}
		if left.Kind() == reflect.Float32 || left.Kind() == reflect.Float64 || right.Kind() == reflect.Float32 || right.Kind() == reflect.Float64 {
			result = reflect.TypeFor[float64]()
		}
	}
	if a.Kind() == reflect.Pointer || b.Kind() == reflect.Pointer {
		result = nullableType(result)
	}
	return result, nil
}

// computedNumericType widens SQL computations, not value-selecting CASE arms.
// SQL arithmetic and SUM can exceed a narrow source Go field's range.
func computedNumericType(t reflect.Type) reflect.Type {
	base := baseType(t)
	var result reflect.Type
	switch base.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result = reflect.TypeFor[int64]()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		result = reflect.TypeFor[uint64]()
	case reflect.Float32, reflect.Float64:
		result = reflect.TypeFor[float64]()
	}
	if t.Kind() == reflect.Pointer {
		result = nullableType(result)
	}
	return result
}

// scalarType establishes a conservative native scanner contract for an expression.
func (p *Plan) scalarType(n node.Node, aliases map[string]Column) (reflect.Type, error) {
	switch v := unwrapParenthesis(n).(type) {
	case *expr.Qualify:
		return p.scalarType(v.X, aliases)
	case *expr.Switch:
		return p.caseType(v, aliases)
	case *expr.Literal:
		value, e := literalValue(v)
		return reflect.TypeOf(value), e
	case *expr.Selector:
		r, e := p.resolveSelector(v)
		if e != nil {
			return nil, e
		}
		typ := r.field.Type
		if typ == nil {
			return nil, fmt.Errorf("cube compose field requires concrete type")
		}
		if frame, ok := aliasFrame(r.alias, p.FrameCount); ok && p.nullable[frame-1] {
			typ = nullableType(typ)
		}
		return typ, nil
	case *expr.Ident:
		if c, ok := aliases[normalize(v.Name)]; ok && c.rType != nil {
			return c.rType, nil
		}
	case *expr.Unary:
		typ, e := p.scalarType(v.X, aliases)
		if e != nil {
			return nil, e
		}
		switch strings.ToUpper(strings.TrimSpace(v.Op)) {
		case "IS NULL", "IS NOT NULL":
			return reflect.TypeFor[bool](), nil
		case "NOT":
			if baseType(typ) == reflect.TypeFor[bool]() {
				return typ, nil
			}
		case "+", "-":
			if numericType(typ) {
				if strings.TrimSpace(v.Op) == "-" && baseType(typ).Kind() >= reflect.Uint && baseType(typ).Kind() <= reflect.Uint64 {
					return nil, fmt.Errorf("cube compose cannot safely negate unsigned integer type %v", typ)
				}
				return computedNumericType(typ), nil
			}
		}
	case *expr.Binary:
		op := strings.ToUpper(strings.TrimSpace(v.Op))
		// References and all nested nodes were separately validated against the whitelist.
		switch op {
		case "IS", "IS NOT":
			return reflect.TypeFor[bool](), nil
		case "=", "!=", "<>", "<", "<=", ">", ">=", "IN", "NOT IN", "LIKE", "BETWEEN":
			// SQL comparisons may evaluate to UNKNOWN when an operand is NULL.
			return reflect.TypeFor[*bool](), nil
		}
		a, e := p.scalarType(v.X, aliases)
		if e != nil {
			return nil, e
		}
		b, e := p.scalarType(v.Y, aliases)
		if e != nil {
			return nil, e
		}
		if op == "AND" || op == "OR" {
			if baseType(a) == reflect.TypeFor[bool]() && baseType(b) == reflect.TypeFor[bool]() {
				return mergeScalarType(a, b)
			}
		} else if numericType(a) && numericType(b) {
			merged, err := mergeScalarType(a, b)
			if err != nil {
				return nil, err
			}
			if op == "-" && baseType(merged).Kind() >= reflect.Uint && baseType(merged).Kind() <= reflect.Uint64 {
				return nil, fmt.Errorf("cube compose cannot safely subtract unsigned integer operands")
			}
			if op == "/" {
				// Division can be fractional even for integer operands (for
				// example GoogleSQL INT64 / INT64). Do not constrain the native
				// result scanner to an integer. SQL/dialect semantics stay intact.
				// SQLite can return NULL for a zero divisor even when both
				// operands are nonnullable. Preserve that result in the scanner.
				return reflect.TypeFor[*float64](), nil
			}
			return computedNumericType(merged), nil
		}
	case *expr.Call:
		name := strings.ToUpper(sourceName(v.X))
		if name == "COUNT" {
			return reflect.TypeFor[int64](), nil
		}
		if len(v.Args) == 0 {
			return nil, fmt.Errorf("cube compose function requires arguments")
		}
		typ, e := p.scalarType(v.Args[0], aliases)
		if e != nil {
			return nil, e
		}
		switch name {
		case "COALESCE":
			for _, arg := range v.Args[1:] {
				other, e := p.scalarType(arg, aliases)
				if e != nil {
					return nil, e
				}
				typ, e = mergeScalarType(typ, other)
				if e != nil {
					return nil, e
				}
			}
			return typ, nil
		case "NULLIF":
			return nullableType(typ), nil
		case "AVG":
			if numericType(typ) {
				return reflect.TypeFor[*float64](), nil
			}
		case "SUM":
			if numericType(typ) {
				return nullableType(computedNumericType(typ)), nil
			}
		case "MIN", "MAX":
			return nullableType(typ), nil
		case "ABS", "ROUND":
			if numericType(typ) {
				return computedNumericType(typ), nil
			}
		}
	}
	return nil, fmt.Errorf("cube compose cannot establish compatible scalar type for %T", n)
}

func (r *renderer) renderCase(s *expr.Switch) (string, error) {
	selector, branches, otherwise, err := caseParts(s)
	if err != nil {
		return "", err
	}
	parts := []string{"CASE"}
	if selector != nil {
		value, e := r.renderNode(selector)
		if e != nil {
			return "", e
		}
		parts = append(parts, value)
	}
	for _, b := range branches {
		condition, e := r.renderNode(b.X.X)
		if e != nil {
			return "", e
		}
		value, e := r.renderNode(b.Y)
		if e != nil {
			return "", e
		}
		parts = append(parts, "WHEN", condition, "THEN", value)
	}
	if otherwise != nil {
		value, e := r.renderNode(otherwise)
		if e != nil {
			return "", e
		}
		parts = append(parts, "ELSE", value)
	}
	return strings.Join(append(parts, "END"), " "), nil
}
