// Package criteria validates client predicates using the SQL parser's AST and
// emits only approved columns, operators, and bound values.
package criteria

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
)

// Compiler carries the immutable set of names allowed by view selector policy.
// Values map client names to trusted SQL column expressions.
type Compiler struct {
	Columns map[string]Column
	Methods map[string]Method
}

type Column struct {
	Expression string
	Type       reflect.Type
	TimeLayout string
}

type Method struct {
	Name string
	Args []reflect.Type
}

func (c *Compiler) Compile(source string, placeholders []any) (string, []any, error) {
	sql, args, _, err := c.CompileWithColumns(source, placeholders)
	return sql, args, err
}

// CompileWithColumns also reports the trusted columns referenced by this
// invocation, including column comparisons on the right-hand side.
func (c *Compiler) CompileWithColumns(source string, placeholders []any) (string, []any, []Column, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		if len(placeholders) > 0 {
			return "", nil, nil, fmt.Errorf("unused selector criteria placeholders")
		}
		return "", nil, nil, nil
	}
	cursor := parsly.NewCursor("criteria", []byte(source+" "), 0)
	qualified := &expr.Qualify{}
	if err := sqlparser.ParseQualify(cursor, qualified); err != nil {
		return "", nil, nil, fmt.Errorf("invalid selector criteria: %w", err)
	}
	if strings.TrimSpace(string(cursor.Input[cursor.Pos:])) != "" {
		return "", nil, nil, fmt.Errorf("invalid trailing selector criteria")
	}
	invocation := compilation{compiler: c, placeholders: placeholders}
	sql, err := invocation.predicate(qualified.X)
	if err != nil {
		return "", nil, nil, err
	}
	if invocation.index != len(placeholders) {
		return "", nil, nil, fmt.Errorf("unused selector criteria placeholders")
	}
	return "(" + sql + ")", invocation.args, invocation.columns, nil
}

type compilation struct {
	compiler           *Compiler
	placeholders, args []any
	index              int
	columns            []Column
}

func (c *compilation) predicate(n node.Node) (string, error) {
	switch actual := n.(type) {
	case *expr.Parenthesis:
		cursor := parsly.NewCursor("criteria", []byte(actual.Raw[1:len(actual.Raw)-1]+" "), 0)
		qualified := &expr.Qualify{}
		if err := sqlparser.ParseQualify(cursor, qualified); err != nil {
			return "", err
		}
		if strings.TrimSpace(string(cursor.Input[cursor.Pos:])) != "" {
			return "", fmt.Errorf("invalid nested criteria")
		}
		sql, err := c.predicate(qualified.X)
		return "(" + sql + ")", err
	case *expr.Binary:
		if actual.Op == "" && actual.Y == nil {
			return c.predicate(actual.X)
		}
		if actual.Op == "" {
			return "", fmt.Errorf("missing criteria operator")
		}
		actual = actual.Normalize()
		op := strings.ToUpper(strings.TrimSpace(actual.Op))
		if op == "AND" || op == "OR" {
			x, err := c.predicate(actual.X)
			if err != nil {
				return "", err
			}
			y, err := c.predicate(actual.Y)
			return x + " " + op + " " + y, err
		}
		switch op {
		case "=", "!=", "<>", ">", ">=", "<", "<=", "LIKE", "IN", "NOT IN", "NOT LIKE", "IS", "IS NOT":
		default:
			return "", fmt.Errorf("unsupported criteria operator %q", op)
		}
		column, err := c.column(actual.X)
		if err != nil {
			return "", err
		}
		if err = column.operator(op); err != nil {
			return "", err
		}
		var y string
		if op == "IN" || op == "NOT IN" {
			parent, ok := actual.Y.(*expr.Parenthesis)
			if !ok {
				return "", fmt.Errorf("criteria IN requires a value list")
			}
			y, err = c.list(parent, column)
		} else {
			y, err = c.value(actual.Y, column)
		}
		return column.Expression + " " + op + " " + y, err
	default:
		return "", fmt.Errorf("unsupported criteria predicate %T", n)
	}
}

// Parse each list value independently because the parser's query AST may retain
// a partial parenthesized list. Every byte must belong to a parsed value or its
// comma separator before any expression is accepted.
func (c *compilation) list(parent *expr.Parenthesis, column Column) (string, error) {
	remaining := strings.TrimSpace(parent.Raw[1 : len(parent.Raw)-1])
	var values []string
	for remaining != "" {
		cursor := parsly.NewCursor("criteria list", []byte(remaining+" "), 0)
		qualified := &expr.Qualify{}
		if err := sqlparser.ParseQualify(cursor, qualified); err != nil {
			return "", err
		}
		binary, ok := qualified.X.(*expr.Binary)
		if !ok || binary.Op != "" || binary.Y != nil {
			return "", fmt.Errorf("criteria IN requires scalar values")
		}
		value, err := c.value(binary.X, column)
		if err != nil {
			return "", err
		}
		values = append(values, value)
		remaining = strings.TrimSpace(string(cursor.Input[cursor.Pos:]))
		if remaining == "" {
			break
		}
		if remaining[0] != ',' {
			return "", fmt.Errorf("invalid criteria IN separator")
		}
		remaining = strings.TrimSpace(remaining[1:])
		if remaining == "" {
			return "", fmt.Errorf("missing criteria IN value")
		}
	}
	if len(values) == 0 {
		return "", fmt.Errorf("empty criteria IN list")
	}
	return "(" + strings.Join(values, ", ") + ")", nil
}

func (c *compilation) column(n node.Node) (Column, error) {
	switch n.(type) {
	case *expr.Ident, *expr.Selector:
	default:
		return Column{}, fmt.Errorf("criteria requires an allowed column")
	}
	name := sqlparser.Stringify(n)
	for candidate, target := range c.compiler.Columns {
		if strings.EqualFold(candidate, name) {
			c.columns = append(c.columns, target)
			return target, nil
		}
	}
	return Column{}, fmt.Errorf("criteria column %q is not filterable", name)
}

func (c *compilation) value(n node.Node, column Column) (string, error) {
	var value any
	var err error
	switch actual := n.(type) {
	case *expr.Ident, *expr.Selector:
		other, err := c.column(n)
		if err != nil {
			return "", err
		}
		if column.Type != nil && other.Type != nil && column.scalarType() != other.scalarType() {
			return "", fmt.Errorf("criteria column types do not match")
		}
		return other.Expression, nil
	case *expr.Call:
		return c.method(actual)
	case *expr.Placeholder:
		if actual.Name != "?" || c.index >= len(c.placeholders) {
			return "", fmt.Errorf("missing or unsupported criteria placeholder")
		}
		value = c.placeholders[c.index]
		c.index++
	case *expr.Literal:
		switch actual.Kind {
		case "string":
			if len(actual.Value) < 2 || actual.Value[0] != '\'' || actual.Value[len(actual.Value)-1] != '\'' {
				return "", fmt.Errorf("invalid criteria string")
			}
			value = strings.ReplaceAll(actual.Value[1:len(actual.Value)-1], "''", "'")
		case "int":
			value, err = strconv.ParseInt(actual.Value, 10, 64)
			if err != nil && !strings.HasPrefix(actual.Value, "-") {
				value, err = strconv.ParseUint(actual.Value, 10, 64)
			}
		case "numeric":
			if !strings.ContainsAny(actual.Value, ".eE") {
				value, err = strconv.ParseInt(actual.Value, 10, 64)
				if err != nil && !strings.HasPrefix(actual.Value, "-") {
					value, err = strconv.ParseUint(actual.Value, 10, 64)
				}
			} else {
				value, err = strconv.ParseFloat(actual.Value, 64)
			}
		case "bool":
			value, err = strconv.ParseBool(actual.Value)
		case "null":
			value = nil
		default:
			return "", fmt.Errorf("unsupported criteria literal %q", actual.Kind)
		}
	default:
		return "", fmt.Errorf("unsupported criteria value %T", n)
	}
	if err != nil {
		return "", fmt.Errorf("invalid criteria literal: %w", err)
	}
	value, err = column.convert(value)
	if err != nil {
		return "", err
	}
	c.args = append(c.args, value)
	return "?", nil
}
