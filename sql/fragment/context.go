// Package fragment owns authored SQL predicate fragments and their ordered
// invocation bindings. SQL templates and predicate templates share this owner.
package fragment

import (
	"fmt"
	"strings"

	"github.com/viant/sqlx/metadata/info"
)

type Context struct {
	bindings *Bindings
	dialect  *info.Dialect
}

func New(bindings *Bindings) *Context { return &Context{bindings: bindings} }

// WithDialect configures rendering on a copied context while retaining the
// invocation's single ordered binding collector.
func (c *Context) WithDialect(dialect *info.Dialect) *Context {
	if c == nil {
		return nil
	}
	result := *c
	result.dialect = dialect
	return &result
}

func (c *Context) AppendBinding(value any) (string, error) {
	if c == nil || c.bindings == nil {
		return "", fmt.Errorf("SQL fragment bindings are required")
	}
	return c.bindings.Add(value)
}

func (c *Context) In(column string, value any) (string, error) {
	return c.in(column, predicateValues(value, true), true)
}
func (c *Context) NotIn(column string, value any) (string, error) {
	return c.in(column, predicateValues(value, false), false)
}
func (c *Context) Like(column string, value any) (string, error) {
	return c.like(column, predicateValues(value, true), true, false)
}
func (c *Context) NotLike(column string, value any) (string, error) {
	return c.like(column, predicateValues(value, false), false, false)
}
func (c *Context) Contains(column string, value any) (string, error) {
	return c.like(column, predicateValues(value, true), true, true)
}
func (c *Context) NotContains(column string, value any) (string, error) {
	return c.like(column, predicateValues(value, false), false, true)
}

// Expression follows authored Datly semantics: an empty string omits the
// expression, while zero numbers, false and NULL remain actual bound values.
func (c *Context) Expression(expression string, value any) (string, error) {
	if text, ok := value.(string); ok && text == "" {
		return "", nil
	}
	if _, err := c.AppendBinding(value); err != nil {
		return "", err
	}
	return expression, nil
}

func (c *Context) in(column string, values []any, inclusive bool) (string, error) {
	if len(values) == 0 {
		if inclusive {
			return "1 = 0", nil
		}
		return "0 = 0", nil
	}
	placeholders := make([]string, 0, len(values))
	for _, value := range values {
		placeholder, err := c.AppendBinding(value)
		if err != nil {
			return "", err
		}
		placeholders = append(placeholders, placeholder)
	}
	operator := "IN"
	if !inclusive {
		operator = "NOT IN"
	}
	return fmt.Sprintf("%s %s (%s)", column, operator, strings.Join(placeholders, ", ")), nil
}

func (c *Context) like(column string, values []any, inclusive, wrap bool) (string, error) {
	if len(values) == 0 {
		if inclusive {
			return "1 = 0", nil
		}
		return "0 = 0", nil
	}
	operator, conjunction := "LIKE", " OR "
	if !inclusive {
		operator, conjunction = "NOT LIKE", " AND "
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		text := fmt.Sprint(value)
		if wrap {
			text = "%" + text + "%"
		}
		placeholder, err := c.AppendBinding(text)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", column, operator, placeholder))
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, conjunction) + ")", nil
}
