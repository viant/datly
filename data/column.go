package data

import (
	"strings"

	"github.com/viant/datly/spec"
)

// Column is immutable view-column metadata used by SQL construction and
// selector validation.
type Column struct {
	Name         string      `json:"name"`
	Column       string      `json:"column,omitempty"`
	Expression   string      `json:"expression,omitempty"`
	Groupable    bool        `json:"groupable,omitempty"`
	Nullable     bool        `json:"nullable,omitempty"`
	NullFallback string      `json:"nullFallback,omitempty"`
	DataType     string      `json:"dataType,omitempty"`
	Tag          string      `json:"tag,omitempty"`
	Codec        *spec.Codec `json:"codec,omitempty"`
}

// ConfigureNullability records the scalar SQL fallback resolved during
// compilation. Only scalar value types receive defaults; callers must retain
// pointer shape rather than pass its dereferenced scalar type.
func (c *Column) ConfigureNullability(nullable bool, typeName string) {
	if c == nil {
		return
	}
	c.Nullable = nullable
	c.NullFallback = ""
	if !nullable {
		return
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
		"float32", "float64":
		c.NullFallback = "0"
	case "string":
		c.NullFallback = "''"
	case "bool":
		c.NullFallback = "FALSE"
	}
}

func (c *Column) SelectExpression(allowNulls bool) string {
	if c == nil {
		return ""
	}
	expression := strings.TrimSpace(c.Expression)
	if expression == "" {
		expression = strings.TrimSpace(c.Column)
	}
	if expression == "" {
		expression = strings.TrimSpace(c.Name)
	}
	if expression == "" {
		return ""
	}
	if c.Nullable && !allowNulls && c.NullFallback != "" {
		expression = "COALESCE(" + expression + ", " + c.NullFallback + ")"
	}
	if c.Name != "" && !strings.EqualFold(strings.TrimSpace(c.Name), expression) {
		return expression + " AS " + strings.TrimSpace(c.Name)
	}
	return expression
}
