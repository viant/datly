package spec

import "strings"

// Column is canonical compile-time metadata for one scalar view projection.
// Runtime field handles and database services are resolved by later owners.
type Column struct {
	Name string `json:"name"`
	// NameInferred records Go-row discovery; it is not user alias authority.
	NameInferred bool   `json:"nameInferred,omitempty"`
	Source       string `json:"source,omitempty"`
	Expression   string `json:"expression,omitempty"`
	DatabaseType string `json:"databaseType,omitempty"`
	// ExplicitType is exact standalone CAST authority, including pointer semantics.
	ExplicitType bool    `json:"explicitType,omitempty"`
	Type         TypeRef `json:"type"`
	Nullable     bool    `json:"nullable,omitempty"`
	// NotNull records an authoritative table constraint, independently of reader nullability.
	NotNull       bool    `json:"notNull,omitempty"`
	Groupable     *bool   `json:"groupable,omitempty"`
	PrimaryKey    bool    `json:"primaryKey,omitempty"`
	AutoIncrement bool    `json:"autoIncrement,omitempty"`
	Unique        bool    `json:"unique,omitempty"`
	Default       *string `json:"default,omitempty"`
	Tag           string  `json:"tag,omitempty"`
	Codec         *Codec  `json:"codec,omitempty"`
}

// EffectiveType returns the canonical Go value type represented by the column.
// Database nullability supplies pointer inference unless an explicit CAST
// specifies the exact Go type, including a non-pointer type.
func (c *Column) EffectiveType() TypeRef {
	if c == nil {
		return TypeRef{}
	}
	result := c.Type
	name := strings.TrimSpace(result.Name)
	if !c.ExplicitType && c.Nullable && result.Cardinality != CardinalityMany && name != "" && name != "any" && name != "interface{}" &&
		!strings.HasPrefix(name, "*") && !strings.HasPrefix(name, "[]") && !strings.HasPrefix(name, "map[") {
		result.Pointer = true
	}
	return result
}

// Clone returns detached canonical column metadata.
func (c *Column) Clone() *Column {
	if c == nil {
		return nil
	}
	result := *c
	if c.Groupable != nil {
		groupable := *c.Groupable
		result.Groupable = &groupable
	}
	if c.Default != nil {
		value := *c.Default
		result.Default = &value
	}
	if c.Codec != nil {
		codec := *c.Codec
		codec.Args = append([]string(nil), c.Codec.Args...)
		result.Codec = &codec
	}
	return &result
}

func cloneColumns(source []*Column) []*Column {
	if source == nil {
		return nil
	}
	result := make([]*Column, len(source))
	for index, column := range source {
		if column == nil {
			continue
		}
		result[index] = column.Clone()
	}
	return result
}
