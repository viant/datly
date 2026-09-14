package tag

import (
	"fmt"
	"strings"

	tagly "github.com/viant/tagly/tags"
)

// RelationLink is one typed parent-to-child equality from an on tag.
type RelationLink struct {
	Parent RelationPart
	Child  RelationPart
}

// RelationPart identifies an optional Go field, SQL namespace, and required
// SQL column.
type RelationPart struct {
	Field     string
	Namespace string
	Column    string
	Include   *bool
}

// ParseRelation parses the complete comma-separated on-tag relation contract.
func ParseRelation(value string) ([]*RelationLink, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	var result []*RelationLink
	err := tagly.Values(value).MatchPairs(func(parent, child string) error {
		parent = strings.TrimSpace(parent)
		child = strings.TrimSpace(child)
		if parent == "" || child == "" || strings.Contains(child, "=") {
			return fmt.Errorf("invalid relation link %q=%q", parent, child)
		}
		parentPart, err := parseRelationPart(parent)
		if err != nil {
			return fmt.Errorf("invalid relation parent %q: %w", parent, err)
		}
		childPart, err := parseRelationPart(child)
		if err != nil {
			return fmt.Errorf("invalid relation child %q: %w", child, err)
		}
		if childPart.Include != nil {
			return fmt.Errorf("relation include flag is only valid on the parent side")
		}
		result = append(result, &RelationLink{Parent: parentPart, Child: childPart})
		return nil
	})
	return result, err
}

func parseRelationPart(value string) (RelationPart, error) {
	value = strings.TrimSpace(value)
	result := RelationPart{}
	for _, option := range []struct {
		suffix string
		value  bool
	}{{suffix: "(true)", value: true}, {suffix: "(false)", value: false}} {
		if strings.HasSuffix(value, option.suffix) {
			included := option.value
			result.Include = &included
			value = strings.TrimSpace(strings.TrimSuffix(value, option.suffix))
			break
		}
	}
	if strings.ContainsAny(value, "()=") {
		return RelationPart{}, fmt.Errorf("unsupported relation delimiter")
	}
	if field, column, ok := strings.Cut(value, ":"); ok {
		if strings.Contains(column, ":") {
			return RelationPart{}, fmt.Errorf("expected at most one field separator")
		}
		result.Field = strings.TrimSpace(field)
		result.Column = strings.TrimSpace(column)
	} else {
		result.Column = value
	}
	if result.Column == "" {
		return RelationPart{}, fmt.Errorf("column is required")
	}
	if namespace, column, ok := strings.Cut(result.Column, "."); ok {
		if namespace == "" || column == "" || strings.Contains(column, ".") {
			return RelationPart{}, fmt.Errorf("expected namespace.column")
		}
		result.Namespace = strings.TrimSpace(namespace)
		result.Column = strings.TrimSpace(column)
		if result.Namespace == "" || result.Column == "" {
			return RelationPart{}, fmt.Errorf("expected namespace.column")
		}
	}
	return result, nil
}
