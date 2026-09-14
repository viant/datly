package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/tagly/tags"
)

const internalColumnTag = "internal"

// lowerProjectionExclusions turns the DQL star exclusion extension into
// canonical column metadata and removes it from executable SQL.
func lowerProjectionExclusions(parsed *query.Select, root *spec.View) (bool, error) {
	if parsed == nil || root == nil {
		return false, nil
	}
	views := canonicalViews(root)
	rewritten := false
	for _, item := range parsed.List {
		star, ok := projectionStar(item)
		if !ok || len(star.Except) == 0 {
			continue
		}
		namespace := starNamespace(star)
		if namespace == "" || namespace == "*" {
			namespace = root.Namespace
		}
		view := views[strings.ToLower(namespace)]
		if view == nil {
			return false, &Error{
				Code:  CodeViewDirective,
				Cause: fmt.Errorf("projection exclusion namespace %q has no canonical view", namespace),
			}
		}
		for _, rawName := range star.Except {
			name := projectionColumnName(rawName)
			if name == "" {
				return false, &Error{Code: CodeViewDirective, Cause: fmt.Errorf("projection exclusion for %q has an empty column", namespace)}
			}
			markInternalColumn(view, name)
		}
		star.Except = nil
		rewritten = true
	}
	return rewritten, nil
}

func projectionStar(item *query.Item) (*expr.Star, bool) {
	if item == nil {
		return nil, false
	}
	star, ok := item.Expr.(*expr.Star)
	return star, ok
}

func starNamespace(star *expr.Star) string {
	if star == nil {
		return ""
	}
	switch actual := star.X.(type) {
	case *expr.Selector:
		return strings.TrimSpace(actual.Name)
	case *expr.Ident:
		return strings.TrimSpace(actual.Name)
	default:
		return ""
	}
}

func canonicalViews(root *spec.View) map[string]*spec.View {
	result := map[string]*spec.View{}
	indexCanonicalViews(root, result)
	return result
}

func indexCanonicalViews(view *spec.View, result map[string]*spec.View) {
	if view == nil {
		return
	}
	if namespace := strings.ToLower(strings.TrimSpace(view.Namespace)); namespace != "" {
		result[namespace] = view
	}
	for _, relation := range view.Relations {
		if relation != nil {
			indexCanonicalViews(relation.View, result)
		}
	}
}

func projectionColumnName(value string) string {
	return strings.Trim(strings.TrimSpace(value), "`\"[]")
}

func markInternalColumn(view *spec.View, name string) {
	for _, column := range view.Columns {
		if column == nil || !sameProjectionColumn(column, name) {
			continue
		}
		column.Tag = withInternalColumnTag(column.Tag)
		return
	}
	view.Columns = append(view.Columns, &spec.Column{
		Name: name, Source: name, Tag: withInternalColumnTag(""),
	})
}

func sameProjectionColumn(column *spec.Column, name string) bool {
	wanted := strings.ToLower(projectionColumnName(name))
	return wanted != "" && (strings.ToLower(projectionColumnName(column.Name)) == wanted ||
		strings.ToLower(projectionColumnName(column.Source)) == wanted)
}

func withInternalColumnTag(raw string) string {
	parsed := tags.NewTags(strings.TrimSpace(raw))
	parsed.Set(internalColumnTag, "true")
	return parsed.Stringify()
}
