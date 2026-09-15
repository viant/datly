package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/tagly/tags"
	xshape "github.com/viant/x/shape"
)

// lowerColumnDeclarations consumes standalone authoring annotations. An
// SQL-aliased CAST remains executable, and type authority never implies a
// transient SQL mapping: that is an explicitly authored tag decision.
// Invariants lower to the existing Go field tag on the targeted outer view.
func lowerColumnDeclarations(parsed *query.Select, root *spec.View, types *typecatalog.Resolver, typeContext *spec.TypeContext) (bool, error) {
	views := canonicalViews(root)
	filtered := make(query.List, 0, len(parsed.List))
	changed := false
	casts := map[*spec.Column]spec.TypeRef{}
	targets := map[*spec.View][]string{}
	for _, item := range parsed.List {
		call, ok := item.Expr.(*expr.Call)
		if !ok {
			filtered = append(filtered, item)
			continue
		}
		name := strings.ToLower(strings.TrimSpace(sqlparser.Stringify(call.X)))
		if name == tag.InvariantName && item.Alias != "" {
			return false, fmt.Errorf("invariant must be a standalone SELECT annotation without an alias")
		}
		if item.Alias != "" || (name != "cast" && name != "tag" && name != tag.InvariantName) {
			filtered = append(filtered, item)
			continue
		}
		var target, typeName, rawTag string
		if name == "cast" {
			cast, err := sqlparser.CastExpression(call)
			if err != nil {
				return false, err
			}
			target, typeName = cast.Operand, cast.Type
		} else {
			if len(call.Args) != 2 {
				return false, fmt.Errorf("%s requires a column and a string literal", name)
			}
			target = sqlparser.Stringify(call.Args[0])
			value, ok := viewDirectiveValue(name, 1, call.Args[1])
			if !ok {
				return false, fmt.Errorf("%s requires a non-empty string literal", name)
			}
			rawTag = value
			if name == tag.InvariantName {
				group, err := tag.ParseInvariant(value)
				if err != nil {
					return false, err
				}
				rawTag = (tags.Tags{&tags.Tag{Name: tag.InvariantName, Values: tags.Values(group)}}).Literal()
			}
		}
		parts, err := sqlparser.TableIdentifierParts(target)
		if err != nil || len(parts) != 2 {
			return false, fmt.Errorf("%s target %q must be a qualified view column", name, target)
		}
		view := views[strings.ToLower(parts[0])]
		if view == nil {
			return false, fmt.Errorf("%s target %q has no canonical view", name, target)
		}
		targets[view] = append(targets[view], parts[1])
		var column *spec.Column
		for _, candidate := range view.Columns {
			if candidate != nil && sameProjectionColumn(candidate, parts[1]) {
				if column != nil {
					return false, fmt.Errorf("%s target %q matches multiple canonical columns", name, target)
				}
				column = candidate
			}
		}
		if column == nil {
			column = &spec.Column{Name: parts[1], Source: parts[1]}
			view.Columns = append(view.Columns, column)
		}
		if typeName != "" {
			typeRef, err := dql.ColumnType(typeName, typeContext)
			if err != nil {
				return false, fmt.Errorf("CAST %s: %w", target, err)
			}
			if _, builtinErr := (xshape.Runtime{}).Type(typeName); builtinErr != nil && types != nil {
				resolved, err := types.ResolveShape(typeName)
				if err != nil {
					return false, fmt.Errorf("CAST %s: %w", target, err)
				}
				if resolved == nil || resolved.Descriptor == nil {
					return false, fmt.Errorf("CAST %s type %q was not found", target, typeName)
				}
			}
			if previous, found := casts[column]; found && previous != typeRef {
				return false, fmt.Errorf("CAST %s conflicts with another CAST declaration", target)
			}
			casts[column] = typeRef
			column.ExplicitType = true
			column.Type = typeRef
		}
		if rawTag != "" {
			// The documented validation shorthand is DQL syntax; canonical Go
			// tags are still parsed by Tagly's full-consumption owner.
			if strings.HasPrefix(rawTag, "validate:") && !strings.HasPrefix(rawTag, `validate:"`) {
				rawTag = (tags.Tags{&tags.Tag{Name: "validate", Values: tags.Values(strings.TrimPrefix(rawTag, "validate:"))}}).Literal()
			}
			additions, err := tags.Parse(rawTag)
			if err != nil {
				return false, fmt.Errorf("tag %s: %w", target, err)
			}
			if len(additions) == 0 {
				return false, fmt.Errorf("tag %s contains no Go tags", target)
			}
			merged, err := tags.Parse(column.Tag)
			if err != nil {
				return false, fmt.Errorf("existing tag %s: %w", target, err)
			}
			for _, addition := range additions {
				if addition.Name == "" {
					return false, fmt.Errorf("tag %s has an empty key", target)
				}
				if previous := merged.Lookup(addition.Name); previous != nil && previous.Values != addition.Values {
					return false, fmt.Errorf("tag %s has conflicting %s values", target, addition.Name)
				}
				merged.SetTag(addition)
			}
			column.Tag = merged.Literal()
		}
		changed = true
	}
	if changed && len(filtered) == 0 {
		return false, fmt.Errorf("column declarations cannot be the entire SELECT projection")
	}
	parsed.List = filtered
	if err := validateDeclaredProjectionTargets(parsed, root, targets); err != nil {
		return false, err
	}
	if containsSQLCall(parsed, func(name string) bool { return name == tag.InvariantName }) {
		return false, fmt.Errorf("invariant must be a standalone outer SELECT annotation")
	}
	if err := validateInvariantProjections(parsed, root); err != nil {
		return false, err
	}
	return changed, nil
}
