package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

// identifierBindings describes authority, never the values used for discovery.
type identifierBindings struct {
	declared map[string]bool // true only for immutable constant bindings
	input    *TemplateInput
}

func newIdentifierBindings(component *spec.Component, input *TemplateInput) identifierBindings {
	result := identifierBindings{declared: map[string]bool{}, input: input}
	if component.Settings != nil {
		for name := range component.Settings.Const {
			result.declared[strings.ToLower(name)] = true
		}
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param != nil && !param.EmitOutput && !strings.EqualFold(param.Source.Kind, "output") {
			result.declared[strings.ToLower(param.Name)] = strings.EqualFold(param.Source.Kind, "const")
		}
	}
	return result
}

func (b identifierBindings) dynamic(table string) (bool, error) {
	if !strings.Contains(table, "$") {
		return false, nil
	}
	root, _, err := veltyparser.ParseWithSpansDetailed([]byte(table))
	if err != nil {
		return false, fmt.Errorf("parse identifier dependencies: %w", err)
	}
	dynamic := false
	for _, statement := range root.Statements() {
		selector, ok := statement.(*expr.Select)
		if !ok {
			if _, literal := statement.(*stmt.Append); literal {
				continue
			}
			return false, fmt.Errorf("unsupported identifier dependency in %q", table)
		}
		name := selector.ID
		if name == "Unsafe" {
			member, ok := selector.X.(*expr.Select)
			if !ok || member.X != nil {
				return false, fmt.Errorf("unsupported identifier dependency in %q", table)
			}
			name = member.ID
		} else {
			for next := selector.X; next != nil; {
				member, ok := next.(*expr.Select)
				if !ok {
					return false, fmt.Errorf("unsupported identifier dependency in %q", table)
				}
				next = member.X
			}
		}
		immutable, found := b.declared[strings.ToLower(name)]
		if !found && b.input != nil {
			if _, found = b.input.Const.Lookup(name); found {
				immutable = true
			} else {
				for _, variable := range b.input.Variables {
					if strings.EqualFold(variable.Name, name) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			return false, fmt.Errorf("identifier %q references undeclared binding %q", table, name)
		}
		dynamic = dynamic || !immutable
	}
	return dynamic, nil
}

// NormalizeIdentifierMetadata removes only parser-inferred invocation targets.
// Explicit metadata must remain valid for strict instance-time resolution.
func NormalizeIdentifierMetadata(component *spec.Component) error {
	return normalizeIdentifierMetadata(component, nil)
}

func validateIdentifierTags(bindings identifierBindings, column *spec.Column) error {
	if !strings.Contains(column.Tag, "$") {
		return nil
	}
	tag := sqlio.ParseTag(reflect.StructTag(column.Tag))
	for _, option := range []struct{ name, value string }{{"table", tag.Table}, {"db", tag.Db}, {"refTable", tag.RefTable}, {"refDb", tag.RefDb}} {
		dynamic, err := bindings.dynamic(option.value)
		if err != nil {
			return fmt.Errorf("explicit sqlx %s: %w", option.name, err)
		}
		if dynamic {
			return fmt.Errorf("explicit sqlx %s=%q requires invocation-bound identifiers; only immutable constants are supported", option.name, option.value)
		}
	}
	return nil
}

func normalizeIdentifierMetadata(component *spec.Component, input *TemplateInput) error {
	if component == nil {
		return fmt.Errorf("identifier metadata component is required")
	}
	bindings := newIdentifierBindings(component, input)
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || seen[view] {
			return nil
		}
		seen[view] = true
		if view.Source != nil {
			dynamic, err := bindings.dynamic(view.Source.Table)
			if err != nil {
				return fmt.Errorf("view %s table: %w", view.CanonicalName(), err)
			}
			if dynamic {
				if !view.Source.TableInferred {
					return fmt.Errorf("view %s: explicit table metadata %q requires invocation-bound identifiers; only immutable constants are supported", view.CanonicalName(), view.Source.Table)
				}
				view.Source.Table = ""
			}
		}
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			// SQLX identifier options are explicit at this stage; do not erase
			// authored keys, constraints, or tag options to make them compile.
			if err := validateIdentifierTags(bindings, column); err != nil {
				return fmt.Errorf("view %s column %s: %w", view.CanonicalName(), column.Name, err)
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(component.RootView); err != nil {
		return err
	}
	for _, view := range component.Views {
		if err := visit(view); err != nil {
			return err
		}
	}
	return nil
}
