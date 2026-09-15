package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

// validateRelationSourceOutputs checks the named source before outer selectors
// or internal fields are added. Physical wildcards and resource substitutions
// retain their discovery/resolution boundary; an explicit omission is final.
func validateRelationSourceOutputs(parsed *query.Select, root, view *spec.View) error {
	from := parsed.From
	if view != root {
		found := false
		for _, join := range parsed.Joins {
			if join == nil {
				continue
			}
			alias := join.Alias
			if alias == "" {
				alias = terminalName(join.With)
			}
			if strings.EqualFold(alias, view.Namespace) {
				from = query.From{X: join.With, Alias: alias}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("relation view %s has no authored source", view.Namespace)
		}
	}
	statement := &query.Select{List: query.List{query.NewItem(expr.NewSelector("*"))}, From: from, WithSelects: parsed.WithSelects, WithRecursive: parsed.WithRecursive}
	SQL := (sqlparser.Stringifier{PreserveWindow: true}).String(statement)
	if len(dql.EmbeddedSQLRefs(SQL)) > 0 {
		return nil
	}
	projection := dsql.SelectorProjection{SQL: SQL}
	require := func(name string) error {
		found, complete, err := projection.HasOutput(name)
		if err != nil {
			return fmt.Errorf("relation view %s source projection: %w", view.Namespace, err)
		}
		if complete && !found {
			return fmt.Errorf("required relation output %s.%s is absent from its source projection", view.Namespace, name)
		}
		return nil
	}
	var visit func(*spec.View) error
	visit = func(parent *spec.View) error {
		for _, relation := range parent.Relations {
			if relation == nil || relation.View == nil {
				continue
			}
			for _, link := range relation.On {
				if link == nil {
					continue
				}
				if parent == view {
					if err := require(link.ParentColumn); err != nil {
						return err
					}
				}
				if relation.View == view {
					if err := require(link.ChildColumn); err != nil {
						return err
					}
				}
			}
			if err := visit(relation.View); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}
