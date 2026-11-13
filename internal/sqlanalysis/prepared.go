package sqlanalysis

import (
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"reflect"
)

// PreparedSQL reuses conditional template analysis without evaluating request
// values. Offline metadata can describe one SQL shape: every alternative must
// produce the same complete checked AST. Redundant raw query text is excluded
// only when the native checker supplied its parsed subtree. Authored SQL stays intact.
func PreparedSQL(source string, validate InputValidator) (string, error) {
	variants, err := ConditionalSQL(source, validate)
	if err != nil {
		return "", err
	}
	var result string
	var identity *query.Select
	for index, variant := range variants {
		candidate, err := DiscoveryPredicates(variant)
		if err != nil {
			return "", err
		}
		analysis, parsed, err := Checked(candidate, validate)
		if err != nil {
			return "", err
		}
		parsed = preparedIdentity(parsed)
		if index > 0 && !reflect.DeepEqual(identity, parsed) {
			return "", fmt.Errorf("conditional SQL changes its analyzed statement; requires bound discovery")
		}
		if index == 0 {
			result, identity = analysis, parsed
		}
	}
	if result == "" {
		return "", fmt.Errorf("prepared SQL has no analyzed statement")
	}
	return result, nil
}

// preparedIdentity receives an owned, checked AST. Keep every semantic field,
// including limits, offsets, windows, predicates, aliases and unparsed content.
// Raw subquery/CTE text duplicates the structured child and can differ solely
// because conditional template assignments emitted different whitespace.
func preparedIdentity(parsed *query.Select) *query.Select {
	var visit func(node.Node)
	visit = func(root node.Node) {
		sqlparser.Traverse(root, func(value node.Node) bool {
			switch actual := value.(type) {
			case *query.Select:
				// Generic SQL traversal does not visit CTE declarations.
				for _, with := range actual.WithSelects {
					if with != nil && with.X != nil {
						with.Raw = ""
						visit(with.X)
					}
				}
			case *expr.Raw:
				if nested, ok := actual.X.(*query.Select); ok {
					actual.Raw = ""
					visit(nested)
					return false
				}
			}
			return true
		})
	}
	visit(parsed)
	return parsed
}
