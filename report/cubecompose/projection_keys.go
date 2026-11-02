package cubecompose

import (
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"strings"
)

// ProjectionKeys retains only direct dimension provenance. The initial graph
// capability deliberately accepts one flat frame, never joins or UNIONs.
func (p *Plan) ProjectionKeys() (map[string]string, error) {
	if p == nil || p.FrameCount != 1 || len(p.unionBranches) > 0 || p.selectQuery == nil || p.selectQuery.Union != nil || len(p.selectQuery.Joins) > 0 {
		return nil, fmt.Errorf("projection relations require one non-UNION source frame")
	}
	result := map[string]string{}
	grouped := len(p.selectQuery.GroupBy) > 0
	for _, item := range p.selectQuery.GroupBy {
		if item == nil {
			return nil, fmt.Errorf("projection relations require explicit qualified dimension GROUP BY keys")
		}
		ref, err := p.resolveSelector(item.Expr)
		if err != nil || ref.field.Role != Dimension {
			// Unqualified output aliases can resolve to a different source SQL
			// column, including columns outside this catalog. Do not infer or
			// rewrite their meaning at the projection-relation boundary.
			return nil, fmt.Errorf("projection relations require explicit qualified dimension GROUP BY keys")
		}
	}
	aggregate := projectionHasAggregate(p.selectQuery)
	for i, item := range p.selectQuery.List {
		ref, err := p.resolveSelector(item.Expr)
		if err != nil || ref.field.Role != Dimension {
			continue
		}
		if grouped || aggregate {
			retained := false
			for _, key := range p.selectQuery.GroupBy {
				other, e := p.resolveSelector(key.Expr)
				if e == nil && other.alias == ref.alias && other.field.SQLName == ref.field.SQLName {
					retained = true
				}
			}
			if !retained {
				continue
			}
		}
		name := ref.field.SQLName
		if _, exists := result[name]; exists {
			return nil, fmt.Errorf("ambiguous direct dimension projection %s", name)
		}
		result[name] = p.Columns[i].Name
	}
	return result, nil
}

func projectionHasAggregate(n node.Node) bool {
	found := false
	// Reuse the parser's complete AST traversal, including Range, IN lists,
	// Qualify and nested CASE, rather than maintaining a second expression walk.
	sqlparser.Traverse(n, func(current node.Node) bool {
		if call, ok := current.(*expr.Call); ok {
			switch strings.ToUpper(sourceName(call.X)) {
			case "SUM", "MIN", "MAX", "AVG", "COUNT":
				found = true
			}
		}
		return !found
	})
	return found
}
