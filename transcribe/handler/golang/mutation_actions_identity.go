package golang

import (
	"fmt"
	"go/ast"
)

// sharedIdentityKey projects role-specific tuple types into the cohort's typed
// key. Field order may differ; field names and comparable types must agree.
func (e *actionEmitter) sharedIdentityKey(owner, role actionRole) (ast.Expr, error) {
	expected, actual := owner.record.plan.IdentityKeys(), role.record.plan.IdentityKeys()
	if len(expected) != len(actual) {
		return nil, fmt.Errorf("mutation roles for %s disagree on identity fields", role.record.plan.Table)
	}
	result := &ast.CompositeLit{Type: ast.NewIdent(owner.association.KeyType)}
	for _, want := range expected {
		found := false
		for _, have := range actual {
			if want.Field != have.Field {
				continue
			}
			left, err := e.l.keyType(want.Type)
			if err != nil {
				return nil, err
			}
			right, err := e.l.keyType(have.Type)
			if err != nil {
				return nil, err
			}
			if left != right {
				return nil, fmt.Errorf("mutation roles for %s disagree on identity type %s", role.record.plan.Table, want.Field)
			}
			result.Elts = append(result.Elts, &ast.KeyValueExpr{Key: ast.NewIdent(want.Field), Value: selectExpr(ast.NewIdent("key"), want.Field)})
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("mutation roles for %s disagree on identity field %s", role.record.plan.Table, want.Field)
		}
	}
	return result, nil
}
