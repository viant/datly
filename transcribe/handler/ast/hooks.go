package ast

import (
	"fmt"
	"go/token"
	"strings"
)

// WriteHookName is the canonical optional typed relation hook name used by
// both generated targets.
func (r *RelationPlan) WriteHookName() (string, error) {
	if r == nil || len(r.FieldPath) != 1 {
		return "", fmt.Errorf("relation write hook requires one canonical holder field")
	}
	holder := strings.TrimSpace(r.FieldPath[0])
	if !token.IsIdentifier(holder) || token.Lookup(holder).IsKeyword() {
		return "", fmt.Errorf("relation hook holder %q is not a Go identifier", holder)
	}
	return "Before" + holder + "Write", nil
}
