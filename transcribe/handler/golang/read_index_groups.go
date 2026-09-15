package golang

import (
	"fmt"
	"go/ast"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

// requiredGroups lowers only explicit relationship tuples. Scalar and primary
// key constructors are already generated; other composite tuples reuse the
// same typed key and collection emitters.
func (e *readIndexEmitter) requiredGroups(read plan.ReadCollection, prefix, slice, row string) ([]ast.Decl, []*ast.Field, []ast.Stmt, error) {
	id := ast.NewIdent
	var declarations []ast.Decl
	var fields []*ast.Field
	var initialize []ast.Stmt
	seen := map[string]bool{}
	for _, group := range read.Groups {
		if len(group.Parts) == 0 {
			return nil, nil, nil, fmt.Errorf("empty relationship group for %s", read.Name)
		}
		parts := append([]plan.KeyPart(nil), group.Parts...)
		names := make([]string, len(parts))
		primary := len(parts) == len(read.Keys)
		for i := range parts {
			names[i] = parts[i].Field
			primary = primary && parts[i].Field == read.Keys[i].Field
			expression, err := e.l.typeReference(parts[i].Type)
			if err != nil {
				return nil, nil, nil, err
			}
			rendered, err := renderExpr(expression)
			if err != nil {
				return nil, nil, nil, err
			}
			parts[i].Type = spec.TypeRef{Name: rendered}
			typ, err := e.l.keyType(parts[i].Type)
			if err != nil {
				return nil, nil, nil, err
			}
			comparable, err := e.comparable(typ)
			if err != nil {
				return nil, nil, nil, err
			}
			if !comparable {
				return nil, nil, nil, fmt.Errorf("relationship group %s.%s is not comparable", read.Name, parts[i].Field)
			}
		}
		suffix := strings.Join(names, "And")
		if primary && len(parts) > 1 {
			suffix = "Key"
		}
		if seen[suffix] {
			return nil, nil, nil, fmt.Errorf("relationship group %s.%s collides", read.Name, suffix)
		}
		seen[suffix] = true
		if len(parts) > 1 && !primary {
			key, err := e.l.compileKey(parts, prefix+suffix+"Key")
			if err != nil {
				return nil, nil, nil, err
			}
			declarations = append(declarations, key.declaration())
			methods, err := e.indexMethods(slice, prefix, recordShape{base: row, pointer: true, many: true}, parts, suffix, key.name)
			if err != nil {
				return nil, nil, nil, err
			}
			declarations = append(declarations, methods...)
		}
		field := read.Name + "GroupedBy" + suffix
		fields = append(fields, namedField(field, id(prefix+"GroupedBy"+suffix)))
		initialize = append(initialize, assignStmt(selectExpr(id("result"), field), callExpr(selectExpr(selectExpr(id("result"), read.Name), "GroupBy"+suffix))))
	}
	return declarations, fields, initialize, nil
}
