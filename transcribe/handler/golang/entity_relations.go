package golang

import (
	"fmt"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
	"strings"
)

type entityRelation struct {
	holder  plan.FieldPath
	child   *recordLowering
	storage string
	marker  string
}

func (e *entityEmitter) entityRelations(record *recordLowering) ([]entityRelation, error) {
	result := []entityRelation{}
	for _, relation := range record.plan.Relations {
		child := e.l.recordByPlan[relation.Child]
		if e.enabled(child) {
			marker := ""
			for _, field := range record.plan.Entity.Fields {
				path := e.entityFieldPath(record, field.Name)
				if strings.Join(path, ".") == strings.Join(relation.FieldPath, ".") {
					marker = field.Name
					break
				}
			}
			if marker == "" && len(relation.FieldPath) == 1 {
				marker = relation.FieldPath[0]
			}
			if marker == "" {
				return nil, fmt.Errorf("relation %v has no canonical presence field", relation.FieldPath)
			}
			result = append(result, entityRelation{holder: relation.FieldPath, child: child, storage: e.childrenName(child), marker: marker})
		}
	}
	for _, field := range record.plan.Entity.Fields {
		if !field.Self || !field.Writable {
			continue
		}
		reference, err := (xshape.Resolver{}).Reference(field.Type.Name)
		if err != nil {
			return nil, err
		}
		shape := recordShape{base: record.value.base, pointer: field.Type.Pointer, many: field.Type.Cardinality == spec.CardinalityMany}
		for _, wrapper := range reference.Wrappers {
			switch wrapper.Kind {
			case xshape.WrapperSlice:
				shape.many = true
			case xshape.WrapperPointer:
				shape.pointer = true
			default:
				return nil, fmt.Errorf("self relation %s has unsupported wrapper %s", field.Name, wrapper.Kind)
			}
		}
		packagePath := record.plan.Entity.Type.Package
		if packagePath == "" {
			packagePath = e.l.config.Package
		}
		resolver := xshape.Resolver{Package: packagePath, Imports: e.l.pathsByAlias}
		expected := record.plan.Entity.Type.Name
		if expected == "" {
			expected = record.value.base
		}
		expected, err = resolver.Canonical(expected)
		if err != nil {
			return nil, err
		}
		if field.Type.Package != "" {
			resolver.Package = field.Type.Package
		}
		actual, err := resolver.Canonical(reference.QualifiedName())
		if err != nil {
			return nil, err
		}
		if actual != expected {
			return nil, fmt.Errorf("entity self relation %s type %s differs from %s", field.Name, actual, expected)
		}
		child := *record
		child.value = shape
		result = append(result, entityRelation{holder: e.entityFieldPath(record, field.Name), child: &child, storage: "self" + field.Name, marker: field.Name})
	}
	return result, nil
}

func (e *entityEmitter) entityFieldPath(record *recordLowering, name string) plan.FieldPath {
	for _, field := range record.plan.Entity.Fields {
		if field.Name == name && len(field.Path) > 0 {
			return field.Path
		}
	}
	return plan.FieldPath{name}
}
