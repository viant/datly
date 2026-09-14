package compiler

import (
	"fmt"
	"go/token"

	"github.com/viant/datly/tag"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// EntityInvariants compiles authored field groups without mutating the entity
// plan. Field order remains canonical and group names share typecatalog naming.
func EntityInvariants(entity *plan.EntityPlan) ([]plan.InvariantGroup, error) {
	if entity == nil {
		return nil, nil
	}
	var result []plan.InvariantGroup
	byName := map[string]int{}
	authored := map[string]string{}
	for _, field := range entity.Fields {
		if field.Invariant == "" {
			continue
		}
		if _, err := tag.ParseInvariant(field.Invariant); err != nil {
			return nil, err
		}
		if field.Identity || field.Relation || !field.Writable {
			return nil, fmt.Errorf("invariant field %s must be a writable business value, not an identity or relation", field.Name)
		}
		name := typecatalog.ExportedFieldName(field.Invariant)
		if !token.IsIdentifier(name) || !token.IsExported(name) {
			return nil, fmt.Errorf("invariant group %q does not form an exported method name", field.Invariant)
		}
		if prior, ok := authored[name]; ok && prior != field.Invariant {
			return nil, fmt.Errorf("invariant groups %q and %q collide as %s", prior, field.Invariant, name)
		}
		authored[name] = field.Invariant
		index, ok := byName[name]
		if !ok {
			index = len(result)
			byName[name] = index
			result = append(result, plan.InvariantGroup{Name: name})
		}
		result[index].Fields = append(result[index].Fields, field.Name)
	}
	return result, nil
}
