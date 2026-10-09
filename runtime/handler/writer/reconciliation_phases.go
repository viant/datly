package writer

import (
	"fmt"
	"github.com/viant/datly/spec"
	"reflect"
)

// Internal compiled phase authority. Both public source-phases compilation
// gates remain closed until native execution/allocation/payload proofs exist.
type finiteSourcePhases struct {
	insert, update []finiteSourcePhase
}
type finiteSourcePhase struct {
	name, scope, workflow, updateBasis string
	role                               *Relation
	fields                             map[string]Field
	followup                           *finiteSourceFollowup
}
type finiteSourceFollowup struct {
	placement string
	fields    map[string]Field
}

func compileFiniteSourcePhases(root *Record, declaration *spec.Reconciliation) (*finiteSourcePhases, error) {
	if declaration == nil {
		return nil, fmt.Errorf("source phases require a descriptor")
	}
	declaration = declaration.Clone()
	if err := declaration.ValidateSourcePhases(); err != nil {
		return nil, err
	}
	if root == nil || root.EntityType == nil || root.EntityType.Kind() != reflect.Struct || root.Table == "" || root.CurrentField < 0 {
		return nil, fmt.Errorf("source phases require a canonical physical root with Current")
	}
	if _, err := compileFiniteRootDecision(root); err != nil {
		return nil, err
	}
	rootFields, err := reconciliationFields(root, declaration.RootFields, nil)
	if err != nil {
		return nil, err
	}
	roles := map[string]*Relation{}
	for _, relation := range root.Relations {
		if relation == nil || relation.Child == nil || len(relation.Field) != 1 || relation.Field[0] < 0 || relation.Field[0] >= root.EntityType.NumField() {
			return nil, fmt.Errorf("source phases require direct canonical leaf holders")
		}
		field := root.EntityType.Field(relation.Field[0])
		child := relation.Child
		if child.EntityType == nil || child.EntityType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("source phases require canonical child record types")
		}
		if field.Type.Kind() != reflect.Slice || field.Type.Elem() != reflect.PointerTo(child.EntityType) || child.Auxiliary || child.Table == "" || len(child.Relations) != 0 || len(relation.Links) == 0 || len(child.Keys) == 0 || child.CurrentField < 0 || roles[field.Name] != nil {
			return nil, fmt.Errorf("source phases require linked physical leaf collections with Current")
		}
		roles[field.Name] = relation
	}
	if len(roles) != len(declaration.Roles) {
		return nil, fmt.Errorf("source phases must declare every canonical leaf role")
	}
	roleFields := map[string]map[string]Field{}
	for _, role := range declaration.Roles {
		relation := roles[role.Holder]
		if relation == nil {
			return nil, fmt.Errorf("unknown source phase holder %s", role.Holder)
		}
		if role.AdoptIdentity {
			return nil, fmt.Errorf("source phase identity adoption is not yet supported")
		}
		fields, err := reconciliationFields(relation.Child, role.Fields, relation)
		if err != nil {
			return nil, err
		}
		roleFields[role.Holder] = fields
	}
	compile := func(branch []spec.ReconciliationPhase) ([]finiteSourcePhase, error) {
		out := make([]finiteSourcePhase, 0, len(branch))
		for _, phase := range branch {
			result := finiteSourcePhase{name: phase.Name, scope: phase.Scope, workflow: phase.Workflow, updateBasis: phase.UpdateBasis, role: roles[phase.Holder], fields: roleFields[phase.Holder]}
			if phase.Followup != nil {
				fields := map[string]Field{}
				for _, name := range phase.Followup.Fields {
					field, ok := rootFields[name]
					if !ok {
						return nil, fmt.Errorf("unknown source phase followup field %s", name)
					}
					fields[name] = field
				}
				result.followup = &finiteSourceFollowup{placement: phase.Followup.Placement, fields: fields}
			}
			out = append(out, result)
		}
		return out, nil
	}
	insert, err := compile(declaration.SourcePhases.Insert)
	if err != nil {
		return nil, err
	}
	update, err := compile(declaration.SourcePhases.Update)
	if err != nil {
		return nil, err
	}
	return &finiteSourcePhases{insert: insert, update: update}, nil
}
