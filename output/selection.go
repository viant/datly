package output

import (
	"fmt"
	"reflect"

	dexec "github.com/viant/datly/exec"
	xshape "github.com/viant/x/shape"
)

// selectedPresentation reuses the compiled typed presentation owner for every
// non-JSON codec. A new request plan owns encoder caches for the projected type;
// registered plans and application values are never modified.
func (p *Plan) selectedPresentation(value any, filter dexec.OutputFieldFilter) (*Plan, any, error) {
	indexed, ok := filter.(indexExcluder)
	if !ok {
		return nil, nil, fmt.Errorf("output selection requires canonical field indexes")
	}
	projection, err := (presentationCompiler{excluded: p.exclude, selected: indexed, representation: p.representation, hideJSON: p.effectiveSource != nil, authority: p.formatPresentation}).compile(p.typeOf, "", nil)
	if err != nil {
		return nil, nil, err
	}
	source := reflect.ValueOf(value)
	for source.Kind() == reflect.Pointer && source.Type() != p.typeOf {
		if source.IsNil() {
			source = reflect.Zero(p.typeOf)
			break
		}
		source = source.Elem()
	}
	projected, err := projection.value(source)
	if err != nil {
		return nil, nil, err
	}
	plan, err := p.presentationPlan(projected.Type())
	if err != nil {
		return nil, nil, err
	}
	return plan, projected.Interface(), nil
}

func (p *Plan) presentationPlan(typ reflect.Type) (*Plan, error) {
	holder := ""
	if p.rows != nil {
		holder = p.rows.name
	}
	plan, err := (Compiler{}).compile(CompileInput{Type: typ, DataField: holder}, false)
	if err != nil {
		return nil, err
	}
	if holder != "" && (plan.rows == nil || plan.rows.name != holder) {
		return nil, fmt.Errorf("output presentation hides declared rows")
	}
	if p.rows != nil && p.rows.name == "" && p.rows.sourceType.Kind() == reflect.Struct {
		// Projection changes the type, not the registered direct-row authority.
		plan.rows = newRowsPlan((xshape.Runtime{}).Indirect(typ))
	}
	plan.caseFormat, plan.timeLayout, plan.omitEmpty = p.caseFormat, p.timeLayout, p.omitEmpty
	plan.title = p.title
	plan.nilSlicePolicy = p.nilSlicePolicy

	return plan, nil
}
