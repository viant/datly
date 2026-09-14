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
	projection, err := (presentationCompiler{excluded: p.exclude, selected: indexed}).compile(reflect.TypeOf(value), "", nil)
	if err != nil {
		return nil, nil, err
	}
	projected, err := projection.value(reflect.ValueOf(value))
	if err != nil {
		return nil, nil, err
	}
	holder := ""
	if p.rows != nil {
		holder = p.rows.name
	}
	plan, err := (Compiler{}).Compile(CompileInput{Type: projected.Type(), DataField: holder})
	if err != nil {
		return nil, nil, err
	}
	if p.rows != nil && p.rows.name == "" && p.rows.sourceType.Kind() == reflect.Struct {
		// Projection changes the type, not the registered direct-row authority.
		plan.rows = newRowsPlan((xshape.Runtime{}).Indirect(projected.Type()))
	}
	plan.caseFormat, plan.timeLayout, plan.omitEmpty = p.caseFormat, p.timeLayout, p.omitEmpty
	plan.title = p.title
	return plan, projected.Interface(), nil
}
