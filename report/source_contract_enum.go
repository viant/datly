package report

import (
	"reflect"

	"github.com/viant/datly/exec"
	"github.com/viant/tagly/choice"
	"github.com/viant/tagly/tags"
	xcontract "github.com/viant/xdatly/contract"
)

// Collection filters retain typed storage for the native query-list provider.
// Their report body therefore needs the source's existing enum input codec.
// Facade presence/null-as-omission stays with the wrapper; source binding still
// owns requiredness. Scalar filters use the compiled source's raw wire type and continue
// through its native scalar codec. Never turn an output alias into input policy.
func projectFilterEnum(source *exec.EffectiveContract, index []int, target reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
	original, err := contractInputField(source.Input, source.InputRepresentation, index)
	if err != nil {
		return nil, err
	}
	enumeration, err := choice.ParseEnum(original.Tag, original.Type)
	if err != nil {
		return nil, err
	}
	if enumeration == nil || enumeration.CollectionItems() == nil {
		return nil, nil
	}
	if dereference(target) != dereference(original.Type) {
		return nil, nil
	}
	own, err := choice.ParseEnum(field.Tag, target)
	if err != nil {
		return nil, err
	}
	if own != nil {
		return nil, nil
	} // Preserve the derived producer's own selected restriction.
	sourceTags, err := tags.Parse(string(original.Tag))
	if err != nil {
		return nil, err
	}
	selected, err := tags.Parse(string(field.Tag))
	if err != nil {
		return nil, err
	}
	for _, item := range sourceTags {
		if inputEnumTag(item.Name) {
			selected = append(selected, item)
		}
	}
	updated := field.Clone()
	updated.Tag = reflect.StructTag(selected.Literal())
	return updated, nil
}
func inputEnumTag(name string) bool {
	switch name {
	case "mcpEnum", "choice", "enumAccept", "enumCase", "enumTrim", "enumEmpty", "enumZero", "enumInteger", "enumUnknownInteger", "enumUnknownString", "enumAlias":
		return true
	}
	return false
}
