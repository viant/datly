package expand

import (
	"github.com/viant/datly/view/keywords"
	"github.com/viant/godiff"
	"github.com/viant/velty/functions"
	"reflect"
)

var FnsDiffer = keywords.AddAndGet("differ",
	functions.NewEntry(
		&Differ{},
		functions.NewFunctionNamespace(reflect.TypeOf(&Differ{})),
	),
)

type (
	Differ struct{}
)

var differRegistry = godiff.NewRegistry()

func (d Differ) Diff(val1 interface{}, val2 interface{}) *godiff.ChangeLog {
	differ, err := differRegistry.Get(reflect.TypeOf(val1), reflect.TypeOf(val2), &godiff.Tag{})
	if err != nil {
		return nil
	}

	diff := differ.Diff(val1, val2)
	return diff
}
