package expand

import (
	"github.com/viant/datly/view/keywords"
	"github.com/viant/godiff"
	"github.com/viant/velty/functions"
	"reflect"
)

var fnsDiffer = keywords.AddAndGet("differ",
	functions.NewEntry(
		&Differ{},
		functions.NewFunctionNamespace(reflect.TypeOf(&Differ{})),
	),
)

type (
	Differ struct{}
)

var differRegistry = godiff.NewRegistry()

func (d Differ) Diff(from interface{}, fo interface{}) *godiff.ChangeLog {
	differ, err := differRegistry.Get(reflect.TypeOf(from), reflect.TypeOf(fo), &godiff.Tag{})
	if err != nil {
		return nil
	}

	diff := differ.Diff(from, fo)
	return diff
}
