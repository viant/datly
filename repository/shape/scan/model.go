package scan

import (
	"embed"
	"reflect"

	"github.com/viant/datly/repository/shape/componenttag"
	"github.com/viant/datly/view/tags"
)

// Result holds scan output produced from a struct source.
type Result struct {
	RootType        reflect.Type
	EmbedFS         *embed.FS
	Fields          []*Field
	ByPath          map[string]*Field
	ViewFields      []*Field
	StateFields     []*Field
	ComponentFields []*Field
}

// Field describes one scanned struct field.
type Field struct {
	Path                string
	Name                string
	Index               []int
	Type                reflect.Type
	QuerySelector       string
	ComponentInputType  reflect.Type
	ComponentOutputType reflect.Type
	ComponentInputName  string
	ComponentOutputName string
	Tag                 reflect.StructTag
	Anonymous           bool
	ViewTypeName        string
	ViewDest            string

	HasViewTag      bool
	HasStateTag     bool
	HasComponentTag bool
	ViewTag         *tags.Tag
	StateTag        *tags.Tag
	ComponentTag    *componenttag.Tag
}
