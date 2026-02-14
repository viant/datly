package scan

import (
	"embed"
	"reflect"

	"github.com/viant/datly/view/tags"
)

// Result holds scan output produced from a struct source.
type Result struct {
	RootType    reflect.Type
	EmbedFS     *embed.FS
	Fields      []*Field
	ByPath      map[string]*Field
	ViewFields  []*Field
	StateFields []*Field
}

// Field describes one scanned struct field.
type Field struct {
	Path      string
	Name      string
	Index     []int
	Type      reflect.Type
	Tag       reflect.StructTag
	Anonymous bool

	HasViewTag  bool
	HasStateTag bool
	ViewTag     *tags.Tag
	StateTag    *tags.Tag
}
