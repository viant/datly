package plan

import (
	"embed"
	"reflect"

	"github.com/viant/datly/repository/shape/typectx"
)

// Result is normalized shape plan produced from scan descriptors.
type Result struct {
	RootType reflect.Type
	EmbedFS  *embed.FS

	Fields      []*Field
	ByPath      map[string]*Field
	Views       []*View
	ViewsByName map[string]*View
	States      []*State
	TypeContext *typectx.Context
}

// Field is a normalized projection of scanned field metadata.
type Field struct {
	Path  string
	Name  string
	Type  reflect.Type
	Index []int
}

// View is a normalized view field plan.
type View struct {
	Path                   string
	Name                   string
	Ref                    string
	Table                  string
	Connector              string
	CacheRef               string
	Partitioner            string
	PartitionedConcurrency int
	RelationalConcurrency  int
	SQL                    string
	SQLURI                 string
	Summary                string
	Links                  []string
	Holder                 string

	Cardinality string
	ElementType reflect.Type
	FieldType   reflect.Type
}

// State is a normalized parameter field plan.
type State struct {
	Path         string
	Name         string
	Kind         string
	In           string
	When         string
	Scope        string
	DataType     string
	Required     *bool
	Async        bool
	Cacheable    *bool
	With         string
	URI          string
	ErrorCode    int
	ErrorMessage string

	TagType       reflect.Type
	EffectiveType reflect.Type
}
