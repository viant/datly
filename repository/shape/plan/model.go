package plan

import (
	"embed"
	"reflect"
	"strings"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view/state"
)

// Result is normalized shape plan produced from scan descriptors.
type Result struct {
	RootType reflect.Type
	EmbedFS  *embed.FS

	Fields           []*Field
	ByPath           map[string]*Field
	Views            []*View
	ViewsByName      map[string]*View
	States           []*State
	Components       []*ComponentRoute
	Types            []*Type
	ColumnsDiscovery bool
	Const            map[string]string
	TypeContext      *typectx.Context
	Directives       *dqlshape.Directives
	Diagnostics      []*dqlshape.Diagnostic
}

type ComponentRoute struct {
	Path       string
	FieldName  string
	Type       reflect.Type
	InputType  reflect.Type
	OutputType reflect.Type
	InputName  string
	OutputName string
	ViewName   string
	SourceURL  string
	SummaryURL string
	Name       string
	RoutePath  string
	Method     string
	Connector  string
	Marshaller string
	Handler    string
}

// Type is normalized type metadata collected during compile.
type Type struct {
	Name        string
	Alias       string
	DataType    string
	Cardinality string
	Package     string
	ModulePath  string
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
	Mode                   string
	Table                  string
	Module                 string
	Connector              string
	CacheRef               string
	Partitioner            string
	PartitionedConcurrency int
	RelationalConcurrency  int
	SQL                    string
	SQLURI                 string
	Summary                string
	SummaryURL             string
	SummaryName            string
	Relations              []*Relation
	Holder                 string

	AllowNulls             *bool
	Groupable              *bool
	SelectorNamespace      string
	SelectorLimit          *int
	SelectorNoLimit        *bool
	SelectorCriteria       *bool
	SelectorProjection     *bool
	SelectorOrderBy        *bool
	SelectorOffset         *bool
	SelectorPage           *bool
	SelectorFilterable     []string
	SelectorOrderByColumns map[string]string
	SchemaType             string
	ColumnsDiscovery       bool
	Self                   *SelfReference

	Cardinality string
	ElementType reflect.Type
	FieldType   reflect.Type
	Declaration *ViewDeclaration
}

// ViewDeclaration captures declaration options used to derive a view from DQL directives.
type ViewDeclaration struct {
	Tag           string
	TypeName      string
	Dest          string
	Codec         string
	CodecArgs     []string
	HandlerName   string
	HandlerArgs   []string
	StatusCode    *int
	ErrorMessage  string
	QuerySelector string
	CacheRef      string
	Limit         *int
	Cacheable     *bool
	When          string
	Scope         string
	DataType      string
	Of            string
	Value         string
	Async         bool
	Output        bool
	Predicates    []*ViewPredicate
	ColumnsConfig map[string]*ViewColumnConfig
}

// ViewPredicate captures WithPredicate / EnsurePredicate metadata.
type ViewPredicate struct {
	Name      string
	Source    string
	Ensure    bool
	Arguments []string
}

// ViewColumnConfig captures declaration-level per-column overrides.
type ViewColumnConfig struct {
	DataType  string
	Tag       string
	Groupable *bool
}

// Relation is normalized relation metadata extracted from DQL joins.
type Relation struct {
	Name          string
	Parent        string
	Holder        string
	Ref           string
	Table         string
	Kind          string
	Raw           string
	ColumnsConfig map[string]*ViewColumnConfig
	On            []*RelationLink
	Warnings      []string
}

// RelationLink represents one parent/ref join predicate.
type RelationLink struct {
	ParentField     string
	ParentNamespace string
	ParentColumn    string
	RefField        string
	RefNamespace    string
	RefColumn       string
	Expression      string
}

// SelfReference captures self-join tree metadata parsed from DQL.
type SelfReference struct {
	Holder string
	Child  string
	Parent string
}

// State is a normalized parameter field plan.
type State struct {
	state.Parameter `yaml:",inline"`
	QuerySelector   string
	OutputDataType  string
	EmitOutput      bool
}

func (s *State) KindString() string {
	if s == nil || s.In == nil {
		return ""
	}
	return strings.TrimSpace(string(s.In.Kind))
}

func (s *State) InName() string {
	if s == nil || s.In == nil {
		return ""
	}
	return strings.TrimSpace(s.In.Name)
}
