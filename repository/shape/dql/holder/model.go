package holder

// ComponentHolder is a meta/tag-driven canonical holder for DQL/YAML parity
// and conversion to Datly internal/YAML representation.
type ComponentHolder struct {
	Route        RouteShape        `shape:"route"`
	Component    ComponentShape    `shape:"component"`
	Input        IOShape           `shape:"input"`
	Output       IOShape           `shape:"output"`
	ViewGraph    ViewGraphShape    `shape:"views"`
	Dependencies DependencyShape   `shape:"deps"`
	Meta         map[string]string `shape:"meta"`
}

type RouteShape struct {
	Name        string `shape:"route.name"`
	URI         string `shape:"route.uri"`
	Method      string `shape:"route.method"`
	Service     string `shape:"route.service"`
	Description string `shape:"route.description"`
	MCPTool     bool   `shape:"route.mcpTool"`
	ViewRef     string `shape:"route.viewRef"`
}

type ComponentShape struct {
	Name         string            `shape:"component.name"`
	Package      string            `shape:"component.package"`
	SourceURL    string            `shape:"component.sourceURL"`
	Handler      string            `shape:"component.handler"`
	Settings     map[string]string `shape:"component.settings"`
	Dependencies []string          `shape:"component.dependencies"`
}

type IOShape struct {
	TypeName    string           `shape:"io.typeName"`
	Package     string           `shape:"io.package"`
	Cardinality string           `shape:"io.cardinality"`
	CaseFormat  string           `shape:"io.caseFormat"`
	Exclude     []string         `shape:"io.exclude"`
	Parameters  []ParameterShape `shape:"io.parameters"`
}

type ParameterShape struct {
	Name            string            `shape:"param.name"`
	Kind            string            `shape:"param.kind"`
	In              string            `shape:"param.in"`
	Required        *bool             `shape:"param.required"`
	DataType        string            `shape:"param.dataType"`
	Package         string            `shape:"param.package"`
	Cardinality     string            `shape:"param.cardinality"`
	Tag             string            `shape:"param.tag"`
	TagMeta         map[string]string `shape:"param.tagMeta"`
	CodecName       string            `shape:"param.codec.name"`
	CodecArgs       []string          `shape:"param.codec.args"`
	ErrorStatusCode int               `shape:"param.errorStatusCode"`
	Cacheable       *bool             `shape:"param.cacheable"`
	Scope           string            `shape:"param.scope"`
	Connector       string            `shape:"param.connector"`
	Limit           *int              `shape:"param.limit"`
	Value           string            `shape:"param.value"`
	Predicates      []PredicateShape  `shape:"param.predicates"`
	LocationInput   *LocationShape    `shape:"param.locationInput"`
}

type PredicateShape struct {
	Name   string   `shape:"predicate.name"`
	Group  int      `shape:"predicate.group"`
	Ensure bool     `shape:"predicate.ensure"`
	Args   []string `shape:"predicate.args"`
}

type LocationShape struct {
	Name       string           `shape:"location.name"`
	Package    string           `shape:"location.package"`
	Parameters []ParameterShape `shape:"location.parameters"`
}

type ViewGraphShape struct {
	Root  string      `shape:"views.root"`
	Views []ViewShape `shape:"views.items"`
}

type ViewShape struct {
	Name                   string            `shape:"view.name"`
	Mode                   string            `shape:"view.mode"`
	Table                  string            `shape:"view.table"`
	Module                 string            `shape:"view.module"`
	AllowNulls             *bool             `shape:"view.allowNulls"`
	Connector              string            `shape:"view.connector"`
	Partitioner            string            `shape:"view.partitioner"`
	PartitionedConcurrency int               `shape:"view.partitionedConcurrency"`
	RelationalConcurrency  int               `shape:"view.relationalConcurrency"`
	SourceURL              string            `shape:"view.sourceURL"`
	Selector               SelectorShape     `shape:"view.selector"`
	With                   []RelationShape   `shape:"view.with"`
	Columns                map[string]string `shape:"view.columns"`
}

type SelectorShape struct {
	Namespace  string `shape:"selector.namespace"`
	Limit      *int   `shape:"selector.limit"`
	Criteria   *bool  `shape:"selector.criteria"`
	Projection *bool  `shape:"selector.projection"`
	OrderBy    *bool  `shape:"selector.orderBy"`
	Offset     *bool  `shape:"selector.offset"`
}

type RelationShape struct {
	Name          string      `shape:"relation.name"`
	Holder        string      `shape:"relation.holder"`
	Cardinality   string      `shape:"relation.cardinality"`
	IncludeColumn *bool       `shape:"relation.includeColumn"`
	Ref           string      `shape:"relation.ref"`
	On            []JoinShape `shape:"relation.on"`
}

type JoinShape struct {
	Namespace string `shape:"join.namespace"`
	Column    string `shape:"join.column"`
	Field     string `shape:"join.field"`
}

type DependencyShape struct {
	With          []string `shape:"deps.with"`
	Connectors    []string `shape:"deps.connectors"`
	Constants     []string `shape:"deps.constants"`
	Substitutions []string `shape:"deps.substitutions"`
}
