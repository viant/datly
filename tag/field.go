package tag

import (
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
)

const (
	SQLName         = "sql"
	RelationName    = "on"
	SourceName      = "source"
	SelectorAlias   = "selectorAlias"
	GroupableName   = "groupable"
	DescriptionName = "desc"
	ExampleName     = "example"
)

type SQL struct {
	Text string
	URI  string
}

// Field is the parsed Datly metadata for one Go field. Generic binding and
// SQLX metadata remain represented by their owning packages.
type Field struct {
	Component     *Component
	View          *View
	Self          *SelfReference
	Codec         *Codec
	SQL           *SQL
	Binding       *bindly.BindingSpec
	Relation      []*RelationLink
	Predicates    []*spec.Predicate
	QuerySelector *QuerySelector
	Source        string
	SelectorAlias string
	Description   string
	Example       string
	Groupable     bool
	MCP           *bool
	PathMCP       *bool
	Invariant     string
}

func ParseField(field reflect.StructField) (*Field, error) {
	result := &Field{
		Source:        strings.TrimSpace(field.Tag.Get(SourceName)),
		SelectorAlias: strings.TrimSpace(field.Tag.Get(SelectorAlias)),
		Description:   strings.TrimSpace(field.Tag.Get(DescriptionName)),
		Example:       strings.TrimSpace(field.Tag.Get(ExampleName)),
	}
	var err error
	if value, ok := field.Tag.Lookup(InvariantName); ok {
		result.Invariant, err = ParseInvariant(value)
		if err != nil {
			return nil, err
		}
	}
	for name, destination := range map[string]**bool{"mcpEnabled": &result.MCP, "pathMcpEnabled": &result.PathMCP} {
		if value, ok := field.Tag.Lookup(name); ok {
			parsed, parseErr := parseBool(name, value)
			if parseErr != nil {
				return nil, parseErr
			}
			*destination = &parsed
		}
	}
	if value, ok := field.Tag.Lookup(RelationName); ok {
		result.Relation, err = ParseRelation(value)
		if err != nil {
			return nil, err
		}
	}
	component, ok, err := ParseComponent(field.Tag)
	if err != nil {
		return nil, err
	}
	if ok {
		result.Component = &component
	}
	if value, ok := field.Tag.Lookup(ViewName); ok {
		result.View, err = ParseView(value)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := field.Tag.Lookup(SelfName); ok {
		result.Self, err = ParseSelf(value)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := field.Tag.Lookup(CodecName); ok {
		result.Codec, err = ParseCodec(value)
		if err != nil {
			return nil, err
		}
	}
	result.Predicates, err = ParsePredicates(field.Tag)
	if err != nil {
		return nil, err
	}
	if value, ok := field.Tag.Lookup(QuerySelectorName); ok {
		result.QuerySelector, err = ParseQuerySelector(value)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := field.Tag.Lookup(SQLName); ok {
		result.SQL = ParseSQL(value)
	}
	if value, ok := field.Tag.Lookup(GroupableName); ok && strings.TrimSpace(value) != "" {
		result.Groupable, err = parseBool(GroupableName, strings.TrimSpace(value))
		if err != nil {
			return nil, err
		}
	}
	binding, ok, err := bindly.BindingSpecFromField(field)
	if err != nil {
		return nil, err
	}
	if ok {
		result.Binding = &binding
	}
	return result, nil
}

func ParseSQL(value string) *SQL {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if key, uri, ok := strings.Cut(value, "="); ok && strings.EqualFold(strings.TrimSpace(key), "uri") {
		return &SQL{URI: strings.TrimSpace(uri)}
	}
	return &SQL{Text: value}
}
