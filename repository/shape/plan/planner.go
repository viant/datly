package plan

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/locator/async/keys"
	metakeys "github.com/viant/datly/repository/locator/meta/keys"
	outputkeys "github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/scan"
	"github.com/viant/datly/view/state"
)

// Planner normalizes scan descriptors into shape plan.
type Planner struct{}

// New returns shape planner implementation.
func New() *Planner {
	return &Planner{}
}

// Plan implements shape.Planner.
func (p *Planner) Plan(ctx context.Context, scanned *shape.ScanResult, _ ...shape.PlanOption) (*shape.PlanResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if scanned == nil || scanned.Source == nil {
		return nil, shape.ErrNilSource
	}

	scanResult, ok := scan.DescriptorsFrom(scanned)
	if !ok {
		return nil, fmt.Errorf("shape plan: unsupported descriptors kind %q", scanned.Descriptors.ShapeSpecKind())
	}

	result := &Result{
		RootType:    scanResult.RootType,
		EmbedFS:     scanResult.EmbedFS,
		ByPath:      map[string]*Field{},
		ViewsByName: map[string]*View{},
	}

	for _, item := range scanResult.Fields {
		field := &Field{
			Path:  item.Path,
			Name:  item.Name,
			Type:  item.Type,
			Index: append([]int(nil), item.Index...),
		}
		result.Fields = append(result.Fields, field)
		result.ByPath[field.Path] = field
	}

	for _, item := range scanResult.ViewFields {
		v := normalizeView(item)
		result.Views = append(result.Views, v)
		if v.Name != "" {
			result.ViewsByName[v.Name] = v
		}
	}
	assignNestedRelationParents(result.Views)

	for _, item := range scanResult.StateFields {
		result.States = append(result.States, normalizeState(item))
	}

	for _, item := range scanResult.ComponentFields {
		result.Components = append(result.Components, normalizeComponent(item))
	}

	return &shape.PlanResult{Source: scanned.Source, Plan: result}, nil
}

func normalizeView(field *scan.Field) *View {
	result := &View{
		Path:      field.Path,
		Holder:    field.Name,
		FieldType: field.Type,
	}

	if tag := field.ViewTag; tag != nil {
		if tag.View != nil {
			result.Name = tag.View.Name
			result.Table = tag.View.Table
			result.Connector = tag.View.Connector
			result.CacheRef = tag.View.Cache
			result.Partitioner = tag.View.PartitionerType
			result.PartitionedConcurrency = tag.View.PartitionedConcurrency
			result.RelationalConcurrency = tag.View.RelationalConcurrency
			result.Groupable = tag.View.Groupable
			result.SelectorNamespace = strings.TrimSpace(tag.View.SelectorNamespace)
			result.SelectorLimit = tag.View.Limit
			if tag.View.Limit != nil {
				noLimit := *tag.View.Limit == 0
				result.SelectorNoLimit = &noLimit
			}
			result.SelectorCriteria = tag.View.SelectorCriteria
			result.SelectorProjection = tag.View.SelectorProjection
			result.SelectorOrderBy = tag.View.SelectorOrderBy
			result.SelectorOffset = tag.View.SelectorOffset
			result.SelectorPage = tag.View.SelectorPage
			if len(tag.View.SelectorFilterable) > 0 {
				result.SelectorFilterable = append([]string(nil), tag.View.SelectorFilterable...)
			}
			if len(tag.View.SelectorOrderByColumns) > 0 {
				result.SelectorOrderByColumns = map[string]string{}
				for key, value := range tag.View.SelectorOrderByColumns {
					result.SelectorOrderByColumns[key] = value
				}
			}
			if strings.TrimSpace(tag.View.CustomTag) != "" || strings.TrimSpace(field.ViewTypeName) != "" || strings.TrimSpace(field.ViewDest) != "" {
				result.Declaration = &ViewDeclaration{
					Tag:      strings.TrimSpace(tag.View.CustomTag),
					TypeName: strings.TrimSpace(field.ViewTypeName),
					Dest:     strings.TrimSpace(field.ViewDest),
				}
			}
		}
		result.SQL = tag.SQL.SQL
		result.SQLURI = tag.SQL.URI
		result.Summary = tag.SummarySQL.SQL
		if tag.View != nil && strings.TrimSpace(tag.View.SummaryURI) != "" {
			result.SummaryURL = strings.TrimSpace(tag.View.SummaryURI)
		} else {
			result.SummaryURL = tag.SummarySQL.URI
		}
		if len(tag.LinkOn) > 0 {
			result.Relations = append(result.Relations, relationFromTagLinks(field.Name, tag.LinkOn))
		}
		result.Ref = strings.TrimSpace(tag.TypeName)
	}

	if result.Name == "" {
		result.Name = field.Name
	}

	elem, cardinality := componentType(field.Type)
	result.Cardinality = cardinality
	result.ElementType = elem
	return result
}

func relationFromTagLinks(holder string, links []string) *Relation {
	relation := &Relation{
		Name:   strings.TrimSpace(holder),
		Holder: strings.TrimSpace(holder),
		Ref:    strings.TrimSpace(holder),
	}
	for _, linkExpr := range links {
		linkExpr = strings.TrimSpace(linkExpr)
		if linkExpr == "" {
			continue
		}
		left, right, ok := strings.Cut(linkExpr, "=")
		if !ok {
			continue
		}
		leftField, leftNS, leftCol := splitTagSelector(left)
		rightField, rightNS, rightCol := splitTagSelector(right)
		if leftCol == "" || rightCol == "" {
			continue
		}
		relation.On = append(relation.On, &RelationLink{
			ParentField:     leftField,
			ParentNamespace: leftNS,
			ParentColumn:    leftCol,
			RefField:        rightField,
			RefNamespace:    rightNS,
			RefColumn:       rightCol,
			Expression:      strings.TrimSpace(left) + "=" + strings.TrimSpace(right),
		})
	}
	if relation.Ref == "" {
		relation.Ref = "relation"
	}
	if relation.Holder == "" {
		relation.Holder = relation.Ref
	}
	if relation.Name == "" {
		relation.Name = relation.Holder
	}
	return relation
}

func splitTagSelector(value string) (string, string, string) {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, "(true)")
	value = strings.TrimSuffix(value, "(false)")
	field := ""
	if idx := strings.Index(value, ":"); idx >= 0 {
		field = strings.TrimSpace(value[:idx])
		value = value[idx+1:]
	}
	value = strings.Trim(value, "`\"")
	if value == "" {
		return field, "", ""
	}
	if idx := strings.Index(value, "."); idx >= 0 {
		return field, strings.TrimSpace(value[:idx]), strings.TrimSpace(value[idx+1:])
	}
	return field, "", strings.TrimSpace(value)
}

func normalizeState(field *scan.Field) *State {
	result := &State{
		Parameter: state.Parameter{
			Name: field.Name,
			In:   &state.Location{},
		},
		QuerySelector: strings.TrimSpace(field.QuerySelector),
	}
	if field.StateTag == nil {
		result.Schema = state.NewSchema(field.Type)
		return result
	}

	pTag := field.StateTag.Parameter
	if pTag == nil {
		result.Schema = state.NewSchema(field.Type)
		return result
	}
	result.Name = firstNonEmpty(pTag.Name, field.Name)
	result.In = &state.Location{
		Kind: state.Kind(strings.ToLower(strings.TrimSpace(pTag.Kind))),
		Name: strings.TrimSpace(pTag.In),
	}
	result.When = pTag.When
	result.Scope = pTag.Scope
	result.Required = pTag.Required
	result.Async = pTag.Async
	result.Cacheable = pTag.Cacheable
	result.With = pTag.With
	result.URI = pTag.URI
	result.ErrorStatusCode = pTag.ErrorCode
	result.ErrorMessage = pTag.ErrorMessage
	result.Schema = state.NewSchema(resolveStateType(result, field.Type))
	if typeName := strings.TrimSpace(field.StateTag.TypeName); typeName != "" {
		applyStateTypeName(result.Schema, typeName)
	}
	state.BuildCodec(field.StateTag, &result.Parameter)
	state.BuildHandler(field.StateTag, &result.Parameter)
	if value, err := field.StateTag.GetValue(result.Schema.Type()); err == nil && value != nil {
		result.Value = normalizeStateValue(value)
	}
	if dataType := strings.TrimSpace(pTag.DataType); dataType != "" {
		result.Schema.DataType = dataType
	}
	return result
}

func normalizeStateValue(value interface{}) interface{} {
	switch actual := value.(type) {
	case *string:
		if actual == nil {
			return nil
		}
		return *actual
	}
	return value
}

func applyStateTypeName(schema *state.Schema, typeName string) {
	if schema == nil {
		return
	}
	typeName = strings.TrimSpace(strings.TrimPrefix(typeName, "*"))
	if typeName == "" {
		return
	}
	if idx := strings.LastIndex(typeName, "."); idx != -1 {
		schema.Package = strings.TrimSpace(typeName[:idx])
		schema.PackagePath = schema.Package
		schema.Name = strings.TrimSpace(typeName[idx+1:])
		return
	}
	schema.Name = typeName
}

func normalizeComponent(field *scan.Field) *ComponentRoute {
	result := &ComponentRoute{
		Path:       field.Path,
		FieldName:  field.Name,
		Type:       field.Type,
		InputType:  field.ComponentInputType,
		OutputType: field.ComponentOutputType,
		InputName:  field.ComponentInputName,
		OutputName: field.ComponentOutputName,
		Name:       field.Name,
	}
	if field.ComponentTag != nil && field.ComponentTag.Component != nil {
		tag := field.ComponentTag.Component
		if strings.TrimSpace(tag.Name) != "" {
			result.Name = strings.TrimSpace(tag.Name)
		}
		result.RoutePath = strings.TrimSpace(tag.Path)
		result.Method = strings.TrimSpace(tag.Method)
		result.Connector = strings.TrimSpace(tag.Connector)
		result.Marshaller = strings.TrimSpace(tag.Marshaller)
		result.Handler = strings.TrimSpace(tag.Handler)
		result.ViewName = strings.TrimSpace(tag.View)
		result.SourceURL = strings.TrimSpace(tag.Source)
		result.SummaryURL = strings.TrimSpace(tag.Summary)
	}
	return result
}

func assignNestedRelationParents(views []*View) {
	if len(views) == 0 {
		return
	}
	byPath := map[string]*View{}
	for _, item := range views {
		if item == nil || strings.TrimSpace(item.Path) == "" {
			continue
		}
		byPath[strings.TrimSpace(item.Path)] = item
	}
	for _, item := range views {
		if item == nil || len(item.Relations) == 0 {
			continue
		}
		parentPath := parentViewPath(item.Path)
		if parentPath == "" {
			continue
		}
		parent := byPath[parentPath]
		if parent == nil || strings.TrimSpace(parent.Name) == "" {
			continue
		}
		for _, rel := range item.Relations {
			if rel == nil || strings.TrimSpace(rel.Parent) != "" {
				continue
			}
			rel.Parent = parent.Name
		}
	}
}

func parentViewPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	index := strings.LastIndex(path, ".")
	if index == -1 {
		return ""
	}
	return strings.TrimSpace(path[:index])
}

func resolveStateType(item *State, fallback reflect.Type) reflect.Type {
	if item.In == nil {
		return fallback
	}
	key := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.In.Name, item.Name)))
	switch item.In.Kind {
	case state.KindOutput:
		if rType, ok := outputkeys.Types[key]; ok {
			return rType
		}
	case state.KindMeta:
		if rType, ok := metakeys.Types[key]; ok {
			return rType
		}
	case state.KindAsync:
		if rType, ok := keys.Types[key]; ok {
			return rType
		}
	}
	return fallback
}

func componentType(rType reflect.Type) (reflect.Type, string) {
	if rType == nil {
		return nil, "one"
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() == reflect.Slice {
		elem := rType.Elem()
		for elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		return elem, "many"
	}
	return rType, "one"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
