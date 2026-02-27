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
func (p *Planner) Plan(_ context.Context, scanned *shape.ScanResult, _ ...shape.PlanOption) (*shape.PlanResult, error) {
	if scanned == nil || scanned.Source == nil {
		return nil, shape.ErrNilSource
	}

	scanResult, ok := scanned.Descriptors.(*scan.Result)
	if !ok || scanResult == nil {
		return nil, fmt.Errorf("shape plan: unsupported descriptors type %T", scanned.Descriptors)
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

	for _, item := range scanResult.StateFields {
		result.States = append(result.States, normalizeState(item))
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
		}
		result.SQL = tag.SQL.SQL
		result.SQLURI = tag.SQL.URI
		result.Summary = tag.SummarySQL.SQL
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
	}
	if field.StateTag == nil || field.StateTag.Parameter == nil {
		result.Schema = state.NewSchema(field.Type)
		return result
	}

	pTag := field.StateTag.Parameter
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
	if dataType := strings.TrimSpace(pTag.DataType); dataType != "" {
		result.Schema.DataType = dataType
	}
	return result
}

func resolveStateType(item *State, fallback reflect.Type) reflect.Type {
	if item.In == nil {
		return fallback
	}
	key := strings.ToLower(strings.TrimSpace(firstNonEmpty(item.In.Name, item.Name)))
	switch strings.ToLower(strings.TrimSpace(string(item.In.Kind))) {
	case "output":
		if rType, ok := outputkeys.Types[key]; ok {
			return rType
		}
	case "meta":
		if rType, ok := metakeys.Types[key]; ok {
			return rType
		}
	case "async":
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
