package report

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/report/cubecompose"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"sort"
	"strings"
	"time"
)

type ComposeResponse struct {
	Columns []cubecompose.Column `json:"columns"`
	Data    any                  `json:"data"`
}

type composeHandler struct {
	inputType reflect.Type
	source    *Plan
	config    *spec.CubeComposeSettings
	catalog   *cubecompose.Catalog
	metadata  *metadata
	contract  *registry.RouteInputContract
}

func (h *composeHandler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(h.config.TimeoutMs)*time.Millisecond)
	defer cancel()
	input, err := reportInput(invocation.Input, h.inputType)
	if err != nil {
		return nil, err
	}
	cubes := input.FieldByName("Cubes")
	if cubes.Len() < 1 || cubes.Len() > h.config.MaxCubes {
		return nil, fmt.Errorf("cube compose requires between 1 and %d cubes", h.config.MaxCubes)
	}
	plan, err := cubecompose.Compile(input.FieldByName("SQL").String(), h.catalog, cubes.Len(), h.config.MaxLimit)
	if err != nil {
		return nil, err
	}
	frames, err := h.frames(cubes)
	if err != nil {
		return nil, err
	}
	value, _, err := invocation.Binder.Lookup(ctx, exec.ComponentInvokerKey)
	if err != nil {
		return nil, err
	}
	invoker, ok := value.(exec.ComponentInvoker)
	if !ok {
		return nil, fmt.Errorf("cube compose component invoker is unavailable")
	}
	prepared := make([]cubecompose.Frame, len(frames))
	var projection exec.ProjectionReader
	snapshot := time.Now().UTC()
	for i, frame := range frames {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		alignment := strings.ToLower(strings.TrimSpace(frame.FieldByName("Align").String()))
		if alignment != "" && alignment != "elapsed" {
			return nil, fmt.Errorf("cube %d align must be elapsed or empty", i+1)
		}
		frameCtx := cubecompose.WithFrameContext(ctx, cubecompose.FrameContext{Frame: i + 1, Alignment: alignment, Snapshot: snapshot})
		providers, err := h.providers(frame, plan.Fields[i])
		if err != nil {
			return nil, err
		}
		result, err := invoker.InvokeComponent(frameCtx, exec.ComponentRequest{Target: h.source.target, Providers: providers, PrepareQuery: true})
		if err != nil {
			return nil, fmt.Errorf("prepare cube %d: %w", i+1, err)
		}
		query, ok := result.(*exec.PreparedQuery)
		if !ok || query == nil || query.Projection == nil {
			return nil, fmt.Errorf("cube %d did not return a prepared SQL query", i+1)
		}
		prepared[i] = cubecompose.Frame{SQL: query.SQL, Args: query.Args}
		if projection == nil {
			projection = query.Projection
		}
	}
	SQL, args, err := plan.Render(prepared)
	if err != nil {
		return nil, err
	}
	rowType, err := h.rowType(plan)
	if err != nil {
		return nil, err
	}
	rows, err := projection.ReadProjection(ctx, exec.ProjectionRequest{SQL: SQL, Args: args, RowType: rowType})
	if err != nil {
		return nil, err
	}
	return &ComposeResponse{Columns: plan.Columns, Data: rows}, nil
}

func (h *composeHandler) rowType(plan *cubecompose.Plan) (reflect.Type, error) {
	fields := make([]xshape.RuntimeField, 0, len(plan.Columns))
	seen := map[string]string{}
	for _, column := range plan.Columns {
		name := typecatalog.ExportedFieldName(column.Name)
		if previous, ok := seen[name]; ok {
			return nil, fmt.Errorf("compose outputs %s and %s map to the same Go field %s", previous, column.Name, name)
		}
		seen[name] = column.Name
		typeOf := column.RuntimeType()
		if typeOf == nil {
			typeOf = reflect.TypeFor[any]()
		}
		fields = append(fields, xshape.RuntimeField{Name: name, Type: typeOf, Tag: reflect.StructTag(fmt.Sprintf(`json:"%s" sqlx:"%s"`, column.Name, column.Name))})
	}
	return (xshape.Runtime{}).Struct(fields)
}

func (h *composeHandler) frames(cubes reflect.Value) ([]reflect.Value, error) {
	result := make([]reflect.Value, cubes.Len())
	for i := range result {
		frame := reflect.New(cubes.Type().Elem()).Elem()
		frame.Set(cubes.Index(i))
		parent := frame.FieldByName("InheritFrom")
		if !parent.IsNil() {
			index := int(parent.Elem().Int())
			if index < 1 || index > i {
				return nil, fmt.Errorf("cube %d inheritFrom must reference a preceding cube", i+1)
			}
			filters := frame.FieldByName("Filters")
			previous := result[index-1].FieldByName("Filters")
			for field := 0; field < filters.NumField(); field++ {
				if filters.Field(field).IsNil() {
					filters.Field(field).Set(previous.Field(field))
				}
			}
		}
		result[i] = frame
	}
	return result, nil
}

func (h *composeHandler) providers(frame reflect.Value, fields []string) ([]locator.Provider, error) {
	selectors := xstate.Selectors{&xstate.NamedSelector{Name: h.source.view, Selector: xstate.Selector{Fields: fields}}}
	result := []locator.Provider{handlerprovider.Static(xhandler.SelectorsKey, selectors)}
	byKind := map[string]map[string]typedValue{}
	for _, field := range h.contract.Fields() {
		binding := field.Binding()
		parameter, ok := binding.Extension.(*spec.Parameter)
		if !ok || parameter == nil || parameter.QuerySelector == nil {
			continue
		}
		values := byKind[binding.Location.Kind]
		if values == nil {
			values = map[string]typedValue{}
			byKind[binding.Location.Kind] = values
		}
		values[binding.Location.In] = typedValue{typeOf: field.SourceType()}
	}
	filters := frame.FieldByName("Filters")
	for _, filter := range h.metadata.filters {
		binding := filter.contract.Binding()
		field, ok := filters.Type().FieldByName(filter.fieldName)
		if !ok {
			return nil, fmt.Errorf("compose filter field %s is missing", filter.fieldName)
		}
		value, present, err := filterSourceValue(filters, field.Index, filter.contract.SourceType())
		if err != nil {
			return nil, err
		}
		var supplied any
		if present {
			supplied = value.Interface()
		}
		// Found nil is an explicit absence at this higher provider layer. It
		// prevents ambient wrapper query values from supplying omitted filters.
		values := byKind[binding.Location.Kind]
		if values == nil {
			values = map[string]typedValue{}
			byKind[binding.Location.Kind] = values
		}
		values[binding.Location.In] = typedValue{typeOf: filter.contract.SourceType(), value: supplied}
	}
	kinds := make([]string, 0, len(byKind))
	for kind := range byKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		values := byKind[kind]
		result = append(result, handlerprovider.Named(kind, func(_ context.Context, target reflect.Type, name string) (any, bool, error) {
			value, ok := values[name]
			if !ok {
				return nil, false, nil
			}
			if target != value.typeOf {
				return nil, false, fmt.Errorf("compose filter %s type mismatch", name)
			}
			return value.value, true, nil
		}))
	}
	return result, nil
}
