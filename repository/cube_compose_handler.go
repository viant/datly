package repository

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/cubecompose"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	xhandler "github.com/viant/xdatly/handler"
)

type cubeComposeHandler struct {
	Dispatcher contract.Dispatcher
	Path       *contract.Path
	Metadata   *ReportMetadata
	Original   *Component
	BodyType   reflect.Type
	Config     *CubeCompose
}

type cubeComposeResponse struct {
	Columns []cubecompose.Column `json:"columns"`
	Data    any                  `json:"data"`
}

func (h *cubeComposeHandler) Exec(ctx context.Context, session xhandler.Session) (interface{}, error) {
	if h == nil || h.Dispatcher == nil || h.Path == nil || h.Metadata == nil || h.Original == nil || h.Config == nil {
		return nil, fmt.Errorf("cube compose handler was not initialized")
	}
	timeout := time.Duration(h.Config.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	preparer, ok := h.Dispatcher.(contract.QueryPreparer)
	if !ok {
		return nil, fmt.Errorf("Datly dispatcher does not support cube SQL preparation")
	}
	request, err := session.Http().NewRequest(execCtx)
	if err != nil {
		return nil, err
	}
	input, err := readReportBody(execCtx, request, h.BodyType)
	if err != nil {
		return nil, err
	}
	root := indirectValue(reflect.ValueOf(input))
	if !root.IsValid() || root.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid cube compose input %T", input)
	}
	sqlValue := fieldByName(root, "SQL")
	if !sqlValue.IsValid() || sqlValue.Kind() != reflect.String {
		return nil, fmt.Errorf("cube compose SQL was empty")
	}
	catalog, err := h.catalog()
	if err != nil {
		return nil, err
	}
	cubes := indirectValue(fieldByName(root, "Cubes"))
	if !cubes.IsValid() || cubes.Kind() != reflect.Slice || cubes.Len() == 0 {
		return nil, fmt.Errorf("cube compose requires at least one cube")
	}
	if cubes.Len() > h.Config.MaxCubes {
		return nil, fmt.Errorf("cube compose accepts at most %d cubes", h.Config.MaxCubes)
	}
	plan, err := cubecompose.Compile(sqlValue.String(), catalog, cubes.Len(), h.Config.MaxLimit)
	if err != nil {
		return nil, err
	}
	frames, err := resolveComposeFrames(cubes)
	if err != nil {
		return nil, err
	}
	snapshot := time.Now().UTC()
	prepared := make([]cubecompose.Frame, len(frames))
	for i, frame := range frames {
		frameCtx, err := composeFrameContext(execCtx, frame, i+1, snapshot)
		if err != nil {
			return nil, err
		}
		frameRequest, err := h.frameRequest(request, frame, plan.Fields[i])
		if err != nil {
			return nil, err
		}
		query, err := preparer.PrepareQuery(frameCtx, h.Path, frameRequest)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare cube %d: %w", i+1, err)
		}
		prepared[i] = cubecompose.Frame{SQL: query.SQL, Args: query.Args}
	}
	finalSQL, args, err := plan.Render(prepared)
	if err != nil {
		return nil, err
	}
	rows, err := h.readRows(execCtx, plan, finalSQL, args)
	if err != nil {
		return nil, err
	}
	return &cubeComposeResponse{Columns: plan.Columns, Data: rows}, nil
}

func (h *cubeComposeHandler) readRows(ctx context.Context, plan *cubecompose.Plan, SQL string, args []interface{}) (interface{}, error) {
	rowType, err := plan.RowType()
	if err != nil {
		return nil, err
	}
	rowsType := reflect.SliceOf(reflect.PtrTo(rowType))
	schema := state.NewSchema(rowsType)
	schema.Cardinality = state.Many
	projection, err := view.New(
		h.Original.View.Name+"#cubeComposeProjection",
		"",
		view.WithSQL(SQL),
		view.WithConnector(h.Original.View.Connector),
		view.WithSchema(schema),
	)
	if err != nil {
		return nil, err
	}
	projection.Description = "Request-local Cube Compose projection"
	if err = projection.Init(ctx, h.Original.View.GetResource()); err != nil {
		return nil, err
	}
	rows := reflect.New(rowsType)
	if err = reader.New().ReadInto(ctx, rows.Interface(), projection,
		reader.WithQuery(SQL, args...),
		reader.WithCacheDisabled(true),
	); err != nil {
		return nil, err
	}
	return rows.Elem().Interface(), nil
}

func composeFrameContext(ctx context.Context, frame reflect.Value, index int, snapshot time.Time) (context.Context, error) {
	alignment := ""
	if value := fieldByName(indirectValue(frame), "Align"); value.IsValid() && value.Kind() == reflect.String {
		alignment = strings.ToLower(strings.TrimSpace(value.String()))
	}
	if alignment != "" && alignment != contract.CubeComposeAlignmentElapsed {
		return nil, fmt.Errorf("cube%d align must be %q or empty", index, contract.CubeComposeAlignmentElapsed)
	}
	return contract.WithCubeComposeFrameContext(ctx, contract.CubeComposeFrameContext{
		Frame:     index,
		Alignment: alignment,
		Snapshot:  snapshot,
	}), nil
}

func readReportBody(ctx context.Context, request *http.Request, bodyType reflect.Type) (interface{}, error) {
	handler := &cubeHandler{BodyType: bodyType}
	return handler.reportInput(ctx, request)
}

func (h *cubeComposeHandler) catalog() (*cubecompose.Catalog, error) {
	fields := make([]cubecompose.Field, 0, len(h.Metadata.Dimensions)+len(h.Metadata.Measures))
	appendField := func(field *ReportField, role cubecompose.Role) error {
		column, ok := h.Original.View.ColumnByName(field.Name)
		if !ok {
			return fmt.Errorf("cube compose metadata field %q has no view column", field.Name)
		}
		fields = append(fields, cubecompose.Field{Name: column.Name, Type: column.ColumnType(), Role: role})
		return nil
	}
	for _, field := range h.Metadata.Dimensions {
		if err := appendField(field, cubecompose.Dimension); err != nil {
			return nil, err
		}
	}
	for _, field := range h.Metadata.Measures {
		if err := appendField(field, cubecompose.Measure); err != nil {
			return nil, err
		}
	}
	return cubecompose.NewCatalog(fields...)
}

func (h *cubeComposeHandler) frameRequest(source *http.Request, frame reflect.Value, fields []string) (*http.Request, error) {
	request := source.Clone(source.Context())
	request.Method = h.Path.Method
	request.URL = cloneURL(source.URL)
	request.URL.Path = h.Path.URI
	request.URL.RawPath = h.Path.URI
	request.Body = http.NoBody
	query := cloneValues(source.URL.Query())
	for _, key := range []string{view.OrderByQuery, view.LimitQuery, view.OffsetQuery, view.PageQuery, view.CriteriaQuery} {
		query.Del(key)
	}
	selector := h.Original.View.Selector
	if selector != nil {
		for _, parameter := range []*state.Parameter{selector.FieldsParameter, selector.OrderByParameter, selector.LimitParameter, selector.OffsetParameter, selector.PageParameter, selector.CriteriaParameter} {
			if parameter != nil && parameter.In != nil {
				query.Del(parameter.In.Name)
			}
		}
	}
	// A compose request owns each frame's filter values. Do not let query
	// parameters on the wrapper endpoint leak into either generated cube SQL.
	for _, filter := range h.Metadata.Filters {
		if filter != nil && filter.Parameter != nil && filter.Parameter.In != nil {
			query.Del(filter.Parameter.In.Name)
		}
	}
	if selector != nil && selector.FieldsParameter != nil && selector.FieldsParameter.In != nil {
		query.Set(selector.FieldsParameter.In.Name, strings.Join(fields, ","))
	} else {
		query.Set("_fields", strings.Join(fields, ","))
	}
	helper := &cubeHandler{Metadata: h.Metadata, Original: h.Original}
	if err := helper.collectFilters(indirectValue(frame), query); err != nil {
		return nil, err
	}
	request.URL.RawQuery = query.Encode()
	request.RequestURI = request.URL.RequestURI()
	return request, nil
}

func resolveComposeFrames(cubes reflect.Value) ([]reflect.Value, error) {
	result := make([]reflect.Value, cubes.Len())
	for i := 0; i < cubes.Len(); i++ {
		frame := indirectValue(cubes.Index(i))
		if !frame.IsValid() || frame.Kind() != reflect.Struct {
			return nil, fmt.Errorf("cube compose cube %d was invalid", i+1)
		}
		inheritFrom := fieldByName(frame, "InheritFrom")
		if !inheritFrom.IsValid() || inheritFrom.Kind() != reflect.Ptr || inheritFrom.IsNil() {
			result[i] = frame
			continue
		}
		parent := int(inheritFrom.Elem().Int())
		if parent < 1 || parent > i {
			return nil, fmt.Errorf("cube %d inheritFrom must reference a preceding cube between 1 and %d", i+1, i)
		}
		result[i] = inheritComposeFrame(result[parent-1], frame)
	}
	return result, nil
}

func inheritComposeFrame(parent, frame reflect.Value) reflect.Value {
	parent = indirectValue(parent)
	frame = indirectValue(frame)
	result := reflect.New(frame.Type()).Elem()
	result.Set(frame)
	left := fieldByName(parent, "Filters")
	right := fieldByName(result, "Filters")
	left = indirectValue(left)
	if !left.IsValid() || !right.IsValid() {
		return result
	}
	if right.Kind() == reflect.Ptr {
		if right.IsNil() {
			right.Set(reflect.New(right.Type().Elem()))
		}
		right = right.Elem()
	}
	for i := 0; i < right.NumField() && i < left.NumField(); i++ {
		if right.Field(i).Kind() == reflect.Ptr && right.Field(i).IsNil() && !left.Field(i).IsNil() {
			right.Field(i).Set(left.Field(i))
		}
	}
	return result
}

func cloneValues(source url.Values) url.Values {
	result := url.Values{}
	for key, values := range source {
		result[key] = append([]string{}, values...)
	}
	return result
}
