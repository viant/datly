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
	preparer, ok := h.Dispatcher.(contract.QueryPreparer)
	if !ok {
		return nil, fmt.Errorf("Datly dispatcher does not support cube SQL preparation")
	}
	request, err := session.Http().NewRequest(ctx)
	if err != nil {
		return nil, err
	}
	input, err := readReportBody(ctx, request, h.BodyType)
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
	plan, err := cubecompose.Compile(sqlValue.String(), catalog, h.Config.MaxLimit)
	if err != nil {
		return nil, err
	}
	cube1 := fieldByName(root, "Cube1")
	cube2 := fieldByName(root, "Cube2")
	if !cube1.IsValid() || !cube2.IsValid() {
		return nil, fmt.Errorf("cube compose requires cube1 and cube2")
	}
	cube2 = inheritComposeFrame(cube1, cube2)
	snapshot := time.Now().UTC()
	frame1Context, err := composeFrameContext(ctx, cube1, 1, snapshot)
	if err != nil {
		return nil, err
	}
	frame2Context, err := composeFrameContext(ctx, cube2, 2, snapshot)
	if err != nil {
		return nil, err
	}
	frame1Request, err := h.frameRequest(request, cube1, plan.Fields1)
	if err != nil {
		return nil, err
	}
	frame2Request, err := h.frameRequest(request, cube2, plan.Fields2)
	if err != nil {
		return nil, err
	}
	frame1, err := preparer.PrepareQuery(frame1Context, h.Path, frame1Request)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare cube1: %w", err)
	}
	frame2, err := preparer.PrepareQuery(frame2Context, h.Path, frame2Request)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare cube2: %w", err)
	}
	finalSQL, args, err := plan.Render(frame1.SQL, frame2.SQL, frame1.Args, frame2.Args)
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(h.Config.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
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

func inheritComposeFrame(cube1, cube2 reflect.Value) reflect.Value {
	cube1 = indirectValue(cube1)
	cube2 = indirectValue(cube2)
	inherit := fieldByName(cube2, "Inherit")
	if !inherit.IsValid() || inherit.Kind() != reflect.Bool || !inherit.Bool() {
		return cube2
	}
	result := reflect.New(cube2.Type()).Elem()
	result.Set(cube2)
	left := fieldByName(cube1, "Filters")
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
