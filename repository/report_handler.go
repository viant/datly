package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view/state"
	xhandler "github.com/viant/xdatly/handler"
	xdhttp "github.com/viant/xdatly/handler/http"
)

type reportHandler struct {
	Dispatcher contract.Dispatcher
	Path       *contract.Path
	Metadata   *ReportMetadata
	Original   *Component
	BodyType   reflect.Type
}

func (r *reportHandler) Exec(ctx context.Context, session xhandler.Session) (interface{}, error) {
	if r == nil || r.Dispatcher == nil || r.Path == nil || r.Metadata == nil || r.Original == nil {
		return nil, fmt.Errorf("report handler was not initialized")
	}
	request, err := session.Http().NewRequest(ctx)
	if err != nil {
		return nil, err
	}
	input, err := r.reportInput(ctx, request)
	if err != nil {
		return nil, err
	}
	query, err := r.buildQuery(input)
	if err != nil {
		return nil, err
	}
	internalReq := request.Clone(ctx)
	internalReq.Method = r.Path.Method
	internalReq.URL = cloneURL(request.URL)
	internalReq.URL.Path = strings.TrimSuffix(request.URL.Path, "/report")
	internalReq.URL.RawPath = internalReq.URL.Path
	internalReq.URL.RawQuery = query.Encode()
	internalReq.RequestURI = internalReq.URL.RequestURI()
	redirect := &xdhttp.Route{URL: r.Path.URI, Method: r.Path.Method}
	return nil, session.Http().Redirect(ctx, redirect, internalReq)
}

func (r *reportHandler) reportInput(ctx context.Context, request *http.Request) (interface{}, error) {
	input := ctx.Value(xhandler.InputKey)
	if request != nil && request.Body != nil && r.BodyType != nil {
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			targetType := r.BodyType
			for targetType.Kind() == reflect.Ptr {
				targetType = targetType.Elem()
			}
			target := reflect.New(targetType)
			if err := json.Unmarshal(payload, target.Interface()); err != nil {
				return nil, err
			}
			return target.Interface(), nil
		}
	}
	if input != nil {
		return input, nil
	}
	if input == nil {
		return nil, fmt.Errorf("report input was empty")
	}
	return input, nil
}

func (r *reportHandler) buildQuery(input interface{}) (url.Values, error) {
	root := indirectValue(reflect.ValueOf(input))
	if !root.IsValid() || root.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported report input type %T", input)
	}
	root = bodyRoot(root, r.Metadata.BodyFieldName)
	query := url.Values{}
	fields, err := r.collectSelections(root, r.Metadata.Dimensions, r.Metadata.Measures)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("report requires at least one dimension or measure")
	}
	if fieldsParameter := r.Original.View.Selector.FieldsParameter; fieldsParameter != nil && fieldsParameter.In != nil {
		query.Set(fieldsParameter.In.Name, strings.Join(fields, ","))
	}
	if err := r.collectFilters(root, query); err != nil {
		return nil, err
	}
	if err := r.collectStrings(root, r.Metadata.OrderBy, query, r.selectorName(r.Original.View.Selector.OrderByParameter, "_orderby")); err != nil {
		return nil, err
	}
	if err := r.collectInts(root, r.Metadata.Limit, query, r.selectorName(r.Original.View.Selector.LimitParameter, "_limit")); err != nil {
		return nil, err
	}
	if err := r.collectInts(root, r.Metadata.Offset, query, r.selectorName(r.Original.View.Selector.OffsetParameter, "_offset")); err != nil {
		return nil, err
	}
	return query, nil
}

func (r *reportHandler) selectorName(parameter *state.Parameter, fallback string) string {
	if parameter != nil && parameter.In != nil && strings.TrimSpace(parameter.In.Name) != "" {
		return parameter.In.Name
	}
	return fallback
}

func (r *reportHandler) collectSelections(root reflect.Value, groups ...[]*ReportField) ([]string, error) {
	var result []string
	for _, group := range groups {
		for _, field := range group {
			section := fieldByName(root, field.Section)
			if !section.IsValid() {
				continue
			}
			value := fieldByName(indirectValue(section), field.FieldName)
			if !value.IsValid() || value.Kind() != reflect.Bool {
				continue
			}
			if value.Bool() {
				result = append(result, field.Name)
			}
		}
	}
	return result, nil
}

func (r *reportHandler) collectFilters(root reflect.Value, query url.Values) error {
	filters := fieldByName(root, r.Metadata.FiltersKey)
	if !filters.IsValid() {
		return nil
	}
	filters = indirectValue(filters)
	for _, filter := range r.Metadata.Filters {
		value := fieldByName(filters, filter.FieldName)
		if !value.IsValid() || isEmptyValue(value) {
			continue
		}
		if filter.Parameter == nil || filter.Parameter.In == nil {
			continue
		}
		appendQueryValue(query, filter.Parameter.In.Name, value)
	}
	return nil
}

func (r *reportHandler) collectStrings(root reflect.Value, fieldName string, query url.Values, key string) error {
	if fieldName == "" {
		return nil
	}
	value := fieldByName(root, fieldName)
	if !value.IsValid() {
		return nil
	}
	value = indirectValue(value)
	if !value.IsValid() || value.Kind() != reflect.Slice {
		return nil
	}
	var parts []string
	for i := 0; i < value.Len(); i++ {
		item := indirectValue(value.Index(i))
		if item.IsValid() && item.Kind() == reflect.String && item.Len() > 0 {
			parts = append(parts, item.String())
		}
	}
	if len(parts) > 0 {
		query.Set(key, strings.Join(parts, ","))
	}
	return nil
}

func (r *reportHandler) collectInts(root reflect.Value, fieldName string, query url.Values, key string) error {
	if fieldName == "" {
		return nil
	}
	value := fieldByName(root, fieldName)
	if !value.IsValid() {
		return nil
	}
	value = indirectValue(value)
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		query.Set(key, strconv.FormatInt(value.Int(), 10))
	}
	return nil
}

func appendQueryValue(query url.Values, key string, value reflect.Value) {
	value = indirectValue(value)
	switch value.Kind() {
	case reflect.String:
		if value.String() != "" {
			query.Add(key, value.String())
		}
	case reflect.Bool:
		query.Add(key, strconv.FormatBool(value.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		query.Add(key, strconv.FormatInt(value.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		query.Add(key, strconv.FormatUint(value.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		query.Add(key, strconv.FormatFloat(value.Float(), 'f', -1, 64))
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			appendQueryValue(query, key, value.Index(i))
		}
	}
}

func fieldByName(root reflect.Value, name string) reflect.Value {
	root = indirectValue(root)
	if !root.IsValid() || root.Kind() != reflect.Struct || name == "" {
		return reflect.Value{}
	}
	return root.FieldByName(name)
}

func indirectValue(value reflect.Value) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func isEmptyValue(value reflect.Value) bool {
	value = indirectValue(value)
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
		return value.Len() == 0
	case reflect.Bool:
		return !value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return value.Float() == 0
	}
	return false
}

func cloneURL(source *url.URL) *url.URL {
	if source == nil {
		return &url.URL{}
	}
	clone := *source
	return &clone
}

func bodyRoot(root reflect.Value, bodyField string) reflect.Value {
	if bodyField == "" {
		return root
	}
	body := fieldByName(root, bodyField)
	if !body.IsValid() {
		return root
	}
	body = indirectValue(body)
	if !body.IsValid() || body.Kind() != reflect.Struct {
		return root
	}
	return body
}
