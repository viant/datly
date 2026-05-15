package repository

import (
	"context"
	"encoding"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/viant/structology/encoding/json"
	tagformat "github.com/viant/tagly/format"
	ftime "github.com/viant/tagly/format/time"

	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	xhandler "github.com/viant/xdatly/handler"
	xdhttp "github.com/viant/xdatly/handler/http"
)

var (
	textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	stringerType      = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	timeType          = reflect.TypeOf(time.Time{})
)

type cubeHandler struct {
	Dispatcher contract.Dispatcher
	Path       *contract.Path
	Metadata   *ReportMetadata
	Original   *Component
	BodyType   reflect.Type
}

func (r *cubeHandler) Exec(ctx context.Context, session xhandler.Session) (interface{}, error) {
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
	query, err := r.buildQuery(input, request)
	if err != nil {
		return nil, err
	}
	internalReq := request.Clone(ctx)
	internalReq.Method = r.Path.Method
	internalReq.URL = cloneURL(request.URL)
	internalReq.Form = query
	internalReq.URL.Path = strings.TrimSuffix(request.URL.Path, "/cube")
	internalReq.URL.RawPath = internalReq.URL.Path
	internalReq.URL.RawQuery = query.Encode()
	internalReq.RequestURI = internalReq.URL.RequestURI()
	redirect := &xdhttp.Route{URL: r.Path.URI, Method: r.Path.Method}
	return nil, session.Http().Redirect(ctx, redirect, internalReq)
}

func (r *cubeHandler) reportInput(ctx context.Context, request *http.Request) (interface{}, error) {
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

func (r *cubeHandler) buildQuery(input interface{}, request *http.Request) (url.Values, error) {
	root := indirectValue(reflect.ValueOf(input))
	if !root.IsValid() || root.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported report input type %T", input)
	}

	_ = request.ParseForm()

	root = bodyRoot(root, r.Metadata.BodyFieldName)
	query := url.Values{}
	for k, v := range request.Form {
		query[k] = v
	}
	dimensions, err := r.collectSelections(root, r.Metadata.Dimensions)
	if err != nil {
		return nil, err
	}
	measures, err := r.collectSelections(root, r.Metadata.Measures)
	if err != nil {
		return nil, err
	}
	fields := append(append([]string{}, dimensions...), measures...)
	fields = appendAutoIncludedRelationHolders(r.Original.View, fields, dimensions)
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

func appendAutoIncludedRelationHolders(rootView *view.View, fields, dimensions []string) []string {
	if rootView == nil || len(rootView.With) == 0 || len(dimensions) == 0 {
		return fields
	}
	selected := make(map[string]bool, len(dimensions))
	for _, dimension := range dimensions {
		selected[normalizeRelationSelectionKey(dimension)] = true
	}

	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		seen[field] = true
	}

	for _, relation := range rootView.With {
		if relation == nil || strings.TrimSpace(relation.Holder) == "" {
			continue
		}
		if seen[relation.Holder] {
			continue
		}
		if relationMatchesSelectedDimensions(relation, selected) {
			fields = append(fields, relation.Holder)
			seen[relation.Holder] = true
		}
	}
	return fields
}

func relationMatchesSelectedDimensions(relation *view.Relation, selected map[string]bool) bool {
	if relation == nil {
		return false
	}
	for _, link := range relation.On {
		if link == nil {
			continue
		}
		if link.Field != "" && selected[normalizeRelationSelectionKey(link.Field)] {
			return true
		}
		if link.Column != "" && selected[normalizeRelationSelectionKey(link.Column)] {
			return true
		}
	}
	return false
}

func normalizeRelationSelectionKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	replacer := strings.NewReplacer("_", "", ".", "", "-", "", " ", "")
	return replacer.Replace(value)
}

func (r *cubeHandler) selectorName(parameter *state.Parameter, fallback string) string {
	if parameter != nil && parameter.In != nil && strings.TrimSpace(parameter.In.Name) != "" {
		return parameter.In.Name
	}
	return fallback
}

func (r *cubeHandler) collectSelections(root reflect.Value, groups ...[]*ReportField) ([]string, error) {
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

func (r *cubeHandler) collectFilters(root reflect.Value, query url.Values) error {
	filters := fieldByName(root, r.Metadata.FiltersKey)
	if !filters.IsValid() {
		return nil
	}
	filters = indirectValue(filters)
	for _, filter := range r.Metadata.Filters {
		value, field, ok := fieldByNameDetails(filters, filter.FieldName)
		if !ok || shouldOmitFilterValue(value) {
			continue
		}
		if filter.Parameter == nil || filter.Parameter.In == nil {
			continue
		}
		formatTag, err := resolveFilterFormatTag(filter, field)
		if err != nil {
			return err
		}
		if err := appendQueryValue(query, filter.Parameter.In.Name, value, filter, formatTag); err != nil {
			return err
		}
	}
	return nil
}

func (r *cubeHandler) collectStrings(root reflect.Value, fieldName string, query url.Values, key string) error {
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

func (r *cubeHandler) collectInts(root reflect.Value, fieldName string, query url.Values, key string) error {
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

func appendQueryValue(query url.Values, key string, value reflect.Value, filter *ReportFilter, formatTag *tagformat.Tag) error {
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil
		}
		return appendQueryValue(query, key, value.Elem(), filter, formatTag)
	}
	if value.Kind() == reflect.Struct && value.Type() == timeType {
		aTime := value.Interface().(time.Time)
		if aTime.IsZero() {
			return nil
		}
		query.Add(key, normalizeTimeFormatTag(formatTag).FormatTime(&aTime))
		return nil
	}

	if text, ok, err := marshalTextQueryValue(value); ok || err != nil {
		if err != nil {
			return wrapUnsupportedFilterValue(filter, key, value, err)
		}
		query.Add(key, text)
		return nil
	}
	if text, ok := stringifyQueryValue(value); ok {
		query.Add(key, text)
		return nil
	}

	switch value.Kind() {
	case reflect.String:
		if value.String() != "" {
			query.Add(key, value.String())
		}
		return nil
	case reflect.Bool:
		query.Add(key, strconv.FormatBool(value.Bool()))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		query.Add(key, strconv.FormatInt(value.Int(), 10))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		query.Add(key, strconv.FormatUint(value.Uint(), 10))
		return nil
	case reflect.Float32, reflect.Float64:
		query.Add(key, strconv.FormatFloat(value.Float(), 'f', -1, 64))
		return nil
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := appendQueryValue(query, key, value.Index(i), filter, formatTag); err != nil {
				return err
			}
		}
		return nil
	}
	return wrapUnsupportedFilterValue(filter, key, value, nil)
}

func fieldByName(root reflect.Value, name string) reflect.Value {
	root = indirectValue(root)
	if !root.IsValid() || root.Kind() != reflect.Struct || name == "" {
		return reflect.Value{}
	}
	return root.FieldByName(name)
}

func fieldByNameDetails(root reflect.Value, name string) (reflect.Value, reflect.StructField, bool) {
	root = indirectValue(root)
	if !root.IsValid() || root.Kind() != reflect.Struct || name == "" {
		return reflect.Value{}, reflect.StructField{}, false
	}
	field, ok := root.Type().FieldByName(name)
	if !ok {
		return reflect.Value{}, reflect.StructField{}, false
	}
	return root.FieldByName(name), field, true
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

func shouldOmitFilterValue(value reflect.Value) bool {
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return true
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return true
	}
	if value.Kind() == reflect.Ptr {
		return value.IsNil()
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
	case reflect.Struct:
		if value.Type() == timeType {
			return value.Interface().(time.Time).IsZero()
		}
		return value.IsZero()
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

func resolveFilterFormatTag(filter *ReportFilter, field reflect.StructField) (*tagformat.Tag, error) {
	if field.Tag != "" {
		parsed, err := tagformat.Parse(field.Tag)
		if err != nil {
			return nil, fmt.Errorf("invalid format tag on report filter field %s: %w", field.Name, err)
		}
		if hasTimeFormat(parsed) {
			return normalizeTimeFormatTag(parsed), nil
		}
	}
	if filter == nil || filter.Parameter == nil {
		return normalizeTimeFormatTag(nil), nil
	}
	if strings.TrimSpace(filter.Parameter.Tag) != "" {
		parsed, err := tagformat.Parse(reflect.StructTag(filter.Parameter.Tag))
		if err != nil {
			return nil, fmt.Errorf("invalid format tag on report filter parameter %s: %w", filter.FieldName, err)
		}
		if hasTimeFormat(parsed) {
			return normalizeTimeFormatTag(parsed), nil
		}
	}
	if dateFormat := strings.TrimSpace(filter.Parameter.DateFormat); dateFormat != "" {
		return &tagformat.Tag{
			DateFormat: dateFormat,
			TimeLayout: ftime.DateFormatToTimeLayout(dateFormat),
		}, nil
	}
	return normalizeTimeFormatTag(nil), nil
}

func hasTimeFormat(tag *tagformat.Tag) bool {
	if tag == nil {
		return false
	}
	return strings.TrimSpace(tag.DateFormat) != "" || strings.TrimSpace(tag.TimeLayout) != "" || strings.TrimSpace(tag.Timezone) != ""
}

func normalizeTimeFormatTag(tag *tagformat.Tag) *tagformat.Tag {
	if tag == nil {
		return &tagformat.Tag{}
	}
	if tag.TimeLayout == "" && tag.DateFormat != "" {
		tag.TimeLayout = ftime.DateFormatToTimeLayout(tag.DateFormat)
	}
	return tag
}

func marshalTextQueryValue(value reflect.Value) (string, bool, error) {
	if !value.IsValid() {
		return "", false, nil
	}
	if value.Type().Implements(textMarshalerType) && value.CanInterface() {
		text, err := value.Interface().(encoding.TextMarshaler).MarshalText()
		return string(text), true, err
	}
	if value.CanAddr() && value.Addr().Type().Implements(textMarshalerType) {
		text, err := value.Addr().Interface().(encoding.TextMarshaler).MarshalText()
		return string(text), true, err
	}
	if value.Type().Kind() != reflect.Ptr {
		ptr := reflect.New(value.Type())
		ptr.Elem().Set(value)
		if ptr.Type().Implements(textMarshalerType) {
			text, err := ptr.Interface().(encoding.TextMarshaler).MarshalText()
			return string(text), true, err
		}
	}
	return "", false, nil
}

func stringifyQueryValue(value reflect.Value) (string, bool) {
	if !value.IsValid() {
		return "", false
	}
	if value.Type().Implements(stringerType) && value.CanInterface() {
		return value.Interface().(fmt.Stringer).String(), true
	}
	if value.CanAddr() && value.Addr().Type().Implements(stringerType) {
		return value.Addr().Interface().(fmt.Stringer).String(), true
	}
	if value.Type().Kind() != reflect.Ptr {
		ptr := reflect.New(value.Type())
		ptr.Elem().Set(value)
		if ptr.Type().Implements(stringerType) {
			return ptr.Interface().(fmt.Stringer).String(), true
		}
	}
	return "", false
}

func wrapUnsupportedFilterValue(filter *ReportFilter, key string, value reflect.Value, cause error) error {
	filterName := ""
	if filter != nil {
		filterName = filter.FieldName
	}
	if filterName == "" {
		filterName = key
	}
	err := fmt.Errorf("unable to serialize report filter %q as query param %q: unsupported value type %s (kind=%s)", filterName, key, value.Type().String(), value.Kind())
	if cause != nil {
		return fmt.Errorf("%w: %v", err, cause)
	}
	return err
}
