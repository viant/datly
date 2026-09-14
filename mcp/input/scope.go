package input

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	bodyprovider "github.com/viant/bindly/provider/body"
	requestprovider "github.com/viant/bindly/provider/request"
)

// Source resolves one compiled MCP argument from a protocol payload. The
// concrete sources below cover tool argument objects and resource URIs.
type Source interface {
	resolve(Argument) (resolvedValue, bool, error)
	validate([]Argument) error
}

// Arguments is an MCP tool's public JSON argument object.
type Arguments map[string]interface{}

// URI is an already-decoded MCP resource URI.
type URI struct {
	Path  map[string]string
	Query url.Values
}

type resolvedValue struct {
	wire []string
	body json.RawMessage
}

// Scope projects either MCP source through one compiled mapping into Bindly's
// canonical request providers. Bindly remains the sole owner of request-source
// lookup and component input conversion.
func (p *Plan) Scope(source Source) (*requestprovider.Scope, error) {
	if p == nil {
		return nil, fmt.Errorf("MCP input plan is required")
	}
	if source == nil {
		return nil, fmt.Errorf("MCP input source is required")
	}
	if err := source.validate(p.arguments); err != nil {
		return nil, err
	}
	var pathValues map[string]string
	var queryValues url.Values
	var headers http.Header
	var cookies map[string]string
	var formValues url.Values
	var namedBody map[string]json.RawMessage
	var wholeBody json.RawMessage
	for _, argument := range p.arguments {
		resolved, found, err := source.resolve(argument)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		switch argument.Source.Kind {
		case requestprovider.BodyKind:
			if argument.Source.In == "" {
				wholeBody = append(json.RawMessage(nil), resolved.body...)
			} else {
				if namedBody == nil {
					namedBody = map[string]json.RawMessage{}
				}
				namedBody[argument.Source.In] = append(json.RawMessage(nil), resolved.body...)
			}
		case requestprovider.PathKind:
			value, err := scalarWire(argument, resolved.wire)
			if err != nil {
				return nil, err
			}
			if pathValues == nil {
				pathValues = map[string]string{}
			}
			pathValues[argument.Source.In] = value
		case requestprovider.CookieKind:
			value, err := scalarWire(argument, resolved.wire)
			if err != nil {
				return nil, err
			}
			if cookies == nil {
				cookies = map[string]string{}
			}
			cookies[argument.Source.In] = value
		case requestprovider.QueryKind:
			queryValues = appendValues(queryValues, argument.Source.In, resolved.wire)
		case requestprovider.HeaderKind:
			if headers == nil {
				headers = make(http.Header)
			}
			for _, value := range resolved.wire {
				headers.Add(argument.Source.In, value)
			}
		case requestprovider.FormKind:
			formValues = appendValues(formValues, argument.Source.In, resolved.wire)
		}
	}
	options := make([]requestprovider.Option, 0, 6)
	if pathValues != nil {
		options = append(options, requestprovider.WithPathParams(pathValues))
	}
	if queryValues != nil {
		options = append(options, requestprovider.WithQuery(queryValues))
	}
	if headers != nil {
		options = append(options, requestprovider.WithHeaders(headers))
	}
	if cookies != nil {
		options = append(options, requestprovider.WithCookies(cookies))
	}
	if formValues != nil {
		options = append(options, requestprovider.WithForm(formValues))
	}
	if wholeBody != nil || len(namedBody) != 0 {
		raw := wholeBody
		if len(namedBody) != 0 {
			var err error
			raw, err = json.Marshal(namedBody)
			if err != nil {
				return nil, fmt.Errorf("encode named MCP body arguments: %w", err)
			}
		}
		body, err := bodyprovider.New(raw, "application/json", nil, bodyprovider.WithExactFieldNames())
		if err != nil {
			return nil, err
		}
		options = append(options, requestprovider.WithBodySource(body))
	}
	return requestprovider.NewValues(options...), nil
}

func (a Arguments) resolve(argument Argument) (resolvedValue, bool, error) {
	raw, found := a[argument.PublicName]
	if !found {
		return resolvedValue{}, false, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return resolvedValue{}, false, fmt.Errorf("encode MCP argument %q: %w", argument.PublicName, err)
	}
	if argument.Source.Kind == requestprovider.BodyKind {
		return resolvedValue{body: encoded}, true, nil
	}
	value, err := decodeSourceValue(encoded, argument.SourceType)
	if err != nil {
		return resolvedValue{}, false, fmt.Errorf("decode MCP argument %q as %s: %w", argument.PublicName, argument.SourceType, err)
	}
	wire, err := wireValues(value)
	if err != nil {
		return resolvedValue{}, false, fmt.Errorf("encode MCP argument %q as request value: %w", argument.PublicName, err)
	}
	return resolvedValue{wire: wire}, true, nil
}

func (a Arguments) validate(arguments []Argument) error {
	known := make(map[string]bool, len(arguments))
	for _, argument := range arguments {
		known[argument.PublicName] = true
	}
	for name := range a {
		if !known[name] {
			return fmt.Errorf("unknown MCP argument %q", name)
		}
	}
	return nil
}

func (u URI) resolve(argument Argument) (resolvedValue, bool, error) {
	switch argument.Source.Kind {
	case requestprovider.PathKind:
		value, found := u.Path[argument.Source.In]
		return resolvedValue{wire: []string{value}}, found, nil
	case requestprovider.QueryKind:
		values := u.Query[argument.Source.In]
		if len(values) > 1 && !isCollection(argument.SourceType) {
			return resolvedValue{}, false, fmt.Errorf("MCP query argument %q is scalar and cannot be repeated", argument.PublicName)
		}
		return resolvedValue{wire: append([]string(nil), values...)}, len(values) != 0, nil
	default:
		return resolvedValue{}, false, fmt.Errorf("MCP URI argument %q has unsupported binding kind %q", argument.PublicName, argument.Source.Kind)
	}
}

func (u URI) validate(arguments []Argument) error {
	knownPath := map[string]bool{}
	knownQuery := map[string]bool{}
	for _, argument := range arguments {
		switch argument.Source.Kind {
		case requestprovider.PathKind:
			knownPath[argument.Source.In] = true
		case requestprovider.QueryKind:
			knownQuery[argument.Source.In] = true
		default:
			return fmt.Errorf("MCP URI argument %q has unsupported binding kind %q", argument.PublicName, argument.Source.Kind)
		}
	}
	for name := range u.Path {
		if !knownPath[name] {
			return fmt.Errorf("unknown MCP path value %q", name)
		}
	}
	for name := range u.Query {
		if !knownQuery[name] {
			return fmt.Errorf("unknown MCP query value %q", name)
		}
	}
	return nil
}

func decodeSourceValue(raw []byte, targetType reflect.Type) (interface{}, error) {
	if targetType == reflect.TypeOf(json.RawMessage{}) {
		return append(json.RawMessage(nil), raw...), nil
	}
	if targetType == reflect.TypeOf([]byte{}) {
		var value []byte
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		return value, nil
	}
	target := reflect.New(targetType)
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(target.Interface()); err != nil {
		return nil, err
	}
	return target.Elem().Interface(), nil
}

func wireValues(value interface{}) ([]string, error) {
	if value == nil {
		return []string{"null"}, nil
	}
	actual := reflect.ValueOf(value)
	if encoded, ok, err := textValue(actual); ok || err != nil {
		return []string{encoded}, err
	}
	for actual.Kind() == reflect.Ptr {
		if actual.IsNil() {
			return []string{"null"}, nil
		}
		actual = actual.Elem()
	}
	if encoded, ok, err := textValue(actual); ok || err != nil {
		return []string{encoded}, err
	}
	if actual.Kind() == reflect.Slice || actual.Kind() == reflect.Array {
		if actual.Type().Elem().Kind() == reflect.Uint8 {
			if actual.Type() == reflect.TypeOf(json.RawMessage{}) {
				return []string{string(actual.Bytes())}, nil
			}
			data := make([]byte, actual.Len())
			for index := range data {
				data[index] = byte(actual.Index(index).Uint())
			}
			return []string{base64.StdEncoding.EncodeToString(data)}, nil
		}
		result := make([]string, 0, actual.Len())
		for index := 0; index < actual.Len(); index++ {
			encoded, err := wireValues(actual.Index(index).Interface())
			if err != nil {
				return nil, err
			}
			result = append(result, encoded...)
		}
		return result, nil
	}
	switch actual.Kind() {
	case reflect.String:
		return []string{actual.String()}, nil
	case reflect.Bool:
		return []string{strconv.FormatBool(actual.Bool())}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []string{strconv.FormatInt(actual.Int(), 10)}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []string{strconv.FormatUint(actual.Uint(), 10)}, nil
	case reflect.Float32, reflect.Float64:
		return []string{strconv.FormatFloat(actual.Float(), 'g', -1, actual.Type().Bits())}, nil
	}
	encoded, err := json.Marshal(actual.Interface())
	if err != nil {
		return nil, err
	}
	return []string{string(encoded)}, nil
}

func textValue(value reflect.Value) (string, bool, error) {
	if !value.IsValid() || !value.CanInterface() {
		return "", false, nil
	}
	encoder, ok := value.Interface().(encoding.TextMarshaler)
	if !ok && value.Kind() != reflect.Ptr {
		addressable := reflect.New(value.Type())
		addressable.Elem().Set(value)
		encoder, ok = addressable.Interface().(encoding.TextMarshaler)
	}
	if !ok {
		return "", false, nil
	}
	encoded, err := encoder.MarshalText()
	if err != nil {
		return "", true, err
	}
	return string(encoded), true, nil
}

func scalarWire(argument Argument, values []string) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("MCP %s argument %q requires one value, got %d", argument.Source.Kind, argument.PublicName, len(values))
	}
	return values[0], nil
}

func appendValues(target url.Values, name string, values []string) url.Values {
	if target == nil {
		target = make(url.Values)
	}
	target[name] = append(target[name], values...)
	return target
}

func isCollection(typeOf reflect.Type) bool {
	for typeOf != nil && typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}
	return typeOf != nil && (typeOf.Kind() == reflect.Slice || typeOf.Kind() == reflect.Array)
}
