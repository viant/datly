package structql

import (
	"context"
	"fmt"
	"io/fs"
	"reflect"
	"strings"

	"github.com/viant/sqlparser"
	query "github.com/viant/structql"
	xcodec "github.com/viant/xdatly/codec"
)

const Name = "structql"

// Factory compiles the one built-in StructQL codec from explicit source and
// destination contract types.
type Factory struct{}

func (Factory) New(config *xcodec.Config, options ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil {
		return nil, fmt.Errorf("StructQL codec config is required")
	}
	if config.Body != Name {
		return nil, fmt.Errorf("unsupported built-in codec %q", config.Body)
	}
	if len(config.Args) != 1 || strings.TrimSpace(config.Args[0]) == "" {
		return nil, fmt.Errorf("StructQL codec requires one query argument")
	}
	if config.SourceType == nil || config.DestinationType == nil {
		return nil, fmt.Errorf("StructQL source and destination types are required")
	}
	if err := rejectInterfaceType("source", config.SourceType); err != nil {
		return nil, err
	}
	if err := rejectInterfaceType("destination", config.DestinationType); err != nil {
		return nil, err
	}
	sql := strings.TrimSpace(config.Args[0])
	if key, reference, ok := strings.Cut(sql, "="); ok && strings.EqualFold(key, "uri") {
		resources := xcodec.NewOptions(options).ResourceFS
		if resources == nil {
			return nil, fmt.Errorf("StructQL resource %q requires a resource filesystem", reference)
		}
		content, err := fs.ReadFile(resources, strings.TrimSpace(reference))
		if err != nil {
			return nil, fmt.Errorf("read StructQL resource %q: %w", reference, err)
		}
		sql = strings.TrimSpace(string(content))
	}
	if strings.HasPrefix(sql, "?") || strings.HasPrefix(sql, "!") {
		sql = strings.TrimSpace(sql[1:])
	}
	parsed, err := sqlparser.ParseQuery(sql)
	if err != nil {
		return nil, fmt.Errorf("parse StructQL query: %w", err)
	}
	if parsed == nil || parsed.From.X == nil || len(parsed.List) == 0 {
		return nil, fmt.Errorf("StructQL requires a SELECT query with a source")
	}
	compiled, err := query.NewQuery(sql, config.SourceType, config.DestinationType)
	if err != nil {
		return nil, fmt.Errorf("compile StructQL codec: %w", err)
	}
	return &instance{query: compiled, resultType: config.DestinationType}, nil
}

type instance struct {
	query      *query.Query
	resultType reflect.Type
}

func (i *instance) Value(_ context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	if i == nil || i.query == nil || i.resultType == nil {
		return nil, fmt.Errorf("StructQL codec is not initialized")
	}
	var result interface{}
	var err error
	if i.resultType.Kind() == reflect.Slice {
		result, err = i.query.Select(raw)
	} else {
		result, err = i.query.First(raw)
	}
	if err != nil || result == nil {
		return result, err
	}
	value := reflect.ValueOf(result)
	if value.Type().AssignableTo(i.resultType) {
		return value.Interface(), nil
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if !value.Type().AssignableTo(i.resultType) {
		return nil, fmt.Errorf("StructQL result type %s does not match %s", value.Type(), i.resultType)
	}
	return value.Interface(), nil
}

func rejectInterfaceType(role string, root reflect.Type) error {
	if path, ok := interfacePath(root, role, map[reflect.Type]bool{}); ok {
		return fmt.Errorf("StructQL %s type %s contains unsupported interface at %s", role, root, path)
	}
	return nil
}

func interfacePath(current reflect.Type, path string, visiting map[reflect.Type]bool) (string, bool) {
	if current == nil {
		return "", false
	}
	if current.Kind() == reflect.Interface {
		return path, true
	}
	if visiting[current] {
		return "", false
	}
	visiting[current] = true
	defer delete(visiting, current)

	switch current.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array:
		return interfacePath(current.Elem(), path+"[]", visiting)
	case reflect.Map:
		if found, ok := interfacePath(current.Key(), path+".key", visiting); ok {
			return found, true
		}
		return interfacePath(current.Elem(), path+".value", visiting)
	case reflect.Struct:
		for i := 0; i < current.NumField(); i++ {
			field := current.Field(i)
			if found, ok := interfacePath(field.Type, path+"."+field.Name, visiting); ok {
				return found, true
			}
		}
	}
	return "", false
}
