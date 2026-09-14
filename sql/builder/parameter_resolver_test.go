package builder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/sqlx"
)

func parameterResolver(values map[string]any) sqlx.ParameterResolver {
	return func(name string) (any, bool, error) {
		value, ok := values[strings.ToLower(strings.TrimSpace(name))]
		return value, ok, nil
	}
}

func withTestParameterProjection(t *testing.T, inputType reflect.Type, aliases map[string]string) BuilderOption {
	t.Helper()
	for inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	byPath := map[string][]string{}
	for alias, path := range aliases {
		byPath[path] = append(byPath[path], alias)
	}
	fields := make([]bindly.ProjectionField, 0, len(byPath))
	for path, names := range byPath {
		fields = append(fields, bindly.ProjectionField{Path: path, Names: names})
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatalf("NewInjector() error = %v", err)
	}
	plan, err := injector.CompilePlan(inputType)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}
	projection, err := plan.Projection(fields...)
	if err != nil {
		t.Fatalf("Projection() error = %v", err)
	}
	return func(options *builderOptions) {
		options.parameterResolver = func(name string) (any, bool, error) {
			if !options.input.IsValid() {
				return nil, false, nil
			}
			return projection.Value(options.input.Interface(), name)
		}
	}
}

func TestParameterBindingInterleavesNamedAndPositionalValues(t *testing.T) {
	sqlText, args, err := bindParameters(
		"SELECT :Prefix, ?, :Suffix, ?",
		parameterResolver(map[string]any{"prefix": 3, "suffix": 9}),
		7, true,
	)
	if err != nil {
		t.Fatalf("bindParameters() error = %v", err)
	}
	if sqlText != "SELECT ?, ?, ?, ?" || !reflect.DeepEqual(args, []any{3, 7, 9, true}) {
		t.Fatalf("bindParameters() = %q, %#v", sqlText, args)
	}
}

func TestParameterBindingUsesSQLXParser(t *testing.T) {
	source := "SELECT ':ID' AS marker, data ? ?, data ?| array['a'] FROM events WHERE id = :ID -- :ID"
	actualSQL, actualArgs, err := bindParameters(source, parameterResolver(map[string]any{"id": 7}), "key")
	if err != nil {
		t.Fatalf("bindParameters() error = %v", err)
	}
	expectSQL := "SELECT ':ID' AS marker, data ? ?, data ?| array['a'] FROM events WHERE id = ? -- :ID"
	if actualSQL != expectSQL || !reflect.DeepEqual(actualArgs, []any{"key", 7}) {
		t.Fatalf("bindParameters() = %q %#v", actualSQL, actualArgs)
	}
}

func TestParameterBindingRejectsMissingNamedValue(t *testing.T) {
	if _, _, err := bindParameters("SELECT :Missing", nil); err == nil {
		t.Fatal("expected missing named value error")
	}
}
