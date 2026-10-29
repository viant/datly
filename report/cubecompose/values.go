package cubecompose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/govalidator"
	"github.com/viant/tagly/tags"
	xshape "github.com/viant/x/shape"
)

// ValuesContract is an authored, flat Go scalar shape. Its JSON field names
// identify placeholders of the form $Values.name; no value is SQL text.
type ValuesContract struct {
	typ      reflect.Type
	fields   map[string]reflect.StructField
	checks   *govalidator.Checks
	wireType reflect.Type
}

func NewValuesContract(typ reflect.Type) (*ValuesContract, error) {
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("cube compose values require a struct")
	}
	var wireFields []xshape.RuntimeField
	result := &ValuesContract{typ: typ, fields: map[string]reflect.StructField{}, checks: nil}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" || f.Anonymous || f.Tag.Get("internal") == "true" || f.Tag.Get("mcp") == "-" || f.Tag.Get("setMarker") == "true" {
			return nil, fmt.Errorf("cube compose value %s must be an exported scalar", f.Name)
		}
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "" {
			name = f.Name
		}
		if !safeIdentifier.MatchString(name) {
			return nil, fmt.Errorf("cube compose value %s requires a safe JSON name", f.Name)
		}
		t := f.Type
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		switch t.Kind() {
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		default:
			return nil, fmt.Errorf("cube compose value %s must be scalar", f.Name)
		}
		if _, ok := result.fields[name]; ok {
			return nil, fmt.Errorf("duplicate cube compose value %q", name)
		}
		result.fields[name] = f
		wireTag, err := tags.Parse(string(f.Tag))
		if err != nil {
			return nil, err
		}
		jsonTag := f.Tag.Get("json")
		if jsonTag == "" {
			jsonTag = name
		}
		jsonTag += ",omitempty"
		wireTag.Set("json", jsonTag)
		wireTag.Set("required", "false")
		wireFields = append(wireFields, xshape.RuntimeField{Name: f.Name, TypeExpr: t.Kind().String(), Tag: reflect.StructTag(wireTag.Literal())})
	}
	wire, err := (xshape.Runtime{}).Struct(wireFields)
	if err != nil {
		return nil, err
	}
	result.wireType = wire
	checks, err := govalidator.NewChecks(typ)
	if err != nil {
		return nil, err
	}
	if err = checks.ValidateAvailability(nil, nil); err != nil {
		return nil, err
	}
	result.checks = checks
	return result, nil
}

func (c *ValuesContract) WireType() reflect.Type { return c.wireType }

func (c *ValuesContract) Type() reflect.Type {
	if c == nil {
		return nil
	}
	return c.typ
}

// CompileWithValues admits only named scalar references in otherwise validated
// expressions. Existing Compile callers continue rejecting all placeholders.
func CompileWithValues(SQL string, catalog *Catalog, frameCount, maxLimit int, resultLimit *int, values *ValuesContract) (*Plan, error) {
	return compileResult(SQL, catalog, frameCount, maxLimit, resultLimit, values)
}

func (p *Plan) placeholder(raw string) (string, error) {
	name := strings.TrimPrefix(raw, "$Values.")
	if name == raw || p.valuesContract == nil {
		return "", fmt.Errorf("caller placeholders are not allowed")
	}
	if _, ok := p.valuesContract.fields[name]; !ok {
		return "", fmt.Errorf("undeclared cube compose value %q", name)
	}
	return name, nil
}

// BindValues returns a request-local plan. It never mutates the compiled plan.
// Referenced values must be present and non-null; declared unused values may be
// omitted. Explicit zero/false/empty string remain valid unless authored
// validation tags reject them. Standard JSON decoding preserves integer widths.
func (p *Plan) BindValues(ctx context.Context, raw json.RawMessage) (*Plan, error) {
	if p.valuesContract == nil {
		if len(bytes.TrimSpace(raw)) > 0 {
			return nil, fmt.Errorf("cube compose values are not declared")
		}
		return p, nil
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, fmt.Errorf("cube compose values: %w", err)
	}
	if object == nil {
		return nil, fmt.Errorf("cube compose values must be an object")
	}
	keys := make([]string, 0, len(object))
	for name := range object {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		if _, ok := p.valuesContract.fields[name]; !ok {
			return nil, fmt.Errorf("undeclared cube compose value %q", name)
		}
	}
	refs := make([]string, 0, len(p.references))
	for name := range p.references {
		refs = append(refs, name)
	}
	sort.Strings(refs)
	for _, name := range refs {
		v, ok := object[name]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return nil, fmt.Errorf("cube compose value %q is required and cannot be null", name)
		}
	}
	value := reflect.New(p.valuesContract.typ)
	// Exact key admission precedes canonical JSON decoding, retaining authored
	// scalar tags and integer widths without case-fold aliases or float maps.
	for _, name := range keys {
		if bytes.Equal(bytes.TrimSpace(object[name]), []byte("null")) {
			return nil, fmt.Errorf("cube compose value %q cannot be null", name)
		}
	}
	if err := json.Unmarshal(raw, value.Interface()); err != nil {
		return nil, fmt.Errorf("cube compose values: %w", err)
	}
	prepared, err := p.valuesContract.checks.Prepare(govalidator.WithShallow(true), govalidator.WithFieldFilter(func(path string) bool {
		for name, f := range p.valuesContract.fields {
			if f.Name == path {
				_, ok := object[name]
				return ok
			}
		}
		return false
	}))
	if err != nil {
		return nil, err
	}
	validation, err := prepared.Validate(ctx, value.Interface())
	if err != nil {
		return nil, err
	}
	if validation.Failed {
		return nil, validation
	}
	result := *p
	result.values = map[string]interface{}{}
	for _, name := range refs {
		v := value.Elem().FieldByIndex(p.valuesContract.fields[name].Index)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		result.values[name] = v.Interface()
	}
	return &result, nil
}
