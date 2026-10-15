package output

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type errorDataRow struct {
	ID int `json:"id"`
}
type errorDataRows []*errorDataRow
type errorDataOtherRows []*errorDataRow
type errorDataOutput struct {
	legacy.Status
	Rows    errorDataRows `json:"rows"`
	Other   []string      `json:"other"`
	Summary *int          `json:"summary"`
}
type errorDataPointer *errorDataOutput
type errorDataOrdinary struct {
	legacy.Status
	Rows  []*errorDataRow
	Other []string
}
type errorDataCustom struct {
	legacy.Status
	Rows errorDataRows
}

func (*errorDataCustom) MarshalJSON() ([]byte, error) { return []byte(`"custom"`), nil }

type errorDataCustomRows []*errorDataRow

func (errorDataCustomRows) MarshalJSON() ([]byte, error) { return []byte(`"custom-rows"`), nil }

type errorDataMarshaller struct{}

func (*errorDataMarshaller) Marshal(any) ([]byte, error) { return []byte(`"custom"`), nil }

type errorDataPromoted struct{ Rows errorDataRows }

type errorDataPromotedOther struct{ Rows errorDataRows }

func errorDataComponent(name, expression string) *spec.Component {
	return &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT 1 AS ID"}}, Settings: &spec.Settings{CaseFormat: "lc"}, Parameters: []*spec.Parameter{declaredErrorStatus("Status"), {Name: name, TypeExpr: expression, Source: spec.BindSource{Kind: "output", Name: "view"}}}}
}
func TestLegacyDeclaredErrorDataExactFreshSlot(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[errorDataOutput](), reflect.TypeFor[*errorDataOutput](), reflect.TypeFor[errorDataPointer]()} {
		t.Run(typ.String(), func(t *testing.T) {
			c := errorDataComponent("Rows", "errorDataRows")
			p, err := (Compiler{}).Compile(CompileInput{Component: c, Type: typ, DataField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			c.Parameters = nil
			c.RootView.Name = "mutated"
			for n := 0; n < 2; n++ {
				v, ok, err := p.ErrorEnvelope("safe")
				if err != nil || !ok || reflect.TypeOf(v) != typ {
					t.Fatalf("%T %v %v", v, ok, err)
				}
				rv := reflect.ValueOf(v)
				if rv.Kind() == reflect.Pointer {
					rv = rv.Elem()
				}
				rows := rv.FieldByName("Rows")
				if rows.Type() != reflect.TypeFor[errorDataRows]() || rows.IsNil() || rows.Len() != 0 || !rv.FieldByName("Other").IsNil() || !rv.FieldByName("Summary").IsNil() {
					t.Fatal(rv.Interface())
				}
				status := rv.FieldByName("Status").Interface().(legacy.Status)
				if status.Errors != nil || status.Warning != nil || status.Extras != nil {
					t.Fatal(status)
				}
			}
		})
	}
	c := errorDataComponent("Rows", "[]*errorDataRow")
	c.Parameters[1].EmitOutput = true
	c.Parameters[1].Source.Kind = "body"
	p, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[errorDataOrdinary]()})
	if err != nil {
		t.Fatal(err)
	}
	v, _, _ := p.ErrorEnvelope("safe")
	if v.(errorDataOrdinary).Rows == nil {
		t.Fatal("existing output direction lost")
	}
}
func TestLegacyDeclaredErrorDataConcurrentAndWirePolicy(t *testing.T) {
	c := errorDataComponent("Rows", "errorDataRows")
	p, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[*errorDataOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _, e := p.ErrorEnvelope("public")
			if e != nil {
				t.Error(e)
				return
			}
			out := v.(*errorDataOutput)
			if out.Rows == nil || len(out.Rows) != 0 {
				t.Error("not fresh")
			}
			out.Rows = append(out.Rows, &errorDataRow{ID: 7})
			out.Other = []string{"caller"}
		}()
	}
	wg.Wait()
	v, _, err := p.ErrorEnvelope("public")
	if err != nil {
		t.Fatal(err)
	}
	out := v.(*errorDataOutput)
	if out.Rows == nil || len(out.Rows) != 0 || out.Other != nil {
		t.Fatal(out)
	}
	encoded, err := p.Encode(context.Background(), "json", out)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err = json.Unmarshal(encoded.Data, &body); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(body["rows"], []any{}) || body["other"] != nil {
		t.Fatalf("single slot policy %s", encoded.Data)
	}
	success, err := p.Encode(context.Background(), "json", &errorDataOutput{})
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(success.Data, &body); err != nil {
		t.Fatal(err)
	}
	if body["rows"] != nil {
		t.Fatal("success nil slice changed")
	}
	c.Settings.Output = &spec.OutputSettings{NilSlicePolicy: "empty_array"}
	configured, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[*errorDataOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = configured.Encode(context.Background(), "json", &errorDataOutput{})
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded.Data, &body); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(body["rows"], []any{}) || !reflect.DeepEqual(body["other"], []any{}) {
		t.Fatalf("configured policy changed %s", encoded.Data)
	}
}
func TestLegacyDeclaredErrorDataFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    reflect.Type
		mutate func(*spec.Component)
		holder string
	}{
		{"missing-declaration", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters = c.Parameters[:1] }, "Rows"},
		{"ambiguous", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "output", Name: "view"}})
		}, "Rows"},
		{"absent-member", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].Name = "Missing" }, "Rows"},
		{"wrong-member-case", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].Name = "rows" }, "Rows"},
		{"datafield-mismatch", reflect.TypeFor[errorDataOutput](), nil, "Other"},
		{"type-mismatch", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "[]string" }, "Rows"},
		{"named-container-erasure", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "[]*errorDataRow" }, "Rows"},
		{"other-named-container", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "errorDataOtherRows" }, "Rows"},
		{"unknown-qualified", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "unknown.errorDataRows" }, "Rows"},
		{"output-type-mismatch", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].OutputTypeExpr = "[]string" }, "Rows"},
		{"nonslice", reflect.TypeFor[struct {
			legacy.Status
			Rows errorDataRow
		}](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "errorDataRow" }, "Rows"},
		{"array", reflect.TypeFor[struct {
			legacy.Status
			Rows [2]int
		}](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "[2]int" }, "Rows"},
		{"promoted", reflect.TypeFor[struct {
			legacy.Status
			errorDataPromoted
		}](), nil, "Rows"},
		{"ambiguous-promoted", reflect.TypeFor[struct {
			legacy.Status
			errorDataPromoted
			errorDataPromotedOther
		}](), nil, "Rows"},
		{"legacy-singleton", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.RootView.Cardinality = "One" }, "Rows"},
		{"unknown-cardinality", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.RootView.Cardinality = "unknown" }, "Rows"},
		{"body-not-view", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].Source.Name = "body" }, "Rows"},
		{"ordinary-input", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Parameters[1].Source.Kind = "body" }, "Rows"},
		{"no-executable-root", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.RootView.Source = nil }, "Rows"},
		{"in-memory", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.RootView.InMemory = true }, "Rows"},
		{"auxiliary", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.RootView.Auxiliary = true }, "Rows"},
		{"handler", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Routes = []*spec.Route{{Handler: "custom"}} }, "Rows"},
		{"mutation", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.Settings.Mutation = "patch" }, "Rows"},
		{"singleton", reflect.TypeFor[errorDataOutput](), func(c *spec.Component) { c.RootView.Cardinality = spec.CardinalityOne }, "Rows"},
		{"custom-root", reflect.TypeFor[errorDataCustom](), nil, "Rows"},
		{"byte-body", reflect.TypeFor[struct {
			legacy.Status
			Rows []byte
		}](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "[]byte" }, "Rows"},
		{"pointer-slice", reflect.TypeFor[struct {
			legacy.Status
			Rows *[]int
		}](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "*[]int" }, "Rows"},
		{"custom-slice", reflect.TypeFor[struct {
			legacy.Status
			Rows errorDataCustomRows
		}](), func(c *spec.Component) { c.Parameters[1].TypeExpr = "errorDataCustomRows" }, "Rows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := errorDataComponent("Rows", "errorDataRows")
			if tc.mutate != nil {
				tc.mutate(c)
			}
			p, err := (Compiler{}).Compile(CompileInput{Component: c, Type: tc.typ, DataField: tc.holder})
			if err != nil {
				t.Fatal("changed unrelated compile", err)
			}
			v, ok, err := p.ErrorEnvelope("public")
			if err != nil || !ok {
				t.Fatal(err, ok)
			}
			rv := reflect.ValueOf(v)
			if rv.Kind() == reflect.Pointer {
				rv = rv.Elem()
			}
			if field := rv.FieldByName("Rows"); field.IsValid() && !field.IsZero() {
				t.Fatal("non-authoritative data initialized")
			}
		})
	}
	c := errorDataComponent("Rows", "errorDataRows")
	c.Settings.JSONMarshalType = "Custom"
	p, err := (Compiler{Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[errorDataMarshaller](), nil }}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[errorDataOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	v, _, _ := p.ErrorEnvelope("public")
	if v.(errorDataOutput).Rows != nil {
		t.Fatal("configured custom changed")
	}
	modernComponent := errorDataComponent("Rows", "[]int")
	p, err = (Compiler{}).Compile(CompileInput{Component: modernComponent, Type: reflect.TypeFor[struct {
		modern.Status
		Rows []int
	}]()})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, err := p.ErrorEnvelope("public"); err != nil || ok || v != nil {
		t.Fatal("modern changed")
	}
}

func TestLegacyDeclaredErrorDataExistingMetadataAuthority(t *testing.T) {
	c := errorDataComponent("Rows", "record.errorDataRows")
	c.TypeContext = &spec.TypeContext{PackagePath: reflect.TypeFor[errorDataOutput]().PkgPath(), Imports: []spec.ImportSpec{{Alias: "record", Package: reflect.TypeFor[errorDataOutput]().PkgPath()}}}
	c.Parameters[1].Declaration = spec.DeclarationKindDefine
	c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Rows", Declaration: spec.DeclarationKindSet, TypeExpr: "[]string", Source: spec.BindSource{Kind: "output", Name: "view"}})
	p, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[errorDataOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	v, _, _ := p.ErrorEnvelope("public")
	if v.(errorDataOutput).Rows == nil {
		t.Fatal("effective qualified declaration lost")
	}
	builtin := errorDataComponent("Rows", "[]int")
	p, err = (Compiler{}).Compile(CompileInput{Component: builtin, Type: reflect.TypeFor[struct {
		legacy.Status
		Rows []int
	}]()})
	if err != nil {
		t.Fatal(err)
	}
	value, _, _ := p.ErrorEnvelope("public")
	if reflect.ValueOf(value).FieldByName("Rows").IsNil() {
		t.Fatal("builtin exact type lost")
	}
}
