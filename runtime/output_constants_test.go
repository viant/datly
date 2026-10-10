package runtime

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
)

type constantOutputRow struct{ ID int }
type constantReaderOutput struct {
	Rows        []*constantOutputRow `parameter:"Rows,kind=output,in=view"`
	MergeConfig string               `parameter:"MergeConfig,kind=const,in=dbRuleMergeConfig,value=null"`
	Count       int                  `parameter:"Count,kind=const,in=count,value=7"`
	observed    bool
	seenConfig  string
	seenCount   int
}

func (o *constantReaderOutput) Finalize(_ context.Context, cause error) error {
	if cause == nil {
		o.observed = true
		o.seenConfig, o.seenCount = o.MergeConfig, o.Count
	}
	return nil
}

func TestReaderOutputConstantsBindBeforeFinalizeAndRejectRequestOverrides(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	c := componentSpec("Records", "GET", "/records", nil)
	c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
	text, count := "null", "7"
	c.Parameters = []*spec.Parameter{
		{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		{Name: "MergeConfig", TypeExpr: "string", Source: spec.BindSource{Kind: "const", Name: "dbRuleMergeConfig"}, Value: &text, EmitOutput: true},
		{Name: "Count", TypeExpr: "int", Source: spec.BindSource{Kind: "const", Name: "count"}, Value: &count, EmitOutput: true},
	}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[constantReaderOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[constantReaderOutput](), Plan: a.Reader, SQL: &rsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[constantReaderOutput](), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(ctx)
	actual, err := executeTestRoute(t, rt, ctx, testharness.NewRequest("GET", "/records?dbRuleMergeConfig=forged&count=99"))
	if err != nil {
		t.Fatal(err)
	}
	o := actual.(*constantReaderOutput)
	if o.MergeConfig != "null" || o.Count != 7 || !o.observed || o.seenConfig != "null" || o.seenCount != 7 || len(o.Rows) != 1 || o.Rows[0].ID != 1 {
		t.Fatalf("constant output contract: %+v", o)
	}
}

func TestOutputConstantTagDiscoveryRetainsServerSource(t *testing.T) {
	c := componentSpec("Records", "GET", "/records", nil)
	c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
	c.Parameters = []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[constantReaderOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, p := range a.Component.Parameters {
		if p.Name == "MergeConfig" || p.Name == "Count" {
			if p.Source.Kind != "const" || !p.EmitOutput || p.Value == nil {
				t.Fatalf("output constant source lost: %+v", p)
			}
			found[p.Name] = true
		}
	}
	if len(found) != 2 {
		t.Fatalf("missing output constants: %v", found)
	}
}

func TestDiscoveredOutputConstantsRetainExplicitEmptyAndZeroInstanceValues(t *testing.T) {
	c := componentSpec("Records", "GET", "/records", nil)
	c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
	c.Parameters = []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}
	values, err := constant.New(map[string]string{"dbRuleMergeConfig": "", "count": "0"})
	if err != nil {
		t.Fatal(err)
	}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Const: values, Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[constantReaderOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"MergeConfig": "", "Count": "0"}
	for _, p := range a.Component.Parameters {
		if value, ok := want[p.Name]; ok {
			if p.Value == nil || *p.Value != value {
				t.Fatalf("instance value lost for %s: %+v", p.Name, p.Value)
			}
			delete(want, p.Name)
		}
	}
	if len(want) != 0 || len(c.Parameters) != 1 {
		t.Fatal("output constants missing or authored component mutated")
	}
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[constantReaderOutput](), Plan: a.Reader, SQL: &rsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[constantReaderOutput](), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(ctx)
	actual, err := executeTestRoute(t, rt, ctx, testharness.NewRequest("GET", "/records?dbRuleMergeConfig=forged&count=99"))
	if err != nil {
		t.Fatal(err)
	}
	o := actual.(*constantReaderOutput)
	if o.MergeConfig != "" || o.Count != 0 || !o.observed || o.seenConfig != "" || o.seenCount != 0 || len(o.Rows) != 1 || o.Rows[0].ID != 1 {
		t.Fatalf("instance constants not applied before finalize: %+v", o)
	}
}

func TestInstanceOverrideCannotConcealDiscoveredOutputConstantConflict(t *testing.T) {
	c := componentSpec("Records", "GET", "/records", nil)
	c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
	c.Parameters = []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}
	c.Settings = &spec.Settings{Const: map[string]string{"dbRuleMergeConfig": "authored"}}
	values, err := constant.New(map[string]string{"dbRuleMergeConfig": "null"})
	if err != nil {
		t.Fatal(err)
	}
	for _, instance := range []*constant.Values{nil, values} {
		_, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Const: instance, Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[constantReaderOutput]()})
		if err == nil || !strings.Contains(err.Error(), "conflicting defaults") {
			t.Fatalf("authored conflict concealed: %v", err)
		}
	}
	if c.Settings.Const["dbRuleMergeConfig"] != "authored" {
		t.Fatal("authored constant mutated")
	}
}
