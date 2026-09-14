package report

import (
	"reflect"
	"testing"

	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type reportSourceInput struct {
	AccountIDs []int
	Fields     []string
}

type reportSourceOutput struct {
	Rows []reportSourceRow
}

type reportSourceRow struct {
	AccountID  int
	TotalSpend float64
	Details    []string
}

func reportSource(t *testing.T, settings *spec.ReportSettings) Source {
	t.Helper()
	groupable := true
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/acme/reporting", Name: "Spend"},
		Name: "Spend", Description: "Spend report",
		Settings:    &spec.Settings{Report: settings, InputType: "reportSourceInput", OutputType: "reportSourceOutput"},
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/acme/reporting"},
		Routes:      []*spec.Route{{Method: "GET", Path: "/spend", Name: "Spend"}},
		Parameters: []*spec.Parameter{
			{Name: "AccountIDs", Source: spec.BindSource{Kind: "query", Name: "accountID"}, Predicates: []*spec.Predicate{{Name: "in", Args: []string{"spend", "account_id"}}}},
			{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "spend", Property: spec.SelectorPropertyFields}},
		},
		RootView: &spec.View{
			Key: spec.Key{Kind: spec.KindView, Scope: "example.com/acme/reporting", Name: "spend"}, Name: "spend", Groupable: &groupable,
			Source: &spec.ViewSource{SQL: "SELECT account_id, total_spend FROM spend"},
			Columns: []*spec.Column{
				{Name: "AccountID", Source: "account_id", Groupable: &groupable},
				{Name: "TotalSpend", Source: "total_spend"},
			},
			Relations: []*spec.Relation{{
				Name: "details", Holder: "Details",
				On: []*spec.RelationLink{{ParentColumn: "account_id", ChildColumn: "account_id"}},
			}},
		},
	}
	compiled, err := handlercompiler.New(handlercompiler.Input{
		Component: component, InputType: reflect.TypeOf(reportSourceInput{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile source input: %v", err)
	}
	return Source{Component: component, Input: compiled.Input, OutputType: reflect.TypeOf(reportSourceOutput{})}
}

func boolPointer(value bool) *bool { return &value }

func generatedProject(t *testing.T, settings *spec.ReportSettings) (*Project, *typecatalog.Catalog) {
	t.Helper()
	catalog := typecatalog.NewCatalog()
	project, err := NewProjectCompiler(ProjectConfig{Types: catalog}).Compile([]Source{reportSource(t, settings)})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return project, catalog
}
