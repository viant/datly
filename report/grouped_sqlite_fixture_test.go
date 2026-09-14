package report

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type groupedReportHarness struct {
	runtime  *druntime.Runtime
	service  *mcp.Service
	artifact *ComponentArtifact
	codec    *groupedCSVCodecFactory
	sql      *dsql.SQLComponent
}

func newGroupedReportHarness(t *testing.T, linked bool, configure ...func(*spec.ReportSettings)) *groupedReportHarness {
	t.Helper()
	return (groupedReportHarnessConfig{linked: linked, report: configure}).build(t)
}

type groupedReportHarnessConfig struct {
	linked  bool
	skipMCP bool
	report  []func(*spec.ReportSettings)
	route   func(*spec.Route)
	source  func(*spec.Component, *typecatalog.Catalog)
}

func (c groupedReportHarnessConfig) build(t *testing.T) *groupedReportHarness {
	t.Helper()
	ctx := context.Background()
	database := testharness.NewSQLiteHarness(t)
	if err := database.ExecStatements(ctx,
		`CREATE TABLE report_spend (tenant TEXT, account_id INTEGER, region TEXT, channel TEXT, status TEXT, amount REAL);`,
		`CREATE TABLE report_detail (tenant TEXT, account_id INTEGER, label TEXT);`,
		`INSERT INTO report_spend VALUES
            ('acme', 1, 'EU', 'web', 'active', 100),
            ('acme', 1, 'EU', 'web', 'active', 50),
            ('acme', 1, 'US', 'web', 'active', 1000),
            ('acme', 1, 'EU', 'store', 'active', 2000),
            ('acme', 1, 'EU', 'web', 'inactive', 3000),
            ('acme', 2, 'EU', 'web', 'active', 70),
            ('acme', 2, 'EU', 'store', 'active', 30),
            ('acme', 3, 'US', 'web', 'inactive', 20),
            ('other', 1, 'EU', 'web', 'active', 4000);`,
		`INSERT INTO report_detail VALUES
            ('acme', 1, 'alpha'), ('acme', 1, 'beta'),
            ('acme', 2, 'gamma'), ('acme', 3, 'delta'), ('other', 1, 'foreign');`,
	); err != nil {
		t.Fatalf("prepare grouped report database: %v", err)
	}

	types := typecatalog.NewCatalog()
	settings := &spec.ReportSettings{Enabled: true}
	for _, option := range c.report {
		option(settings)
	}
	if c.linked {
		settings.LinkedInputType = "GroupedLinkedInput"
		if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{
			Type: reflect.TypeOf(groupedLinkedInput{}), PkgPath: "example.com/acme/reporting", Name: "GroupedLinkedInput",
		}); err != nil {
			t.Fatal(err)
		}
	}
	codec := &groupedCSVCodecFactory{}
	component := groupedReportComponent(settings)
	if c.source != nil {
		c.source(component, types)
	}
	if c.route != nil {
		for _, route := range component.Routes {
			c.route(route)
		}
	}
	compilation, err := NewProjectCompiler(ProjectConfig{Types: types}).CompileArtifacts([]bootstrap.ArtifactInput{{
		Component: component, InputType: reflect.TypeOf(groupedSpendInput{}),
		OutputType: reflect.TypeOf(groupedSpendOutput{}), DirectViewField: "Rows", CodecFactory: codec,
	}})
	if err != nil {
		t.Fatalf("compile grouped report project: %v", err)
	}
	sourceSQL := &dsql.SQLComponent{DB: database.DB}
	registered, err := compilation.RuntimeComponents(ctx, RuntimeConfigureFunc(func(_ context.Context, artifact *ComponentArtifact) (RuntimeCapabilities, error) {
		if artifact.IsReport() {
			return RuntimeCapabilities{}, nil
		}
		compiled := artifact.ReaderCompilation()
		if compiled == nil {
			return RuntimeCapabilities{}, fmt.Errorf("source reader compilation is unavailable")
		}
		reader, buildErr := compiled.NewExecution(bootstrap.ReaderRuntimeConfig{SQL: sourceSQL})
		return RuntimeCapabilities{Reader: reader}, buildErr
	}))
	if err != nil {
		t.Fatalf("configure grouped report runtime: %v", err)
	}
	runtime, err := druntime.NewRuntime(registered)
	if err != nil {
		t.Fatal(err)
	}
	var service *mcp.Service
	if !c.skipMCP {
		service, err = mcp.New(mcp.Config{Components: registered, Invoker: runtime})
		if err != nil {
			t.Fatal(err)
		}
	}
	var reportArtifact *ComponentArtifact
	for _, artifact := range compilation.Artifacts() {
		if artifact.IsReport() {
			reportArtifact = artifact
			break
		}
	}
	if reportArtifact == nil {
		t.Fatal("grouped report artifact was not derived")
	}
	return &groupedReportHarness{runtime: runtime, service: service, artifact: reportArtifact, codec: codec, sql: sourceSQL}
}

func groupedReportComponent(settings *spec.ReportSettings) *spec.Component {
	groupable := true
	return &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/acme/reporting", Name: "Spend"},
		Name: "Spend", Description: "Grouped spend report",
		Settings:    &spec.Settings{Report: settings, InputType: "GroupedSpendInput", OutputType: "GroupedSpendOutput"},
		TypeContext: &spec.TypeContext{DefaultPackage: "example.com/acme/reporting"},
		Routes:      []*spec.Route{{Method: "GET", Path: "/spend/{tenant}", Name: "Spend"}},
		Parameters: []*spec.Parameter{
			{Name: "AccountIDs", Source: spec.BindSource{Kind: "query", Name: "accountIDs"}, TypeExpr: "string", OutputTypeExpr: "[]int", Codec: &spec.Codec{Body: groupedCSVCodecName, OutputType: "[]int"}, Predicates: []*spec.Predicate{{Name: "in", Args: []string{"s", "account_id"}}}},
			{Name: "Tenant", Source: spec.BindSource{Kind: "path", Name: "tenant"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"s", "tenant"}}}},
			{Name: "Region", Source: spec.BindSource{Kind: "header", Name: "X-Region"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"s", "region"}}}},
			{Name: "Channel", Source: spec.BindSource{Kind: "cookie", Name: "channel"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"s", "channel"}}}},
			{Name: "Status", Source: spec.BindSource{Kind: "form", Name: "status"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"s", "status"}}}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{
			Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/acme/reporting", Name: "spend"},
			Name: "spend", Groupable: &groupable,
			Selector: &spec.Selector{
				AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true,
				Orderable:    []spec.FieldPath{"total_spend", "account_id"},
				OrderAliases: map[string]spec.FieldPath{"TotalSpend": "total_spend", "AccountID": "account_id"},
			},
			Source: &spec.ViewSource{SQL: `SELECT s.tenant, s.account_id, s.region,
    SUM(s.amount) AS total_spend, COUNT(*) AS order_count
FROM report_spend s
WHERE 1 = 1
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}
GROUP BY s.tenant, s.account_id, s.region`},
			Columns: []*spec.Column{
				{Name: "Tenant", Source: "tenant", Groupable: &groupable},
				{Name: "AccountID", Source: "account_id", Groupable: &groupable},
				{Name: "Region", Source: "region", Groupable: &groupable},
				{Name: "TotalSpend", Source: "total_spend"},
				{Name: "OrderCount", Source: "order_count"},
			},
			Relations: []*spec.Relation{{
				Name: "Details", Holder: "Details", Cardinality: spec.CardinalityMany,
				On: []*spec.RelationLink{
					{ParentColumn: "tenant", ChildColumn: "tenant"},
					{ParentColumn: "account_id", ChildColumn: "account_id"},
				},
				View: &spec.View{
					Key: spec.Key{Kind: spec.KindView, Scope: "example.com/acme/reporting", Name: "detail"}, Name: "detail", BatchSize: 2,
					Source: &spec.ViewSource{SQL: "SELECT tenant, account_id, label FROM report_detail WHERE $COLUMN_IN ORDER BY tenant, account_id, label"},
				},
			}},
		},
	}
}
