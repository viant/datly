package transcribe

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	"github.com/viant/x"
)

type linkedDiscoveryInput struct {
	Tenant int `parameter:"TenantID,kind=query,in=tenantID"`
}

type linkedDiscoveryDateInput struct {
	KeywordFrom *time.Time `parameter:"KeywordFrom,kind=form,in=keyword_from"`
	KeywordTo   *time.Time `parameter:"KeywordTo,kind=form,in=keyword_to"`
	Region      string     `parameter:"Region,kind=query,in=region"`
}

func TestDiscoveryInputCompilerUsesLinkedContractIdentity(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(linkedDiscoveryInput{}),
		x.WithName("LinkedDiscoveryInput"),
		x.WithPkgPath("example.com/contracts"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		DefaultPackage: "example.com/contracts",
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	defaultTenant := "17"
	compiled, err := (&discoveryInputCompiler{
		component: &spec.Component{
			Settings: &spec.Settings{InputType: "LinkedDiscoveryInput"},
			Parameters: []*spec.Parameter{
				{Name: "TenantID", TypeExpr: "int", Value: &defaultTenant, Source: spec.BindSource{Kind: "query", Name: "tenantID"}},
				{Name: "Sequencer", TypeExpr: "any", Source: spec.BindSource{Kind: "sequencer"}},
			},
		},
		resolver: resolver,
	}).compile()
	if err != nil {
		t.Fatalf("compile() error = %v", err)
	}
	if compiled.Value.Type() != reflect.TypeOf(linkedDiscoveryInput{}) {
		t.Fatalf("input type = %v", compiled.Value.Type())
	}
	if compiled.Value.FieldByName("Tenant").Int() != 17 {
		t.Fatalf("linked input = %#v", compiled.Value.Interface())
	}
	binder := sqlx.NewParameterBinder(compiled.ParameterResolver)
	SQL, args, err := binder.Bind("SELECT :TenantID, :tenantID, :Tenant")
	if err != nil || SQL != "SELECT ?, ?, ?" || !reflect.DeepEqual(args, []any{17, 17, 17}) {
		t.Fatalf("Bind() = %q, %#v, %v", SQL, args, err)
	}
}

func TestDiscoveryInputCompilerMaterializesGeneratedContractDefaults(t *testing.T) {
	id, table, enabled := "7", "events", "true"
	compiled, err := (&discoveryInputCompiler{
		component: &spec.Component{
			Name: "Events",
			Parameters: []*spec.Parameter{
				{Name: "ID", TypeExpr: "int", Value: &id, Source: spec.BindSource{Kind: "query", Name: "id"}},
				{Name: "Table", TypeExpr: "string", Value: &table, Source: spec.BindSource{Kind: "const", Name: "Table"}},
				{Name: "Enabled", TypeExpr: "bool", Value: &enabled, Source: spec.BindSource{Kind: "query", Name: "enabled"}},
			},
		},
	}).compile()
	if err != nil {
		t.Fatalf("compile() error = %v", err)
	}
	if compiled.Value.Type().Name() != "" || compiled.Value.FieldByName("ID").Int() != 7 ||
		compiled.Value.FieldByName("Table").String() != "events" || !compiled.Value.FieldByName("Enabled").Bool() {
		t.Fatalf("generated input = %#v", compiled.Value.Interface())
	}
	binder := sqlx.NewParameterBinder(compiled.ParameterResolver)
	sqlText, args, err := binder.Bind("SELECT :ID, :id")
	if err != nil || sqlText != "SELECT ?, ?" || !reflect.DeepEqual(args, []any{7, 7}) {
		t.Fatalf("Bind() = %q, %#v, %v", sqlText, args, err)
	}
}

func TestDiscoveryInputCompilerUsesTypedDiscoveryValuesForUnsetOptionalPointers(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(linkedDiscoveryDateInput{}),
		x.WithName("LinkedDiscoveryDateInput"),
		x.WithPkgPath("example.com/contracts"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		DefaultPackage: "example.com/contracts",
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	region := "us"
	compiled, err := (&discoveryInputCompiler{
		component: &spec.Component{
			Settings: &spec.Settings{InputType: "LinkedDiscoveryDateInput"},
			Parameters: []*spec.Parameter{
				{Name: "KeywordFrom", TypeExpr: "*time.Time", Source: spec.BindSource{Kind: "form", Name: "keyword_from"}},
				{Name: "KeywordTo", TypeExpr: "*time.Time", Source: spec.BindSource{Kind: "form", Name: "keyword_to"}},
				{Name: "Region", TypeExpr: "string", Value: &region, Source: spec.BindSource{Kind: "query", Name: "region"}},
			},
		},
		resolver: resolver,
	}).compile()
	if err != nil {
		t.Fatalf("compile() error = %v", err)
	}
	if !compiled.Value.FieldByName("KeywordFrom").IsNil() || !compiled.Value.FieldByName("KeywordTo").IsNil() {
		t.Fatalf("runtime discovery input defaults changed: %#v", compiled.Value.Interface())
	}
	binder := sqlx.NewParameterBinder(compiled.ParameterResolver)
	sqlText, args, err := binder.Bind("SELECT :KeywordFrom, :keyword_to, :Region")
	if err != nil || sqlText != "SELECT ?, ?, ?" {
		t.Fatalf("Bind() = %q, %#v, %v", sqlText, args, err)
	}
	for i := 0; i < 2; i++ {
		if _, ok := args[i].(time.Time); !ok {
			t.Fatalf("arg %d = %T(%#v), want time.Time", i, args[i], args[i])
		}
	}
	if args[2] != "us" {
		t.Fatalf("args=%#v", args)
	}
}

func TestDiscoveryInputCompilerSupportsDatabaseDiscoveryWithUnsetOptionalDates(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE keyword_supply (id INTEGER, dstamp TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(
		reflect.TypeOf(linkedDiscoveryDateInput{}),
		x.WithName("LinkedDiscoveryDateInput"),
		x.WithPkgPath("example.com/contracts"),
	)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{
		DefaultPackage: "example.com/contracts",
	})
	if err != nil {
		t.Fatalf("NewResolver() error = %v", err)
	}
	component := &spec.Component{
		Settings: &spec.Settings{InputType: "LinkedDiscoveryDateInput", DefaultConnector: "main"},
		Parameters: []*spec.Parameter{
			{Name: "KeywordFrom", TypeExpr: "*time.Time", Source: spec.BindSource{Kind: "form", Name: "keyword_from"}},
			{Name: "KeywordTo", TypeExpr: "*time.Time", Source: spec.BindSource{Kind: "form", Name: "keyword_to"}},
		},
		RootView: &spec.View{Name: "KeywordSupply", Source: &spec.ViewSource{
			SQL: `SELECT id, dstamp FROM keyword_supply WHERE dstamp BETWEEN DATE(:KeywordFrom) AND DATE(:KeywordTo)`,
		}},
	}
	input, err := (&discoveryInputCompiler{component: component, resolver: resolver}).compile()
	if err != nil {
		t.Fatalf("compile() error = %v", err)
	}
	if err := tcolumn.New(tcolumn.Connections{"main": harness.DB}).Refine(ctx, component, nil, input); err != nil {
		t.Fatalf("Refine() error = %v", err)
	}
	if len(component.RootView.Columns) != 2 || component.RootView.Columns[0].Name != "id" || component.RootView.Columns[1].Name != "dstamp" {
		t.Fatalf("columns=%+v", component.RootView.Columns)
	}
}
