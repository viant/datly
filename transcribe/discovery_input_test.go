package transcribe

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	"github.com/viant/x"
)

type linkedDiscoveryInput struct {
	Tenant int `parameter:"TenantID,kind=query,in=tenantID"`
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
