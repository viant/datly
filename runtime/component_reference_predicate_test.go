package runtime

import (
	"context"
	"fmt"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
	"reflect"
	"testing"
)

type referencePredicate struct{}

func (*referencePredicate) Compute(_ context.Context, value any) (*xpredicate.Criteria, error) {
	auth, ok := value.(*referenceAuth)
	if !ok || auth == nil {
		return nil, fmt.Errorf("missing reference authorization")
	}
	if !auth.roles["reader"] {
		return &xpredicate.Criteria{Expression: "1=0"}, nil
	}
	return &xpredicate.Criteria{Expression: "tenant_id = ?", Placeholders: []any{7}}, nil
}

type referenceSQLInput struct {
	Auth *referenceAuth    `parameter:"Auth,kind=component,in=GET:/reference-sql-auth" predicate:"handler,example.ReferencePredicate"`
	Has  *referenceMarkers `setMarker:"true"`
}
type referenceSQLRow struct {
	ID int `sqlx:"id"`
}
type referenceSQLOutput struct {
	Data      []referenceSQLRow
	Finalized bool
}

func (o *referenceSQLOutput) Finalize(context.Context) error { o.Finalized = true; return nil }
func TestReaderSeedBorrowedReferenceNativePredicateSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER, tenant_id INTEGER)", "INSERT INTO records VALUES (1, 7), (2, 7), (3, 8)"); err != nil {
		t.Fatal(err)
	}
	child := componentArtifact(t, componentSpec("ReferenceSQLAuth", "GET", "/reference-sql-auth", nil), reflect.TypeFor[componentGuardInput](), reflect.TypeFor[referenceAuth]())
	parent := componentSpec("ReferenceSQLRecords", "GET", "/reference-sql-records", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	parent.RootView = &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`}}
	types := typecatalog.NewCatalog()
	if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[referencePredicate](), PkgPath: "example", Name: "ReferencePredicate"}); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parent, InputType: reflect.TypeFor[referenceSQLInput](), OutputType: reflect.TypeFor[referenceSQLOutput](), DirectViewField: "Data", Types: types})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[referenceSQLInput](), OutputType: reflect.TypeFor[referenceSQLOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: child.Component, Input: child.Input, OutputType: reflect.TypeFor[referenceAuth](), Handler: customhandler.NewFunc[componentGuardInput, referenceAuth](func(context.Context, *componentGuardInput) (*referenceAuth, error) {
		calls++
		return nil, fmt.Errorf("dependency must not execute")
	})}, {Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[referenceSQLOutput](), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	target := dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/reference-sql-records"}}
	for _, granted := range []bool{true, false} {
		input := &referenceSQLInput{Auth: &referenceAuth{roles: map[string]bool{"reader": granted}}, Has: &referenceMarkers{Auth: true}}
		value, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(input)})
		if err != nil {
			t.Fatal(err)
		}
		out := value.(*referenceSQLOutput)
		expected := []referenceSQLRow(nil)
		if granted {
			expected = []referenceSQLRow{{1}, {2}}
		}
		if len(out.Data) != len(expected) || (granted && !reflect.DeepEqual(out.Data, expected)) || !out.Finalized {
			t.Fatalf("predicate/finalize mismatch: %#v", out)
		}
	}
	if calls != 0 {
		t.Fatal("borrowed dependency executed")
	}
}
