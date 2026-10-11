package transcribe

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
	"reflect"
	"strings"
	"testing"
)

type LinkedInputCondition struct {
	Status string `json:"status"`
}
type LinkedInputDiscoveryRow struct {
	Condition *LinkedInputCondition `sqlx:"condition" json:"condition,omitempty"`
	ID        string                `sqlx:"id" json:"id"`
	Hidden    *string               `sqlx:"hidden" json:"-"`
}
type LinkedInputDiscoveryInput struct {
	Current []*LinkedInputDiscoveryRow `parameter:"Current,kind=view,in=Current" view:"Current,table=run" sql:"SELECT r.condition,r.id,r.hidden FROM (SELECT c.*, NULL AS condition, CAST(c.status AS CHAR) AS hidden FROM run c) r"`
}
type LinkedInputDiscoveryOutput struct {
	Data []*LinkedInputDiscoveryRow `parameter:"Data,kind=output,in=view" view:"writer" sql:"$dml.Execute(\"UPDATE run SET status='changed'\")" json:"data"`
}

func TestLinkedInputDiscoveryRetainsDeclaredConditionType(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE run(id TEXT PRIMARY KEY,status TEXT)", "INSERT INTO run VALUES('probe',NULL)"); err != nil {
		t.Fatal(err)
	}
	in, out := reflect.TypeFor[LinkedInputDiscoveryInput](), reflect.TypeFor[LinkedInputDiscoveryOutput]()
	route := &bootstrap.RouteSource{FieldName: "writer", PackagePath: in.PkgPath(), InputType: in.Name(), OutputType: out.Name(), LinkedInputType: in, LinkedOutputType: out, LinkedHandler: func() (rhandler.TypedHandler, error) { return nil, nil }, Tag: tag.Component{Name: "writer", Method: "POST", Path: "/run", Connector: "main", Handler: "Writer"}}
	result, err := (&PackageCompilation{Source: &Source{Scope: in.PkgPath(), Name: "writer", ColumnRefiner: column.New(column.Connections{"main": h.DB})}, Component: route, InputType: in, OutputType: out}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	view := result.Component.Views[0]
	if len(view.Columns) != 2 {
		t.Fatalf("hidden input field widened view projection: %+v", view.Columns)
	}
	found := false
	for _, col := range view.Columns {
		if col.Source == "condition" {
			found = true
			if col.Type.Name != "LinkedInputCondition" || !col.Type.Pointer || !col.Nullable {
				t.Fatalf("linked condition authority lost: %+v", col)
			}
		}
		if col.Source == "hidden" {
			t.Fatal("hidden column widened input view")
		}
	}
	if !found {
		t.Fatal("linked condition declaration missing")
	}
	if view.Source.SQL != in.Field(0).Tag.Get("sql") {
		t.Fatal("authored independent SQL changed")
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: result.Component, InputType: in, OutputType: out, Types: result.Source.Types, HandlerOwnedOutput: true, Handler: custom.NewFunc(func(context.Context, *LinkedInputDiscoveryInput) (*LinkedInputDiscoveryOutput, error) {
		return nil, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.ViewDependencies) != 1 {
		t.Fatal("independent reader plan missing")
	}
	dep := artifact.ViewDependencies[0]
	execution, err := sqlreader.NewExecution(sqlreader.Config{Component: dep.Component, InputType: in, OutputType: dep.TargetType, Plan: dep.Plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := execution.Read(ctx, &LinkedInputDiscoveryInput{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := value.([]*LinkedInputDiscoveryRow)
	if len(rows) != 1 || rows[0].ID != "probe" || rows[0].Condition != nil {
		t.Fatalf("NULL linked condition failed: %+v", rows)
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "Hidden") || strings.Contains(string(encoded), "hidden") {
		t.Fatalf("hidden input SQL field leaked JSON: %s", encoded)
	}
	var status any
	if err = h.DB.QueryRowContext(ctx, "SELECT status FROM run WHERE id='probe'").Scan(&status); err != nil || status != nil {
		t.Fatalf("discovery executed writer root: status=%v err=%v", status, err)
	}

}
