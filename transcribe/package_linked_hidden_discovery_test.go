package transcribe

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/column"
)

type LinkedHiddenDiscoveryInput struct{}
type LinkedHiddenDiscoveryRow struct {
	CreatedAtRaw *string `internal:"true" json:"-" sqlx:"created_at_raw"`
	ActivityRaw  *string `json:"-" sqlx:"activity_raw"`
	ID           string  `sqlx:"id" json:"id"`
}
type LinkedHiddenDiscoveryOutput struct {
	Data []*LinkedHiddenDiscoveryRow `parameter:"Data,kind=output,in=view" view:"reader,table=conversation" json:"data" sql:"SELECT conversation.* FROM (SELECT conversation.created_at_raw, conversation.activity_raw, conversation.id FROM (SELECT t.*,CAST(t.created_at AS CHAR) AS created_at_raw,CAST(COALESCE(t.last_activity,t.updated_at,t.created_at) AS CHAR) AS activity_raw FROM conversation t) conversation) conversation"`
}

func TestLinkedHiddenDiscoveryPreservesSQLMappedTypeAuthority(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE conversation(id TEXT NOT NULL, created_at TIMESTAMP, last_activity TIMESTAMP, updated_at TIMESTAMP)"); err != nil {
		t.Fatal(err)
	}
	in, out := reflect.TypeFor[LinkedHiddenDiscoveryInput](), reflect.TypeFor[LinkedHiddenDiscoveryOutput]()
	route := &bootstrap.RouteSource{FieldName: "reader", PackagePath: in.PkgPath(), InputType: in.Name(), OutputType: out.Name(), LinkedInputType: in, LinkedOutputType: out, Tag: tag.Component{Name: "reader", Method: "GET", Path: "/conversation", Connector: "main"}}
	result, err := (&PackageCompilation{Source: &Source{Scope: in.PkgPath(), Name: "reader", ColumnRefiner: column.New(column.Connections{"main": h.DB})}, Component: route, InputType: in, OutputType: out}).Compile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	declarations, err := bootstrap.DiscoveryOutputColumns(result.Component, out)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, col := range declarations {
		if col.Source == "created_at_raw" || col.Source == "activity_raw" {
			found[col.Source] = true
			if col.Type.Name != "string" || !col.Type.Pointer || !col.Nullable {
				t.Fatalf("linked hidden type/nullability lost: %+v", col)
			}
		}
	}
	for _, name := range []string{"created_at_raw", "activity_raw"} {
		if !found[name] {
			t.Fatalf("linked hidden declaration missing: %s", name)
		}
	}
	if err = h.ExecStatements(ctx, "INSERT INTO conversation(id) VALUES('nullable')"); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: result.Component, InputType: in, OutputType: out, Types: result.Source.Types})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := execution.Read(ctx, &LinkedHiddenDiscoveryInput{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := value.(*LinkedHiddenDiscoveryOutput).Data
	if len(rows) != 1 || rows[0].ID != "nullable" || rows[0].CreatedAtRaw != nil || rows[0].ActivityRaw != nil {
		t.Fatalf("nullable linked row failed: %+v", rows)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "raw") || strings.Contains(string(encoded), "Raw") {
		t.Fatalf("hidden fields leaked: %s", encoded)
	}
	if result.Component.RootView.Source.SQL != reflect.TypeFor[LinkedHiddenDiscoveryOutput]().Field(0).Tag.Get("sql") {
		t.Fatal("authored SQL changed")
	}
	if len(result.Component.RootView.Columns) != 1 || result.Component.RootView.Columns[0].Source != "id" {
		t.Fatalf("hidden fields widened projection: %+v", result.Component.RootView.Columns)
	}
}
