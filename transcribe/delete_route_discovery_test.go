package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"strings"
	"testing"
)

const deleteRouteDQL = `#package('example.com/deletefixture/api/records')
#setting($_ = $route('/records','DELETE'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Rows<[]*Record>(body/data).Optional())
#define($_ = $Data<[]*Record>(output/body))
SELECT r.ID,r.remove,type(r,'Record'),CAST(r.ID AS int64),CAST(r.remove AS bool),tag(r.ID,'sqlx:"ID,primaryKey"'),tag(r.remove,'sqlx:"-"'),delete_marker(r.remove),lifecycle_type(r,'RecordLifecycle')
FROM (SELECT ID,'' AS remove FROM RECORDS) r`

func TestDeleteOnlyDiscoveryThenExplicitGeneration(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"patch", "put", "get", "post"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/deletefixture"}).Write(t, root)
			writeSourceFile(t, root, "source/records/records.dql", deleteRouteDQL)
			project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/deletefixture/source/records"}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}).Compile(ctx)
			if err != nil {
				t.Fatalf("DELETE declaration staging failed: %v", err)
			}
			if len(project.Components) != 1 {
				t.Fatal("component count")
			}
			component := project.Components[0].Component
			if component.Settings != nil && component.Settings.Mutation != "" {
				t.Fatal("staging inferred an operation")
			}
			generated, err := (Generator{Operation: operation}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
			if operation == "get" || operation == "post" {
				if err == nil {
					t.Fatal("unsupported operation emitted DELETE graph")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if generated.Result.Plan.Settings.Mutation != operation || generated.Result.Plan.Routes[0].Method != "DELETE" {
				t.Fatal("operation/transport metadata changed")
			}
			if generated.Result.Plan.HookScaffold == nil {
				t.Fatal("native lifecycle not generated")
			}
			if generated.Package.PkgPath != "example.com/deletefixture/api/records" {
				t.Fatal("destination escaped source authority")
			}
		})
	}
}

func TestDeleteOnlyDiscoveryRejectsMarkerlessAndMixedReaderLifecycle(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{strings.Replace(deleteRouteDQL, ",delete_marker(r.remove)", "", 1), strings.Replace(deleteRouteDQL, "'/records','DELETE'", "'/records','DELETE','GET'", 1)} {
		root := t.TempDir()
		(testharness.GeneratedModule{Path: "example.com/deletefixture"}).Write(t, root)
		writeSourceFile(t, root, "source/records/records.dql", text)
		if _, err := (&Discovery{BaseDir: root, Include: []string{"example.com/deletefixture/source/records"}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}).Compile(ctx); err == nil {
			t.Fatal("unsupported DELETE lifecycle staged")
		}
	}
}
