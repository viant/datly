package transcribe

import (
	"context"
	"fmt"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	column "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"strings"
	"testing"
	"testing/fstest"
)

func TestAuxiliaryNullEmbeddedSourceAuthority(t *testing.T) {
	for _, tc := range []struct {
		name              string
		rootAux, childAux bool
		want              bool
	}{{"both auxiliary", true, true, true}, {"writable root", false, true, false}, {"writable child", true, false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)", "CREATE TABLE evidence(id INTEGER PRIMARY KEY,parent_id INTEGER REFERENCES records(id))"); err != nil {
				t.Fatal(err)
			}
			rootFrom, childFrom := "records", "evidence"
			if tc.rootAux {
				rootFrom = "(records)"
			}
			if tc.childAux {
				childFrom = "(evidence)"
			}
			rootSQL := fmt.Sprintf("SELECT id,name FROM %s", rootFrom)
			childSQL := fmt.Sprintf("SELECT id,parent_id FROM %s", childFrom)
			resources := resource.New()
			if err := resources.Register("", fstest.MapFS{"sql/records.sql": &fstest.MapFile{Data: []byte(rootSQL)}, "sql/evidence.sql": &fstest.MapFile{Data: []byte(childSQL)}}); err != nil {
				t.Fatal(err)
			}
			text := `#setting($_ = $route('/records','PATCH'))
#define($_ = $Rows<[]*RecordsView>(body/data).Cardinality('Many').Required())
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT r.*,e.*,root_null_policy(r,'skip-auxiliary'),nested_null_policy(e,'skip-auxiliary')
FROM (${embed:sql/records.sql}) r JOIN (${embed:sql/evidence.sql}) e ON e.parent_id=r.id`
			source := &Source{Scope: "example.com/auxiliarynull/generated", Name: "Records", Connector: "main", Text: text, Resources: resources, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/auxiliarynull"}).Write(t, root)
			request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}}
			before, diagnosticErr := NewCompiler().Compile(ctx, source)
			if diagnosticErr == nil {
				t.Logf("resolved root aux=%v card=%q policy=%q table=%q child aux=%v policy=%q table=%q", before.Component.RootView.Auxiliary, before.Component.RootView.Cardinality, before.Component.RootView.RootNullPolicy, before.Component.RootView.Source.Table, before.Component.RootView.Relations[0].View.Auxiliary, before.Component.RootView.Relations[0].View.NestedNullPolicy, before.Component.RootView.Relations[0].View.Source.Table)
			}
			_, err := NewCompiler().Transcribe(ctx, request)
			if (err == nil) != tc.want {
				t.Fatalf("want=%v error=%v", tc.want, err)
			}
			if err != nil {
				return
			}
			compiled, err := NewCompiler().Compile(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			view := compiled.Component.RootView
			if !view.Auxiliary || view.RootNullPolicy != "skip-auxiliary" || len(view.Relations) != 1 || !view.Relations[0].View.Auxiliary || view.Relations[0].View.NestedNullPolicy != "skip-auxiliary" {
				t.Fatalf("lost resolved policy: %+v", view)
			}
			if !strings.Contains(view.Source.SQL, "embed:") || !strings.Contains(view.Relations[0].View.Source.SQL, "embed:") {
				t.Fatal("authored embedded sources replaced")
			}
			if source.Text != text {
				t.Fatal("authored DQL changed")
			}
			for name, want := range map[string]string{"sql/records.sql": rootSQL, "sql/evidence.sql": childSQL} {
				got, err := resources.ReadFile(name)
				if err != nil || string(got) != want {
					t.Fatal("SQL resource changed")
				}
			}
		})
	}
}
