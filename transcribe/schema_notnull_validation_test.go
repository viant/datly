package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/sqlx/io/config"
)

func TestSchemaDerivedNotNullValidation(t *testing.T) {
	const ddl = "CREATE TABLE schema_records(id INTEGER, enabled BOOLEAN NOT NULL, note TEXT)"
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if e := h.ExecStatements(ctx, ddl); e != nil {
		t.Fatal(e)
	}
	session, e := config.Session(ctx, h.DB)
	if e != nil {
		t.Fatal(e)
	}
	cols, e := config.Columns(ctx, session, h.DB, "schema_records")
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range cols {
		if c.Name == "enabled" {
			t.Logf("authoritative table enabled Nullable=%q", c.Nullable)
			if c.IsNullable() {
				t.Fatal("fixture lost NOT NULL")
			}
		}
	}
	// The pinned SQLite driver reports result columns as nullable even when
	// table metadata above proves NOT NULL. Neither path can replace the other.
	for _, query := range []string{"SELECT * FROM schema_records", "SELECT enabled AS flag FROM schema_records", "SELECT COALESCE(enabled,0) AS flag FROM schema_records"} {
		rows, err := h.DB.QueryContext(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		types, err := rows.ColumnTypes()
		closeErr := rows.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("result metadata: %v, close: %v", err, closeErr)
		}
		for _, column := range types {
			nullable, known := column.Nullable()
			if !known || !nullable {
				t.Fatalf("SQLite result metadata changed for %s: nullable=%v known=%v", query, nullable, known)
			}
		}
	}
	for _, authored := range []string{"schema", "sqlx", "validate"} {
		t.Run(fmt.Sprintf("authority=%s", authored), func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/schemavalidation"}).Write(t, root)
			text := "#setting($_ = $route('/records','GET'))\nSELECT r.*,CAST(r.enabled AS *bool)"
			if authored == "sqlx" {
				text += `,tag(r.enabled,'sqlx:"enabled,required"')`
			}
			if authored == "validate" {
				text += `,tag(r.enabled,'validate:notnull')`
			}
			text += " FROM schema_records r"
			src := &Source{Scope: "example.com/generated/schemarecords", Name: "SchemaRecords", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: text}
			compiled, e := NewCompiler().Compile(ctx, src)
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("canonical Source.Table=%q", compiled.Component.RootView.Source.Table)
			generated, e := transcribeSource(ctx, root, src)
			if e != nil {
				t.Fatal(e)
			}
			for _, view := range generated.Result.Plan.Views {
				for _, f := range view.Fields {
					if f.Name == "Enabled" {
						t.Logf("generated Enabled type=%s tag=%s", f.Type, f.Tag)
					}
				}
			}
			consumer := fmt.Sprintf(`package %s
import("context";"testing";"github.com/viant/datly/internal/testharness";"github.com/viant/datly/sql/dml"; h "github.com/viant/xdatly/handler")
func TestSchemaNotNullConsumer(t *testing.T){
 ctx:=context.Background();db:=testharness.NewSQLiteHarness(t);if e:=db.ExecStatements(ctx,%q);e!=nil{t.Fatal(e)}
 validator:=dml.NewData(db.DB).FrameworkValidator();policy:=h.ValidationOptions{Action:h.WriteInsert,Shallow:true}
 invalid,e:=validator.Validate(ctx,&%s{},policy);if e!=nil{t.Fatal(e)}
 found:=false;for _,v:=range invalid.Violations{if v.Field=="Enabled"&&v.Check=="notnull"{found=true}}
 if !found{t.Errorf("schema NOT NULL omitted from generated validation: %%+v",invalid)}
 disabled:=false;valid,e:=validator.Validate(ctx,&%s{Enabled:&disabled},policy);if e!=nil||valid.Failed{t.Fatalf("false or optional NULL wrongly rejected: %%v %%v",valid,e)}
}
`, generated.Package.Name, ddl, generated.Result.Plan.RootViewType, generated.Result.Plan.RootViewType)
			if e = os.WriteFile(filepath.Join(root, "generated", "schema_validation_consumer_test.go"), []byte(consumer), 0644); e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command("go", "test", "-mod=mod", "./...")
			cmd.Dir = root
			if out, e := cmd.CombinedOutput(); e != nil {
				t.Fatalf("generated consumer: %v\n%s", e, out)
			}
		})
	}
}
