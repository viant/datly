package transcribe

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func TestValidatorSchemaDQLSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, query, connector, failure string
		static                          bool
	}{
		{name: "table", query: "SELECT r.id FROM records r", connector: "main"},
		{name: "authored connector", query: "#setting($_ = $connector('main'))\nSELECT r.id FROM records r", connector: "fallback"},
		{name: "derived table", query: "SELECT r.id FROM (SELECT id FROM records) r", connector: "main"},
		{name: "query only", query: "SELECT r.id FROM (SELECT id FROM records UNION SELECT id FROM records) r", connector: "main"},
		{name: "missing table", query: "SELECT r.id FROM absent r", connector: "main", failure: "no such table"},
		{name: "missing column", query: "SELECT r.absent FROM records r", connector: "main", failure: "no such column"},
		{name: "missing connector", query: "SELECT r.id FROM records r", connector: "absent", failure: "connector"},
		{name: "static", query: "SELECT r.id FROM absent r", connector: "absent", static: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL)", "INSERT INTO records VALUES(1)", "PRAGMA query_only=ON"); err != nil {
				t.Fatal(err)
			}
			base := t.TempDir()
			writeSourceFile(t, base, "go.mod", discoverGoMod)
			writeSourceFile(t, base, "svc/records/Records.dql", "#setting($_ = $route('/records','GET'))\n"+tc.query)
			writeSourceFile(t, base, "generated/preserve.txt", "existing generated content")
			before := validationFiles(t, base)
			validator := &Validator{BaseDir: base, Include: []string{"example.com/app/svc/records"}, Connector: tc.connector}
			if !tc.static {
				validator.ColumnRefiner = column.New(column.Connections{"main": h.DB})
			}
			report, err := validator.Validate(ctx)
			if tc.failure != "" {
				if err == nil || report.Valid || len(report.Diagnostics) == 0 || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("report=%+v error=%v", report, err)
				}
				if len(report.Schema) != 0 || strings.Contains(strings.Join(report.Completed, ";"), "database column") {
					t.Fatalf("failed discovery reported completed: %+v", report)
				}
			} else {
				if err != nil || !report.Valid {
					t.Fatalf("report=%+v error=%v", report, err)
				}
				if tc.static {
					if len(report.Schema) != 0 || report.Skipped[0] != "database schema and constraints" {
						t.Fatalf("static checks acquired schema claims: %+v", report)
					}
				} else {
					if len(report.Schema) != 1 || report.Schema[0].Connector != "main" || !strings.Contains(report.Skipped[0], "runtime payload") {
						t.Fatalf("schema report=%+v", report)
					}
					if tc.name == "query only" && (report.Schema[0].Table != "" || len(report.Schema[0].Completed) != 2 || strings.Contains(strings.Join(report.Schema[0].Completed, ";"), "metadata")) {
						t.Fatalf("query-only discovery claimed table metadata: %+v", report.Schema)
					}
					if (tc.name == "table" || tc.name == "derived table") && (report.Schema[0].Table != "records" || len(report.Schema[0].Completed) != 4) {
						t.Fatalf("explicit table discovery missing: %+v", report.Schema)
					}
				}
			}
			if after := validationFiles(t, base); !reflect.DeepEqual(before, after) {
				t.Fatal("validation changed source or generated files")
			}
			var count int
			if err := h.DB.QueryRow("SELECT COUNT(*) FROM records WHERE id=1").Scan(&count); err != nil || count != 1 {
				t.Fatalf("database changed: count=%d err=%v", count, err)
			}
		})
	}
}

func TestValidatorSchemaGoAndLinkedSQLite(t *testing.T) {
	for _, overlay := range []bool{false, true} {
		for _, missing := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{false: "go", true: "linked"}[overlay], map[bool]string{false: "valid", true: "missing input view"}[missing]}, "/"), func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				if err := h.ExecStatements(ctx, "CREATE TABLE users(ID INTEGER)", "CREATE TABLE audit(ID INTEGER)", "INSERT INTO users VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				if missing {
					if err := h.ExecStatements(ctx, "DROP TABLE audit"); err != nil {
						t.Fatal(err)
					}
				}
				base := t.TempDir()
				testharness.WriteGeneratedGoMod(t, base)
				writeSourceFile(t, base, "model/audit.go", "package model\ntype AuditRow struct{ID int}")
				text := strings.Replace(discoveredPackageComponent, "import (", "import (\n\"context\"", 1)
				text += "\nfunc init(){panic(\"application initialization executed\")}\nfunc(*UsersInput)Init(context.Context)error{panic(\"application hook executed\")}\n"
				writeSourceFile(t, base, "svc/users/component.go", text)
				if overlay {
					writeSourceFile(t, base, "svc/users/Users.dql", "#setting($_ = $route('/v1/users', 'GET'))\nSELECT ID FROM users")
				}
				before := validationFiles(t, base)
				report, err := (&Validator{BaseDir: base, Include: []string{"example.com/generated/svc/users"}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB})}).Validate(ctx)
				if missing {
					if err == nil || report.Valid || !strings.Contains(err.Error(), "audit") || len(report.Diagnostics) == 0 {
						t.Fatalf("missing Go input-view table accepted: %+v, %v", report, err)
					}
				} else if err != nil || !report.Valid || len(report.Schema) != 2 {
					t.Fatalf("Go schema report=%+v error=%v", report, err)
				}
				if !reflect.DeepEqual(before, validationFiles(t, base)) {
					t.Fatal("validation executed hooks or changed files")
				}
			})
		}
	}
}

func TestValidatorSchemaReportsOnlySourceDiscovery(t *testing.T) {
	report := &ValidationReport{}
	project := &ProjectGeneration{Components: []*Result{{Source: &Source{}, Component: &spec.Component{
		RootView: &spec.View{Name: "NoSQL"},
	}}}}
	if err := report.schemaDiscovery(project); err != nil {
		t.Fatal(err)
	}
	if len(report.Schema) != 0 || len(report.Completed) != 0 || len(report.Skipped) == 0 {
		t.Fatalf("uninspected source reported completed: %+v", report)
	}
}

func TestValidatorSchemaIndependentConnectorSQLite(t *testing.T) {
	ctx := context.Background()
	main, audit := sqlite.New(t), sqlite.New(t)
	if err := main.ExecStatements(ctx, "CREATE TABLE users(ID INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := audit.ExecStatements(ctx, "CREATE TABLE audit(ID INTEGER)"); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "model/audit.go", "package model\ntype AuditRow struct{ID int}")
	text := strings.ReplaceAll(discoveredPackageComponent, `view:"Audit,table=audit"`, `view:"Audit,table=audit,connector=audit"`)
	writeSourceFile(t, base, "svc/users/component.go", text)
	report, err := (&Validator{BaseDir: base, Include: []string{"example.com/generated/svc/users"}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": main.DB, "audit": audit.DB})}).Validate(ctx)
	if err != nil || !report.Valid {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	for _, inspection := range report.Schema {
		if inspection.View == "Audit" && inspection.Connector == "audit" {
			return
		}
	}
	t.Fatalf("independent connector evidence missing: %+v", report.Schema)
}

func TestValidatorSchemaConditionalQueryDoesNotClaimMetadata(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE audit(ID INTEGER)"); err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "model/audit.go", "package model\ntype AuditRow struct{ID int}")
	text := strings.ReplaceAll(discoveredPackageComponent, `view:"Users,table=users"`, `view:"Users"`)
	text = strings.ReplaceAll(text, "SELECT ID FROM users", "#if(false) SELECT ID FROM users #end")
	writeSourceFile(t, base, "svc/users/component.go", text)
	report, err := (&Validator{BaseDir: base, Include: []string{"example.com/generated/svc/users"}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": h.DB})}).Validate(context.Background())
	if err != nil || !report.Valid {
		t.Fatalf("report=%+v error=%v", report, err)
	}
	for _, inspection := range report.Schema {
		if inspection.View == "Users" && (inspection.Table != "" || strings.Contains(strings.Join(inspection.Completed, ";"), "column discovery")) {
			t.Fatalf("inactive query reported as inspected: %+v", inspection)
		}
	}
	if !strings.Contains(strings.Join(report.Skipped, ";"), "Users (query-only evaluated-query evidence unavailable)") {
		t.Fatalf("inactive source coverage not distinguished: %+v", report)
	}
}

func TestValidatorSchemaCancellationAndConnectorFailure(t *testing.T) {
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", discoverGoMod)
	writeSourceFile(t, base, "svc/records/Records.dql", "#setting($_ = $route('/records','GET'))\nSELECT id FROM records")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	validator := &Validator{BaseDir: base, Include: []string{"example.com/app/svc/records"}, Connector: "main", ColumnRefiner: column.New(column.Connections{})}
	report, err := validator.Validate(ctx)
	if !errors.Is(err, context.Canceled) || report.Valid || len(report.Schema) != 0 {
		t.Fatalf("canceled report=%+v error=%v", report, err)
	}
	h := sqlite.New(t)
	_ = h.DB.Close()
	validator.ColumnRefiner = column.New(column.Connections{"main": h.DB})
	report, err = validator.Validate(context.Background())
	if err == nil || report.Valid || len(report.Diagnostics) == 0 || len(report.Schema) != 0 {
		t.Fatalf("closed connection accepted: %+v error=%v", report, err)
	}
}

func validationFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := fs.WalkDir(os.DirFS(root), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := os.ReadFile(filepath.Join(root, name))
		result[name] = string(content)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
