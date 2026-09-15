package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
)

func TestInstanceFilesDiscoveryGenerationPreserveSource(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE `e2e.ds.records` (id INTEGER,name TEXT)", "CREATE TABLE `prod.ds.records` (id INTEGER,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	authored := "#setting($_ = $route('/records', 'GET'))\n#setting($_ = $const('project', 'authored'))\n#define($_ = $ID<int>(const/ID).Value('7'))\nSELECT id, name FROM `$project.ds.records` WHERE id=:ID"
	var generatedSQL map[string]string
	for _, stage := range []string{"e2e", "prod"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			path := filepath.Join(root, "Records.dql")
			if err := os.WriteFile(path, []byte(authored), 0600); err != nil {
				t.Fatal(err)
			}
			values, err := constant.New(map[string]string{"project": stage, "ID": "0"})
			if err != nil {
				t.Fatal(err)
			}
			source := &Source{Scope: "example.com/generated/records", Name: "Records", Path: path, Text: authored, Connector: "main", Const: values, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB})}
			generated, err := transcribeSource(ctx, root, source)
			if err != nil {
				t.Fatal(err)
			}
			if source.Text != authored {
				t.Fatal("source overwritten")
			}
			data, _ := os.ReadFile(path)
			if string(data) != authored {
				t.Fatal("authored file overwritten")
			}
			if len(generated.Result.Plan.Input.Fields) < 2 {
				t.Fatal("constant input fields missing")
			}
			serialized, err := json.Marshal(generated.Result.Plan)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(serialized), stage+".ds.records") {
				t.Fatal("expanded identifier persisted in generation plan")
			}
			foundSQL := false
			sqlFiles := map[string]string{}
			err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				if filepath.Ext(path) != ".sql" {
					return nil
				}
				text, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				relative, _ := filepath.Rel(root, path)
				sqlFiles[relative] = string(text)
				if strings.Contains(string(text), "$project.ds.records") {
					foundSQL = true
				}
				if strings.Contains(string(text), stage+".ds.records") {
					t.Errorf("expanded generated SQL %s", path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !foundSQL {
				t.Fatal("generated SQL lost authored placeholder")
			}
			if generatedSQL == nil {
				generatedSQL = sqlFiles
			} else {
				left, _ := json.Marshal(generatedSQL)
				right, _ := json.Marshal(sqlFiles)
				if string(left) != string(right) {
					t.Fatal("generated SQL differs between instances")
				}
			}
			if !strings.Contains(string(serialized), "value=authored") || !strings.Contains(string(serialized), "value=7") {
				t.Fatalf("authored constant defaults not retained: %s", serialized)
			}
		})
	}
}

func TestInstanceOverrideValidationPrecedesDatabase(t *testing.T) {
	defaults := "#setting($_ = $route('/records', 'GET'))\n#define($_ = $Count<int64>(const/Count).Value('7'))\nSELECT * FROM records"
	for _, text := range []string{defaults, strings.Replace(defaults, "SELECT", "#setting($_ = $const('Count','8'))\nSELECT", 1)} {
		values, _ := constant.New(map[string]string{"Count": "invalid"})
		_, err := NewCompiler().Compile(context.Background(), &Source{Text: text, Name: "Records", Scope: "example/records", Const: values})
		if err == nil {
			t.Fatal("invalid typed override accepted")
		}
		if text != defaults && !strings.Contains(err.Error(), "conflicting") {
			t.Fatalf("external override concealed authored conflict: %v", err)
		}
	}
}

func TestInstanceUnsafeTableDiscoveryUsesEvaluatedTarget(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE actual_records (id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	values, _ := constant.New(map[string]string{"Table": "actual_records"})
	authored := "#setting($_ = $route('/records','GET'))\n#setting($_ = $const('Table','absent_default_table'))\nSELECT id,name FROM $Unsafe.Table"
	compiled, err := NewCompiler().Compile(context.Background(), &Source{Const: values, Name: "Records", Scope: "example/records", Connector: "main", Text: authored, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB})})
	if err != nil {
		t.Fatal(err)
	}
	column := compiled.Component.RootView.Columns[0]
	if !column.PrimaryKey {
		t.Fatalf("metadata used authored default instead of instance table: %+v", column)
	}
	if compiled.Source.Text != authored || !strings.Contains(compiled.Component.RootView.Source.SQL, "$Unsafe.Table") {
		t.Fatal("authored SQL changed")
	}
	if compiled.Component.Settings.Const["Table"] != "absent_default_table" {
		t.Fatal("instance value replaced authored metadata")
	}
}
