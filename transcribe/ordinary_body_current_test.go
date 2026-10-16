package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

const ordinaryBodyCurrentDQL = `#package('github.com/viant/datly/bodycurrentfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Data<[]*Record>(output/body))
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CURRENT_ROW_TYPE').WithColumnType('id','int').WithColumnType('type_id','int').WithColumnType('entity_id','int').WithColumnType('contact_id','int').WithColumnType('created','int').WithColumnType('updated','int') /* SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated FROM records r */)
SELECT r.id, r.type_id, r.entity_id, r.contact_id, r.created, r.updated, r.should_delete,
       type(r,'Record'), CAST(r.id AS int), CAST(r.type_id AS int),
       CAST(r.entity_id AS int), CAST(r.contact_id AS int), CAST(r.created AS int),
       CAST(r.updated AS int), CAST(r.should_delete AS bool), internal_transient(r.should_delete), delete_marker(r.should_delete)
FROM (SELECT src.*, '' AS should_delete FROM records src) r
`

func TestOrdinaryGeneratedBodySharedAndDistinctRows(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, currentType string
		wantErr           string
	}{
		{name: "same nominal row shared", currentType: "Record"},
		{name: "distinct row control", currentType: "CurrentRow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if evidenceRoot := os.Getenv("WATCHLIST_ROW_COUNTERPROOF_DIR"); evidenceRoot != "" {
				root = filepath.Join(evidenceRoot, strings.ReplaceAll(tc.name, " ", "-"))
				if err := os.MkdirAll(root, 0700); err != nil {
					t.Fatal(err)
				}
			}
			(testharness.GeneratedModule{Path: "github.com/viant/datly/bodycurrentfixture"}).Write(t, root)
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY, type_id INTEGER, entity_id INTEGER, contact_id INTEGER, created INTEGER, updated INTEGER)"); err != nil {
				t.Fatal(err)
			}
			sourceText := strings.Replace(ordinaryBodyCurrentDQL, "CURRENT_ROW_TYPE", tc.currentType, 1)
			source := &Source{
				Name: "records", Scope: "github.com/viant/datly/bodycurrentfixture/source",
				Connector: "main", Text: sourceText,
				ColumnRefiner: column.New(column.Connections{"main": db.DB}),
			}
			compiled, err := NewCompiler().Compile(ctx, source)
			if tc.wantErr != "" {
				t.Logf("canonical same-type compile/planning error: %v", err)
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got error %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if compiled.ExternalHandler != nil || compiled.Component.RootView == nil || len(compiled.Component.Views) != 1 {
				t.Fatalf("fixture unexpectedly entered factory or changed ordinary body/current shape: external=%#v root=%v independentViews=%d", compiled.ExternalHandler, compiled.Component.RootView != nil, len(compiled.Component.Views))
			}
			t.Log("compiled ordinary DQL body/current with no ExternalHandler or generated factory InputShape")
			generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
			if err != nil {
				t.Fatal(err)
			}
			if generated == nil || generated.Result == nil {
				t.Fatal("distinct-type control did not return a generation result")
			}
			var bodyType, currentType string
			currentFieldCount := 0
			for _, view := range generated.Result.Plan.Views {
				if view.Name == "Record" {
					bodyType = view.Type
				}
				if view.Name == tc.currentType {
					currentType = view.Type
					currentFieldCount = len(view.Fields)
				}
			}
			wantFields := 6
			if tc.currentType == "Record" {
				wantFields = 0
			}
			if bodyType != "Record" || currentType != tc.currentType || currentFieldCount != wantFields {
				t.Fatalf("unexpected planned views body=%q Current=%q CurrentFields=%d", bodyType, currentType, currentFieldCount)
			}
			t.Logf("distinct-type control generated body=%s Current=%s CurrentFields=%d", bodyType, currentType, currentFieldCount)
			generatedDir := filepath.Join(root, "generated")
			entries, err := os.ReadDir(generatedDir)
			if err != nil {
				t.Fatal(err)
			}
			var goFiles []string
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
					goFiles = append(goFiles, filepath.Join(generatedDir, entry.Name()))
				}
			}
			var bodySource, currentSource string
			for _, path := range goFiles {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				text := string(data)
				if strings.Contains(text, "type Record struct") {
					bodySource += generatedStructBlock(text, "Record")
				}
				if strings.Contains(text, "type CurrentRow struct") {
					currentSource += generatedStructBlock(text, "CurrentRow")
				}
			}
			if !strings.Contains(bodySource, "ShouldDelete bool") || !strings.Contains(bodySource, "*RecordHas") {
				t.Fatalf("body control lost native writer delete/presence shape: %s", bodySource)
			}
			if strings.Contains(currentSource, "ShouldDelete") || strings.Contains(currentSource, "RecordHas") {
				t.Fatalf("distinct Current control unexpectedly inherited writer-only shape: %s", currentSource)
			}
		})
	}
}

func generatedStructBlock(source, name string) string {
	marker := "type " + name + " struct {"
	start := strings.Index(source, marker)
	if start < 0 {
		return ""
	}
	remaining := source[start:]
	end := strings.Index(remaining, "\n}")
	if end < 0 {
		return remaining
	}
	return remaining[:end+2]
}
