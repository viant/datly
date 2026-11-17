package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestBorrowedOwnerDocumentationSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const module = "github.com/viant/datly/borroweddocsfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	db := sqlite.New(t, sqlite.WithDSN(filepath.Join(root, "exclusive-docs.db")))
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER PRIMARY KEY)",
		"CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)"))
	refiner := column.New(column.Connections{"main": db.DB})
	sourceDir := filepath.Join(root, "source")
	ownerDir := filepath.Join(sourceDir, "private")
	require.NoError(t, os.MkdirAll(filepath.Join(ownerDir, "docs/schemas"), 0700))
	resources := map[string]string{
		"docs/global.yml": "Columns:\n  name: Record name\n",
		"docs/owner.yaml": `Paths:
  /private: Save records
Responses:
  /private:
    '200':
      description: Saved records
      content:
        application/json:
          schema: {$ref: 'schemas/row.json'}
`,
		"docs/schemas/row.json": `{"type":"object","properties":{"id":{"type":"integer"}}}`,
	}
	for name, content := range resources {
		require.NoError(t, os.WriteFile(filepath.Join(ownerDir, name), []byte(content), 0600))
	}
	owner := `#package('` + module + `/api')
#setting($_ = $route('/private','PATCH'))
#setting($_ = $file_prefix('private_'))
#setting($_ = $input_type('PrivateInput'))
#setting($_ = $output_type('PrivateOutput'))
#setting($_ = $DocGlobalURLs('docs/global.yml'))
#setting($_ = $DocURLs('docs/owner.yaml'))
#define($_ = $Rows<[]*Row>(body/data).Cardinality('Many').Required())
SELECT Records.*,type(Records,'Row') FROM records Records`
	borrower := `#package('` + module + `/api')
#setting($_ = $route('/public','PATCH'))
#setting($_ = $file_prefix('public_'))
#setting($_ = $input_type('PublicInput'))
#setting($_ = $output_type('PublicOutput'))
#setting($_ = $borrow_sql_row('Rows/Records','` + module + `/api','Row','private/Private.dql','Private','Rows'))
#define($_ = $Rows<[]*PublicView>(body/data).Cardinality('Many').Required())
SELECT p.*,Records.*,type(p,'PublicView'),type(Records,'Row') FROM (parents) p JOIN (records) Records ON Records.parent_id=p.id`
	makeSource := func(scope, name, path, text string) *Source {
		require.NoError(t, os.WriteFile(path, []byte(text), 0600))
		return &Source{Scope: scope, Name: name, Path: path, Text: text, Connector: "main", ColumnRefiner: refiner, Types: typecatalog.NewCatalog()}
	}
	ownerSource := makeSource(module+"/source/private", "Private", filepath.Join(ownerDir, "Private.dql"), owner)
	ownerGenerated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: ownerSource, Destination: root})
	require.NoError(t, err, "standalone owner must package its relative documentation and schema")
	require.Len(t, ownerGenerated.Result.Plan.Documentation.DocURLs, 1)
	require.Contains(t, ownerGenerated.Result.Plan.Documentation.DocURLs[0], "datly_docs/default/docs/owner.yaml")
	packagedDoc, err := os.ReadFile(filepath.Join(root, "api/datly_docs/default/docs/owner.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(packagedDoc), "datly_docs/schemas/", "response schema must be bundled")
	borrowerSource := makeSource(module+"/source", "Public", filepath.Join(sourceDir, "Public.dql"), borrower)
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: borrowerSource, Destination: root})
	require.NoError(t, err, "borrowed owner plans must retain the same relative documentation resources")
	require.Len(t, generated.Result.Plan.BorrowedRows, 1)
	proof := generated.Result.Plan.BorrowedRows[0]
	identity := func(path string) string { return path }
	require.NoError(t, proof.ValidateFiles(identity))
	require.NoError(t, proof.ValidateSchema())
	for name, original := range resources {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(ownerDir, name)
			info, err := os.Stat(path)
			require.NoError(t, err)
			// Preserve length and mtime so only captured bytes detect drift.
			changed := strings.Replace(original, "Record", "record", 1)
			if changed == original {
				changed = strings.Replace(original, "records", "Records", 1)
			}
			if changed == original {
				changed = strings.Replace(original, "integer", "boolean", 1)
			}
			require.NotEqual(t, original, changed)
			require.Equal(t, len(original), len(changed))
			t.Cleanup(func() { require.NoError(t, os.WriteFile(path, []byte(original), 0600)) })
			require.NoError(t, os.WriteFile(path, []byte(changed), 0600))
			require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
			require.ErrorContains(t, proof.ValidateFiles(identity), "dependency content drift")
			require.NoError(t, os.WriteFile(path, []byte(original), 0600))
			require.NoError(t, proof.ValidateFiles(identity))
			require.NoError(t, os.Remove(path))
			require.Error(t, proof.ValidateFiles(identity), "missing documentation/schema must invalidate publication")
			// Revalidation must use sealed bytes, not reopen the owner directory.
			require.NoError(t, proof.ValidateSchema())
		})
	}
}
