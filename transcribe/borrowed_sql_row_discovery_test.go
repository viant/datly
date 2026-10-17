package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestDiscoveryBorrowedSQLRowRetainsConsumedSnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const module = "github.com/viant/datly/borrowedkeyfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	path := filepath.Join(root, "exclusive-boundary.db")
	db := sqlite.New(t, sqlite.WithDSN(path))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), path)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"))
	r := column.New(column.Connections{"main": db.DB})
	sourceDir := filepath.Join(root, "source")
	privateDir := filepath.Join(sourceDir, "private")
	require.NoError(t, os.MkdirAll(privateDir, 0755))
	owner := `#package('` + module + `/api')
#setting($_ = $route('/private','PATCH'))
#setting($_ = $file_prefix('private_'))
#setting($_ = $input_type('PrivateInput'))
#setting($_ = $output_type('PrivateOutput'))
#define($_ = $Rows<[]*Row>(body/data).Cardinality('Many').Required())
SELECT Records.*,type(Records,'Row') FROM records Records`
	borrower := `#package('` + module + `/api')
#import('api','` + module + `/api')
#setting($_ = $route('/public','PATCH'))
#setting($_ = $file_prefix('public_'))
#setting($_ = $input_type('PublicInput'))
#setting($_ = $output_type('PublicOutput'))
#setting($_ = $borrow_sql_row('Rows/Records','` + module + `/api','Row','private/Private.dql','Private','Rows'))
#define($_ = $Rows<[]*PublicView>(body/data).Cardinality('Many').Required())
SELECT p.*,Records.*,type(p,'PublicView'),type(Records,'Row') FROM (parents) p JOIN (records) Records ON Records.parent_id=p.id`
	makeSource := func(scope, name, path, text string) *Source {
		require.NoError(t, os.WriteFile(path, []byte(text), 0600))
		return &Source{Scope: scope, Name: name, Path: path, Text: text, Connector: "main", ColumnRefiner: r, Types: typecatalog.NewCatalog()}
	}
	ownerSource := makeSource(module+"/source/private", "Private", filepath.Join(privateDir, "Private.dql"), owner)
	_, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: ownerSource, Destination: root})
	require.NoError(t, err)
	makeSource(module+"/source", "Public", filepath.Join(sourceDir, "Public.dql"), borrower)
	project, err := (&Discovery{BaseDir: root, Include: []string{module + "/source"}, SourceFile: "Public.dql", Connector: "main", ColumnRefiner: r}).Compile(ctx)
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
	compiled := project.Components[0]
	require.NotNil(t, compiled.Source.dependencySnapshot)
	require.NotEmpty(t, compiled.Source.dependencySnapshot.Packages)
	_, err = compiled.Source.dependencySnapshot.Select(ctx, module+"/api", sourceDir)
	require.ErrorContains(t, err, "dependency build selection directory mismatch")
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
	require.NoError(t, err)
	require.Len(t, generated.Result.Plan.BorrowedRows, 1)
}
