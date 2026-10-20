package transcribe

import (
	"context"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/linkedcontract"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestOrdinaryExternalReadFreshAndPackageReplay(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"))
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[linkedcontract.ComponentRow]())))
	text := `#package('example.com/foreignread/generated')
#import('foreign','github.com/viant/datly/transcribe/testdata/linkedcontract')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Current<[]*foreign.ComponentRow>(view/Current).Cardinality('Many') /* SELECT id FROM records */)
#define($_ = $Data<[]*Record>(output/view))
SELECT r.*, type(r,'Record') FROM records r`
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/foreignread"}).Write(t, root)
	source := &Source{Scope: "example.com/foreignread/generated", Name: "records", Text: text, Types: catalog, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	var prior *spec.Component
	for pass := 0; pass < 2; pass++ {
		if pass == 1 {
			source.PackageComponent = prior.Clone()
			resources := resource.New()
			require.NoError(t, resources.Register("old", fstest.MapFS{"query.sql": &fstest.MapFile{Data: []byte("SELECT id FROM records")}}))
			source.Resources = resources
			// An equivalent packaged URI must not erase explicit foreign ownership
			// when current authored SQL is inline and the contracts are re-merged.
			source.PackageComponent.Views[0].Source = &spec.ViewSource{URI: "old:query.sql", Table: "records"}
		}
		compiled, err := NewCompiler().Compile(ctx, source)
		require.NoError(t, err)
		prior = compiled.Component
		generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		linked := false
		local := false
		for _, view := range generated.Result.Plan.Views {
			if view.Name == "ComponentRow" {
				require.Equal(t, gen.ViewLinked, view.Ownership)
				require.Equal(t, reflect.TypeFor[linkedcontract.ComponentRow]().PkgPath()+".ComponentRow", view.DescriptorKey)
				linked = true
			}
			if view.Name == "Record" {
				require.Equal(t, gen.ViewGenerated, view.Ownership)
				local = true
			}
		}
		require.True(t, linked)
		require.True(t, local)
	}
}
func TestMergeInputViewReferencesKeepsOwnershipAndRejectsConflicts(t *testing.T) {
	original := gen.ViewReferences{"foreign": {DescriptorKey: "example.com/model.Row"}}
	merged, err := mergeInputViewReferences(original, gen.ViewReferences{"local": {DescriptorKey: "example.com/app.Row"}})
	require.NoError(t, err)
	require.Len(t, merged, 2)
	require.Len(t, original, 1)
	_, err = mergeInputViewReferences(original, gen.ViewReferences{"foreign": {DescriptorKey: "example.com/other.Row"}})
	require.ErrorContains(t, err, "conflicting type ownership")
}

func TestOrdinaryUnpackagedLocalReadRemainsGenerated(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY)"))
	result, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/unpackaged", Name: "records", Types: typecatalog.NewCatalog(), Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#setting($_ = $connector('main'))
#setting($_ = $route('/records','GET'))
#define($_ = $Current<[]*Local>(view/Current).Cardinality('Many') /* SELECT id FROM records */)
SELECT r.*,type(r,'Record') FROM records r`})
	require.NoError(t, err)
	require.Empty(t, result.Views, "local inferred types do not require foreign descriptors")
}
