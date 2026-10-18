package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xhandler "github.com/viant/xdatly/handler"
)

func TestGeneratedOutputInvokerPreservesNativeKindOnReload(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"))
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/outputinvoker"}).Write(t, root)
	source := `#package('example.com/outputinvoker/records')
#import('handler','github.com/viant/xdatly/handler')
#setting($_ = $route('/records','GET'))
#define($_ = $Name<string>(query/name).Optional())
#define($_ = $Invoker<handler.ComponentInvoker>(component_invoker/).Required().Output().WithTag('json:"-"'))
SELECT id FROM records`
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[xhandler.ComponentInvoker]())))
	generate := func() *GeneratedPackage {
		g, err := NewCompiler().Transcribe(ctx, Request{Destination: root, Source: &Source{Types: catalog, Name: "Records", Scope: "example.com/outputinvoker/records", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}})
		require.NoError(t, err)
		return g
	}
	first := generate()
	plan := first.Result.Plan
	for _, field := range plan.Input.Fields {
		require.NotEqual(t, "Invoker", field.Name)
	}
	output, found := plan.Output.Field("Invoker")
	require.True(t, found)
	require.Contains(t, output.Tag, "kind=component_invoker")
	require.Contains(t, output.Tag, `json:"-"`)
	before := map[string][]byte{}
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			body, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			before[path] = body
		}
		return nil
	}))
	project, err := (&Discovery{BaseDir: root, Include: []string{plan.Package}}).Compile(ctx)
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
	found = false
	for _, param := range project.Components[0].Component.Parameters {
		if param.Name == "Invoker" {
			require.Equal(t, "component_invoker", param.Source.Kind)
			require.True(t, param.EmitOutput)
			found = true
		}
	}
	require.True(t, found)
	generate()
	for path, want := range before {
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, body, path)
	}
}
