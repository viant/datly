package transcribe

import (
	"context"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestNativeQualifiedRelatedRowsRegenerateFromSQL(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	const pkg = "example.com/related/generated"
	(testharness.GeneratedModule{Path: "example.com/related"}).Write(t, base)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER PRIMARY KEY, first_id INTEGER, second_id INTEGER)",
		"CREATE TABLE first_children(id INTEGER PRIMARY KEY, caption TEXT)",
		"CREATE TABLE second_children(id INTEGER PRIMARY KEY, label TEXT)"))
	text := `#package('` + pkg + `')
#import('self','` + pkg + `')
#setting($_ = $route('/parents','GET'))
#setting($_ = $dest('rows.go'))
#define($_ = $Data<[]*self.Parent>(output/view))
SELECT p.*,a.*,b.*,type(p,'self.Parent'),type(a,'self.First'),type(b,'self.Second')
FROM parents p LEFT JOIN first_children a ON a.id=p.first_id AND 1=1
LEFT JOIN second_children b ON b.id=p.second_id AND 1=1`
	generate := func() *GeneratedPackage {
		compiled, err := (&Discovery{BaseDir: base}).CompileSource(ctx, &Source{Scope: "example.com/related/dql", Name: "Parents", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})})
		require.NoError(t, err)
		generated, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: base})
		require.NoError(t, err)
		for _, name := range []string{"Parent", "First", "Second"} {
			row := generated.Result.Plan.ViewByType(name)
			require.NotNil(t, row)
			require.Equal(t, gen.ViewGenerated, row.Ownership, name)
		}
		for _, file := range generated.Result.Files {
			require.NotContains(t, file.Content, `self "`+pkg+`"`)
		}
		return generated
	}
	first := generate()
	stable := generate()
	require.Equal(t, first.Result.Files, stable.Result.Files, "actual generated source must retain SQL ownership")
	require.NoError(t, db.ExecStatements(ctx, "ALTER TABLE first_children ADD COLUMN extra TEXT"))
	updated := generate()
	_, found := updated.Result.Plan.ViewByType("First").Field("Extra")
	require.True(t, found, "schema evolution regenerates the child without Go edits")
	require.Equal(t, first.Result.Plan.ViewByType("Second"), updated.Result.Plan.ViewByType("Second"))
	text = strings.Replace(text, "type(a,'self.First')", "type(a,'self.First'),CAST(a.caption AS *string)", 1)
	pointer := generate()
	field, found := pointer.Result.Plan.ViewByType("First").Field("Caption")
	require.True(t, found)
	require.Equal(t, "*string", field.Type)
	command := exec.Command("go", "test", "-mod=mod", "./generated")
	command.Dir = base
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestQualifiedRelatedRowOwnershipBoundaries(t *testing.T) {
	const pkg = "example.com/related/generated"
	for _, tc := range []struct {
		name                                string
		origin                              typecatalog.TypeOrigin
		foreign, ambiguous, mismatch, prior bool
	}{
		{name: "generated", origin: typecatalog.TypeOriginGenerated},
		{name: "authored", origin: typecatalog.TypeOriginPackage},
		{name: "foreign generated", origin: typecatalog.TypeOriginGenerated, foreign: true},
		{name: "ambiguous owner", origin: typecatalog.TypeOriginGenerated, ambiguous: true},
		{name: "different SQL owner", origin: typecatalog.TypeOriginGenerated, mismatch: true},
		{name: "borrowed prior", origin: typecatalog.TypeOriginGenerated, prior: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := pkg
			if tc.foreign {
				path = "example.com/foreign"
			}
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.Register(tc.origin, x.NewType(reflect.TypeFor[struct{ Id int }](), x.WithPkgPath(path), x.WithName("Child"))))
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: pkg})
			require.NoError(t, err)
			child := &spec.View{Name: "child", TypeName: path + ".Child"}
			identity, err := child.Identity()
			require.NoError(t, err)
			owners := []*spec.View{child}
			if tc.ambiguous {
				owners = append(owners, child)
			}
			if tc.mismatch {
				owners = []*spec.View{{Name: "other", TypeName: path + ".Child"}}
			}
			authority := &generatedRowAuthority{plan: &gen.Plan{Package: pkg}, owners: map[string][]*spec.View{pkg + ".Child": owners}}
			result := &Result{Component: &spec.Component{RootView: &spec.View{Name: "parent", Relations: []*spec.Relation{{View: child}}}}, TypeResolver: resolver, Views: gen.ViewReferences{}}
			prior := &gen.ViewReference{DescriptorKey: path + ".Child", Borrowed: &gen.BorrowedRowAuthority{OwnerSource: "owner.dql", BorrowerSource: "borrower.dql"}}
			if tc.prior {
				result.Views[identity] = prior
			}
			require.NoError(t, linkDeclaredRelatedViews(result, authority))
			if tc.name == "generated" {
				require.Nil(t, result.Views[identity])
			} else {
				require.NotNil(t, result.Views[identity])
			}
			if tc.prior {
				require.Same(t, prior, result.Views[identity])
				require.Same(t, prior.Borrowed, result.Views[identity].Borrowed)
			}
		})
	}
}
