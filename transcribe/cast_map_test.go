package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/castmodel"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestCASTMapFieldsGenerateAndRunHooks(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE audience(id INTEGER, TARGET TEXT)"))
	for _, policy := range []generate.GenerationPolicy{generate.GenerationPolicyMerge, generate.GenerationPolicyOverwrite} {
		t.Run(string(policy), func(t *testing.T) {
			root := t.TempDir()
			const module = "github.com/viant/datly/mapfixture"
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[castmodel.Signals]())))
			source := &Source{Name: "Audience", Scope: module + "/audience", Connector: "main", Types: catalog,
				ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('audience')
#import('signalmodel','github.com/viant/datly/transcribe/testdata/castmodel')
#setting($_ = $route('/audience','GET'))
#setting($_ = $input_type('AudienceInput'))
#setting($_ = $output_type('AudienceOutput'))
#set($_ = $Data<?>(output/view).WithTag('json:"data"'))
SELECT a.*, type(a,'AudienceRow'),
cast(a.id AS int), cast(a.computed AS int),
required(a.TARGET),
cast(a.INCLUSION_MAP AS map[string][]string), required(a.INCLUSION_MAP),
tag(a.INCLUSION_MAP,'sqlx:"-" internal:"true" json:"-"'),
cast(a.EXCLUSION_MAP AS map[string][]string), required(a.EXCLUSION_MAP),
tag(a.EXCLUSION_MAP,'sqlx:"-" internal:"true" json:"-"'),
cast(a.signals AS map[signalmodel.Signals][]*signalmodel.Signals),
tag(a.signals,'sqlx:"-" internal:"true" json:"-"'),
cast(a.nested AS map[string]map[int][]string),
tag(a.nested,'sqlx:"-" internal:"true" json:"-"')
FROM (SELECT id, TARGET, 0 AS computed, 'not JSON' AS INCLUSION_MAP, 'not JSON' AS EXCLUSION_MAP,
 'not JSON' AS signals, 'not JSON' AS nested FROM audience) a`,
			}
			generator := Generator{Operation: "get", GenerationPolicy: policy}
			generated, err := generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
			require.NoError(t, err)
			view := generated.Result.Plan.Views[0]
			target, ok := view.Field("Target")
			require.True(t, ok)
			require.Equal(t, "string", target.Type, "required suppresses discovered nullable pointer inference")
			require.NotContains(t, target.Tag, "required=", "output nullability must not become a SQLX write constraint")
			for _, tc := range []struct{ field, typ string }{
				{"InclusionMap", "map[string][]string"}, {"ExclusionMap", "map[string][]string"},
				{"Signals", "map[signalmodel.Signals][]*signalmodel.Signals"}, {"Nested", "map[string]map[int][]string"},
			} {
				field, ok := view.Field(tc.field)
				require.True(t, ok)
				require.Equal(t, tc.typ, field.Type)
				require.True(t, field.ExplicitType)
				tags := reflect.StructTag(field.Tag)
				require.Equal(t, "-", tags.Get("sqlx"))
				require.Equal(t, "-", tags.Get("json"))
				require.Equal(t, "true", tags.Get("internal"))
			}
			viewsFile := filepath.Join(root, "audience", view.Destination)
			before, err := os.ReadFile(viewsFile)
			require.NoError(t, err)
			for _, name := range []string{"hooks.go", "runtime_test.go"} {
				content, err := os.ReadFile("testdata/map_columns/" + name + ".txt")
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(root, "audience", name), content, 0600))
			}
			_, err = generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
			require.NoError(t, err)
			after, err := os.ReadFile(viewsFile)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
			command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-timeout=90s", "-v", "./...")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off")
			out, err := command.CombinedOutput()
			require.NoError(t, err, "%s", out)
			t.Logf("generated map regression:\n%s", out)
		})
	}
}

func TestFluentMapColumnTypeGeneration(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const module = "github.com/viant/datly/fluentmapfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[castmodel.Signals]())))
	source := &Source{Name: "Maps", Scope: module + "/maps", Types: catalog, Text: `#package('maps')
#import('signalmodel','github.com/viant/datly/transcribe/testdata/castmodel')
#setting($_ = $route('/maps','GET'))
#define($_ = $Lookup<*LookupRow>(view/Lookup).Optional().ColumnType('values','map[string][]*signalmodel.Signals').ColumnTag('values','sqlx:"-" internal:"true" json:"-"') /* SELECT '' AS values */)
SELECT 1 AS id`}
	generator := Generator{Operation: "get"}
	for pass := 0; pass < 2; pass++ {
		generated, err := generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
		found := false
		for _, view := range generated.Result.Plan.Views {
			if view.Name != "LookupRow" {
				continue
			}
			field, ok := view.Field("Values")
			require.True(t, ok)
			require.Equal(t, "map[string][]*signalmodel.Signals", field.Type)
			require.Equal(t, "-", reflect.StructTag(field.Tag).Get("sqlx"))
			found = true
		}
		require.True(t, found)
	}
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
