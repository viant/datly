package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
	"testing"
)

func TestNativeDistinctSourceOrderingPolicyRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,label TEXT,owner_id INTEGER)", "CREATE TABLE owners(id INTEGER PRIMARY KEY,name TEXT)"))
	const sourceSQL = `SELECT DISTINCT (CASE WHEN :Column = 'label' THEN p.label ELSE o.name END) AS VALUE FROM items p LEFT JOIN owners o ON p.owner_id=o.id`
	source := &Source{Name: "Values", Scope: "example.com/values", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('values')
#setting($_ = $route('/values','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Column<string>(query/column).Required())
#define($_ = $OrderBy<string>(query/sort).QuerySelector('records'))
#define($_ = $Data<[]*Value>(output/view))
SELECT records.*, type(records,'Value'), CAST(records.VALUE AS *string), allow_nulls(records),
 order_by(records,'p.id DESC'), allowed_order_by_columns(records,'p.id,owner:o.name')
FROM (` + sourceSQL + `) records`}
	compiled, e := NewCompiler().Compile(ctx, source)
	require.NoError(t, e)
	dir := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/values"}).Write(t, dir)
	native, e := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: dir})
	require.NoError(t, e)
	again, e := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: dir})
	require.NoError(t, e)
	require.Equal(t, native.Result.Files, again.Result.Files)
	require.Len(t, native.Result.Plan.Views[0].Fields, 1)
	require.Equal(t, "*string", native.Result.Plan.Views[0].Fields[0].Type)
	require.Contains(t, compiled.Component.RootView.Source.SQL, sourceSQL)
	gen := generate.New(generate.Input{Component: compiled.Component})
	input, e := gen.RuntimeInputType()
	require.NoError(t, e)
	output, e := gen.RuntimeOutputType()
	require.NoError(t, e)
	artifact, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: input, OutputType: output, DirectViewField: "Data"})
	require.NoError(t, e)
	check := func(policy *spec.Selector) {
		require.NotNil(t, policy)

		require.Contains(t, policy.Orderable, spec.FieldPath("p.id"))
		require.Equal(t, spec.FieldPath("o.name"), policy.OrderAliases["owner"])
	}
	require.Equal(t, "p.id DESC", compiled.Component.RootView.Source.Controls.OrderBy)
	check(compiled.Component.RootView.Selector)
	require.Equal(t, "p.id DESC", artifact.Reader.Root.View.Spec.Source.Controls.OrderBy)
	check(artifact.Reader.Root.View.Spec.Selector)
}
