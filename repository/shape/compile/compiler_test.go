package compile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
)

func TestDQLCompiler_Compile(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: "SELECT id FROM ORDERS t"})
	require.NoError(t, err)
	require.NotNil(t, res)

	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 1)
	view := planned.Views[0]
	assert.Equal(t, "t", view.Name)
	assert.Equal(t, "ORDERS", view.Table)
	assert.Equal(t, "many", view.Cardinality)
	require.NotNil(t, view.FieldType)
	assert.Contains(t, view.FieldType.String(), "Id")
}

func TestDQLCompiler_Compile_EmptyDQL(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "x"})
	require.Error(t, err)
	assert.ErrorIs(t, err, shape.ErrNilDQL)
}

func TestDQLCompiler_Compile_WithPreamble_NoPanic(t *testing.T) {
	compiler := New()
	dql := `
/* metadata */
#set($_ = $A<string>(query/a).Optional())
SELECT id
`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "sample_report", DQL: dql})
	require.NoError(t, err)
	require.NotNil(t, res)

	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 1)
	assert.Equal(t, "sample_report", planned.Views[0].Name)
	assert.Equal(t, "sample_report", planned.Views[0].Table)
}

func TestDQLCompiler_Compile_PropagatesTypeContext(t *testing.T) {
	compiler := New()
	dql := `
#settings($_ = $package('mdp/performance'))
#settings($_ = $import('perf', 'github.com/acme/mdp/performance'))
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	require.NotNil(t, res)

	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotNil(t, planned.TypeContext)
	assert.Equal(t, "mdp/performance", planned.TypeContext.DefaultPackage)
	require.Len(t, planned.TypeContext.Imports, 1)
	assert.Equal(t, "perf", planned.TypeContext.Imports[0].Alias)
}

func TestDQLCompiler_Compile_PropagatesSpecialDirectives(t *testing.T) {
	compiler := New()
	dql := `
#settings($_ = $meta('docs/orders.md'))
#settings($_ = $connector('analytics'))
#settings($_ = $cache(true, '5m'))
#settings($_ = $mcp('orders.search', 'Search orders', 'docs/mcp/orders.md'))
SELECT id FROM ORDERS o
`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotNil(t, planned.Directives)
	assert.Equal(t, "docs/orders.md", planned.Directives.Meta)
	assert.Equal(t, "analytics", planned.Directives.DefaultConnector)
	require.NotNil(t, planned.Directives.Cache)
	assert.True(t, planned.Directives.Cache.Enabled)
	assert.Equal(t, "5m", planned.Directives.Cache.TTL)
	require.NotNil(t, planned.Directives.MCP)
	assert.Equal(t, "orders.search", planned.Directives.MCP.Name)
	assert.Equal(t, "Search orders", planned.Directives.MCP.Description)
	assert.Equal(t, "docs/mcp/orders.md", planned.Directives.MCP.DescriptionPath)
	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "analytics", planned.Views[0].Connector)
}

func TestDQLCompiler_Compile_ColumnDiscoveryAutoForWildcard(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: "SELECT * FROM ORDERS o"})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.True(t, planned.ColumnsDiscovery)
	require.NotEmpty(t, planned.Views)
	assert.True(t, planned.Views[0].ColumnsDiscovery)
}

func TestDQLCompiler_Compile_ColumnDiscoveryOffFailsWhenRequired(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: "SELECT * FROM ORDERS o"},
		shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryOff))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeColDiscoveryReq, compileErr.Diagnostics[0].Code)
}

func TestDQLCompiler_Compile_TypeContextValidationWarnsInCompat(t *testing.T) {
	compiler := New()
	dql := `
#settings($_ = $package('github.com/acme/perf'))
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql}, shape.WithTypeContextPackageName("bad/name"))
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeTypeCtxInvalid, planned.Diagnostics[0].Code)
	assert.Equal(t, dqlshape.SeverityWarning, planned.Diagnostics[0].Severity)
}

func TestDQLCompiler_Compile_TypeContextValidationFailsInStrict(t *testing.T) {
	compiler := New()
	dql := `SELECT id FROM ORDERS t`
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql},
		shape.WithCompileProfile(shape.CompileProfileStrict),
		shape.WithTypeContextPackageName("bad/name"))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeTypeCtxInvalid, compileErr.Diagnostics[0].Code)
	assert.Equal(t, dqlshape.SeverityError, compileErr.Diagnostics[0].Severity)
}

func TestDQLCompiler_Compile_SyntaxError_HasLineAndChar(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: "SELECT id FROM ORDERS WHERE ("})
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	d := compileErr.Diagnostics[0]
	assert.Equal(t, dqldiag.CodeParseSyntax, d.Code)
	assert.Equal(t, 1, d.Span.Start.Line)
	assert.Equal(t, 29, d.Span.Start.Char)
}

func TestDQLCompiler_Compile_SyntaxError_RemapsAfterSanitize(t *testing.T) {
	compiler := New()
	dql := "SELECT id FROM ORDERS t WHERE t.id = $Id AND ("
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	var diagnostics []*dqlshape.Diagnostic
	if err != nil {
		compileErr, ok := err.(*CompileError)
		require.True(t, ok)
		require.NotEmpty(t, compileErr.Diagnostics)
		diagnostics = compileErr.Diagnostics
	} else {
		planned, ok := res.Plan.(*plan.Result)
		require.True(t, ok)
		diagnostics = planned.Diagnostics
	}
	var d *dqlshape.Diagnostic
	for _, item := range diagnostics {
		if item != nil && item.Code == dqldiag.CodeParseSyntax {
			d = item
			break
		}
	}
	if d != nil {
		assert.Equal(t, 1, d.Span.Start.Line)
		assert.Greater(t, d.Span.Start.Char, 0)
		assert.LessOrEqual(t, d.Span.Start.Char, len(dql))
	}
}

func TestDQLCompiler_Compile_DirectiveOnly_HasLineAndChar(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: "#settings($_ = $package('x'))"})
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	d := compileErr.Diagnostics[0]
	assert.Equal(t, dqldiag.CodeParseEmpty, d.Code)
	assert.Equal(t, 1, d.Span.Start.Line)
	assert.Equal(t, 1, d.Span.Start.Char)
}

func TestDQLCompiler_Compile_InvalidDirective_HasLineAndChar(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "orders_report",
		DQL:  "SELECT id FROM ORDERS t\n#settings($_ = $import('alias'))\nSELECT id FROM ORDERS t",
	})
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	d := compileErr.Diagnostics[0]
	assert.Equal(t, dqldiag.CodeDirImport, d.Code)
	assert.Equal(t, 2, d.Span.Start.Line)
	assert.Equal(t, 1, d.Span.Start.Char)
}

func TestDQLCompiler_Compile_ExtractsJoinLinks(t *testing.T) {
	compiler := New()
	dql := "SELECT o.id, i.sku FROM orders o JOIN order_items i ON o.id = i.order_id"
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	root := planned.ViewsByName["o"]
	require.NotNil(t, root)
	require.Len(t, root.Relations, 1)
	assert.Equal(t, "i", root.Relations[0].Ref)
	require.Len(t, root.Relations[0].On, 1)
	assert.Equal(t, "o.id=i.order_id", root.Relations[0].On[0].Expression)
	assert.Equal(t, "id", root.Relations[0].On[0].ParentColumn)
	assert.Equal(t, "order_id", root.Relations[0].On[0].RefColumn)
	assert.Empty(t, planned.Diagnostics)
}

func TestDQLCompiler_Compile_JoinDiagnostics(t *testing.T) {
	compiler := New()
	dql := "SELECT o.id FROM orders o JOIN order_items i ON o.id > i.order_id"
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeRelUnsupported, planned.Diagnostics[0].Code)
}

func TestDQLCompiler_Compile_StrictRelationWarningsFail(t *testing.T) {
	compiler := New()
	dql := "SELECT o.id FROM orders o JOIN order_items i ON o.id > i.order_id"
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql}, shape.WithCompileStrict(true))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeRelUnsupported, compileErr.Diagnostics[0].Code)
}

func TestDQLCompiler_Compile_ProfileStrictRelationWarningsFail(t *testing.T) {
	compiler := New()
	dql := "SELECT o.id FROM orders o JOIN order_items i ON o.id > i.order_id"
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql}, shape.WithCompileProfile(shape.CompileProfileStrict))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeRelUnsupported, compileErr.Diagnostics[0].Code)
}

func TestDQLCompiler_Compile_StrictAmbiguousLinkFail(t *testing.T) {
	compiler := New()
	dql := "SELECT o.id FROM orders o JOIN order_items i ON x.id = y.order_id"
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql}, shape.WithCompileStrict(true))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeRelAmbiguous, compileErr.Diagnostics[0].Code)
}

func TestDQLCompiler_Compile_SQLInjectionDiagnostic(t *testing.T) {
	compiler := New()
	dql := "SELECT id FROM ORDERS t WHERE t.id = $Unsafe.Id"
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeSQLIRawSelector, planned.Diagnostics[0].Code)
	assert.Equal(t, 1, planned.Diagnostics[0].Span.Start.Line)
	assert.Greater(t, planned.Diagnostics[0].Span.Start.Char, 1)
}

func TestDQLCompiler_Compile_SanitizesBindings(t *testing.T) {
	compiler := New()
	dql := "SELECT id FROM ORDERS t WHERE t.id = $Id"
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Views)
	assert.Contains(t, planned.Views[0].SQL, "$criteria.AppendBinding($Unsafe.Id)")
}

func TestDQLCompiler_Compile_ParameterDerivedView(t *testing.T) {
	compiler := New()
	dql := `
#set($_ = $Extra<?>(view/extra_view) /* SELECT code FROM EXTRA e */)
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 2)
	extra := planned.ViewsByName["e"]
	require.NotNil(t, extra)
	assert.Equal(t, "EXTRA", extra.Table)
	assert.Contains(t, extra.SQL, "SELECT code FROM EXTRA e")
}

func TestDQLCompiler_Compile_ParameterDerivedView_Options(t *testing.T) {
	compiler := New()
	dql := `
#set($_ = $Extra<?>(view/extra_view).WithURI('/v1/extra').WithConnector('analytics').Cardinality('one') /* SELECT code FROM EXTRA e */)
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	extra := planned.ViewsByName["e"]
	require.NotNil(t, extra)
	assert.Equal(t, "/v1/extra", extra.SQLURI)
	assert.Equal(t, "analytics", extra.Connector)
	assert.Equal(t, "one", extra.Cardinality)
}

func TestDQLCompiler_Compile_ParameterDerivedView_MissingSQLHint(t *testing.T) {
	compiler := New()
	dql := `
#set($_ = $Extra<?>(view/extra_view))
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeViewMissingSQL, planned.Diagnostics[len(planned.Diagnostics)-1].Code)
}

func TestDQLCompiler_Compile_ParameterDerivedView_InvalidCardinalityDiagnostic(t *testing.T) {
	compiler := New()
	dql := `
#set($_ = $Extra<?>(view/extra_view).Cardinality('few') /* SELECT code FROM EXTRA e */)
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeViewCardinality, planned.Diagnostics[len(planned.Diagnostics)-1].Code)
}

func TestDQLCompiler_Compile_StrictSQLInjectionWarningsFail(t *testing.T) {
	compiler := New()
	dql := "SELECT id FROM ORDERS t WHERE t.id = $Unsafe.Id"
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql}, shape.WithCompileStrict(true))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeSQLIRawSelector, compileErr.Diagnostics[0].Code)
}

func TestDQLCompiler_Compile_DMLInsert(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "orders_exec",
		DQL:  "INSERT INTO ORDERS(id) VALUES (1)",
	})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 1)
	assert.Equal(t, "ORDERS", planned.Views[0].Table)
	assert.Equal(t, "many", planned.Views[0].Cardinality)
}

func TestDQLCompiler_Compile_DMLServiceMissingArg(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "orders_exec",
		DQL:  "$sql.Insert($rec)",
	})
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	var target *dqlshape.Diagnostic
	for _, item := range compileErr.Diagnostics {
		if item != nil && item.Code == dqldiag.CodeDMLServiceArg {
			target = item
			break
		}
	}
	require.NotNil(t, target)
	assert.Equal(t, 1, target.Span.Start.Line)
	assert.Equal(t, 1, target.Span.Start.Char)
}

func TestDQLCompiler_Compile_DMLSyntaxError_HasLineAndChar(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "orders_exec",
		DQL:  "#settings($_ = $package('x'))\nINSERT INTO ORDERS(id VALUES (1)",
	})
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	var target *dqlshape.Diagnostic
	for _, item := range compileErr.Diagnostics {
		if item != nil && item.Code == dqldiag.CodeDMLInsert {
			target = item
			break
		}
	}
	require.NotNil(t, target)
	assert.Equal(t, 2, target.Span.Start.Line)
	assert.Equal(t, 1, target.Span.Start.Char)
}

func TestDQLCompiler_Compile_MixedReadExec_Warning(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "mixed_exec",
		DQL:  "SELECT id FROM ORDERS\nUPDATE ORDERS SET id = 2",
	})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeDMLMixed, planned.Diagnostics[len(planned.Diagnostics)-1].Code)
}

func TestDQLCompiler_Compile_MixedMode_ExecWins(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "mixed_exec",
		DQL:  "SELECT o.id FROM ORDERS o\nUPDATE ORDERS SET id = 2",
	}, shape.WithMixedMode(shape.CompileMixedModeExecWins))
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "ORDERS", planned.Views[0].Table)
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeDMLMixed, planned.Diagnostics[len(planned.Diagnostics)-1].Code)
}

func TestDQLCompiler_Compile_MixedMode_ReadWins(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "mixed_exec",
		DQL:  "SELECT o.id FROM ORDERS o\nUPDATE ORDERS SET id = 2",
	}, shape.WithMixedMode(shape.CompileMixedModeReadWins))
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "o", planned.Views[0].Name)
	assert.Equal(t, "ORDERS", planned.Views[0].Table)
	assert.Contains(t, planned.Views[0].SQL, "SELECT o.id FROM ORDERS o")
	assert.NotContains(t, planned.Views[0].SQL, "UPDATE ORDERS")
	require.NotEmpty(t, planned.Diagnostics)
	assert.Equal(t, dqldiag.CodeDMLMixed, planned.Diagnostics[len(planned.Diagnostics)-1].Code)
}

func TestDQLCompiler_Compile_MixedMode_ErrorOnMixed(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "mixed_exec",
		DQL:  "SELECT o.id FROM ORDERS o\nUPDATE ORDERS SET id = 2",
	}, shape.WithMixedMode(shape.CompileMixedModeErrorOnMixed))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	assert.Equal(t, dqldiag.CodeDMLMixed, compileErr.Diagnostics[0].Code)
	assert.Equal(t, dqlshape.SeverityError, compileErr.Diagnostics[0].Severity)
}

func TestDQLCompiler_Compile_UnknownNonRead_Warn(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "orders_report",
		DQL:  "$Foo.Bar($x)",
	})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Diagnostics)
	var found *dqlshape.Diagnostic
	for _, item := range planned.Diagnostics {
		if item != nil && item.Code == dqldiag.CodeParseUnknownNonRead {
			found = item
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, dqlshape.SeverityWarning, found.Severity)
	require.NotEmpty(t, planned.Views)
}

func TestDQLCompiler_Compile_UnknownNonRead_ErrorMode(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "orders_report",
		DQL:  "$Foo.Bar($x)",
	}, shape.WithUnknownNonReadMode(shape.CompileUnknownNonReadError))
	require.Error(t, err)
	compileErr, ok := err.(*CompileError)
	require.True(t, ok)
	require.NotEmpty(t, compileErr.Diagnostics)
	var found *dqlshape.Diagnostic
	for _, item := range compileErr.Diagnostics {
		if item != nil && item.Code == dqldiag.CodeParseUnknownNonRead {
			found = item
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, dqlshape.SeverityError, found.Severity)
}

func TestResolveGeneratedCompanionDQL(t *testing.T) {
	tempDir := t.TempDir()
	dqlPath := filepath.Join(tempDir, "platform", "sitelist", "patch.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(dqlPath), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(filepath.Dir(dqlPath), "gen"), 0o755))
	generatedPath := filepath.Join(filepath.Dir(dqlPath), "gen", "patch.sql")
	require.NoError(t, os.WriteFile(generatedPath, []byte("SELECT id FROM SITE_LIST sl"), 0o644))
	source := &shape.Source{
		Path: dqlPath,
		DQL:  `/* {"Type":"sitelist/patch.Handler"} */`,
	}
	actual := resolveGeneratedCompanionDQL(source)
	require.Contains(t, actual, "SELECT id FROM SITE_LIST")
}

func TestDQLCompiler_Compile_UnknownNonRead_UsesGeneratedCompanion(t *testing.T) {
	tempDir := t.TempDir()
	dqlPath := filepath.Join(tempDir, "platform", "adorder", "patch.dql")
	require.NoError(t, os.MkdirAll(filepath.Join(filepath.Dir(dqlPath), "gen", "adorder"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(dqlPath), "gen", "adorder", "patch.dql"), []byte("SELECT o.id FROM ORDERS o JOIN ORDER_ITEM i ON i.ORDER_ID = o.ID"), 0o644))
	source := &shape.Source{
		Name: "patch",
		Path: dqlPath,
		DQL:  `/* {"Type":"adorder/patch.Handler"} */`,
	}

	compiler := New()
	res, err := compiler.Compile(context.Background(), source)
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotNil(t, planned.ViewsByName["o"])
	require.NotNil(t, planned.ViewsByName["i"])
	var hasUnknownNonRead bool
	for _, diag := range planned.Diagnostics {
		if diag != nil && diag.Code == dqldiag.CodeParseUnknownNonRead {
			hasUnknownNonRead = true
			break
		}
	}
	assert.False(t, hasUnknownNonRead)
}

func TestResolveLegacyRouteViews(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "dql", "platform", "campaign", "patch.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(sourcePath), 0o755))
	require.NoError(t, os.WriteFile(sourcePath, []byte(`/* {"Connector":"ci_ads"} */`), 0o644))

	routeDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "platform", "campaign", "patch")
	require.NoError(t, os.MkdirAll(routeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(routeDir, "patch.sql"), []byte(`SELECT 1`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routeDir, "CurCampaign.sql"), []byte(`SELECT * FROM CI_CAMPAIGN`), 0o644))

	views := resolveLegacyRouteViews(&shape.Source{Path: sourcePath, DQL: `/* {"Connector":"ci_ads"} */`})
	require.Len(t, views, 2)
	assert.Equal(t, "patch", views[0].Name)
	assert.Equal(t, "", views[0].Table)
	assert.Equal(t, "patch/patch.sql", views[0].SQLURI)
	assert.Equal(t, "CurCampaign", views[1].Name)
	assert.Equal(t, "CI_CAMPAIGN", views[1].Table)
	assert.Equal(t, "ci_ads", views[1].Connector)
}

func TestResolveLegacyRouteViews_TypeStemSubfolder(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "dql", "platform", "campaign", "post.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(sourcePath), 0o755))
	require.NoError(t, os.WriteFile(sourcePath, []byte(`/* {"Type":"campaign/patch.Handler","Connector":"ci_ads"} */`), 0o644))

	routeDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "platform", "campaign", "patch", "post")
	require.NoError(t, os.MkdirAll(routeDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(routeDir, "post.sql"), []byte(`SELECT 1`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routeDir, "CurCampaign.sql"), []byte(`SELECT * FROM CI_CAMPAIGN`), 0o644))

	views := resolveLegacyRouteViews(&shape.Source{Path: sourcePath, DQL: `/* {"Type":"campaign/patch.Handler","Connector":"ci_ads"} */`})
	require.Len(t, views, 2)
	assert.Equal(t, "post", views[0].Name)
	assert.Equal(t, "CurCampaign", views[1].Name)
	assert.Equal(t, "post/CurCampaign.sql", views[1].SQLURI)
}

func TestDQLCompiler_Compile_HandlerNop_NoSQLiEscalation(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "handler_nop",
		DQL:  "$Nop($Unsafe.Id)",
	}, shape.WithCompileStrict(true))
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	for _, item := range planned.Diagnostics {
		if item == nil {
			continue
		}
		assert.NotEqual(t, dqldiag.CodeSQLIRawSelector, item.Code)
	}
}

func TestDQLCompiler_Compile_SubqueryJoin_BuildsRelatedViewsAndConnectorHints(t *testing.T) {
	compiler := New()
	dql := `
#set($_ = $Jwt<string>(header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
SELECT session.*,
use_connector(session, system),
use_connector(attribute, system)
FROM (SELECT * FROM session WHERE user_id = $Jwt.UserID) session
JOIN (SELECT * FROM session/attributes) attribute ON attribute.user_id = session.user_id
`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "system/session", DQL: dql})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	root := planned.ViewsByName["session"]
	require.NotNil(t, root)
	assert.Equal(t, "system", root.Connector)
	related := planned.ViewsByName["attribute"]
	require.NotNil(t, related)
	assert.Equal(t, "session/attributes", related.Table)
	assert.Equal(t, "system", related.Connector)
}

func TestDQLCompiler_Compile_GeneratedHandler_NoBodyInput_UsesLegacyContractStates(t *testing.T) {
	tempDir := t.TempDir()
	genPath := filepath.Join(tempDir, "dql", "system", "upload", "gen", "upload", "delete.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(genPath), 0o755))
	require.NoError(t, os.WriteFile(genPath, []byte(`/* {"Method":"DELETE","URI":"/v1/api/system/upload"} */`), 0o644))

	legacySQLPath := filepath.Join(tempDir, "dql", "system", "upload", "delete.sql")
	require.NoError(t, os.MkdirAll(filepath.Dir(legacySQLPath), 0o755))
	require.NoError(t, os.WriteFile(legacySQLPath, []byte(`/* {"Type":"upload/delete.Handler","Connector":"system"} */`), 0o644))

	routesDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "system", "upload")
	require.NoError(t, os.MkdirAll(filepath.Join(routesDir, "delete"), 0o755))
	routeYAML := `Resource:
  Parameters:
    - Name: Method
      In:
        Kind: http_request
        Name: method
    - Name: UploadId
      In:
        Kind: query
        Name: uploadId
  Views:
    - Name: delete
      Mode: SQLExec
      Connector:
        Ref: system
      Template:
        SourceURL: delete/delete.sql
`
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "delete.yaml"), []byte(routeYAML), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "delete", "delete.sql"), []byte(`$Nop($Unsafe.UploadId)`), 0o644))

	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "delete", Path: genPath, DQL: `/* {"Method":"DELETE","URI":"/v1/api/system/upload"} */`})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)

	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "delete", planned.Views[0].Name)
	assert.Equal(t, "SQLExec", planned.Views[0].Mode)
	assert.Equal(t, "system", planned.Views[0].Connector)

	stateByName := map[string]*plan.State{}
	for _, item := range planned.States {
		if item == nil {
			continue
		}
		stateByName[item.Name] = item
	}
	require.Contains(t, stateByName, "Method")
	require.Contains(t, stateByName, "UploadId")
	assert.Equal(t, "http_request", stateByName["Method"].Kind)
	assert.Equal(t, "query", stateByName["UploadId"].Kind)
	assert.NotContains(t, stateByName, "Body")
}

func TestDQLCompiler_Compile_HandlerLegacyTypes_PreferredOverComponentNameCollisions(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "dql", "platform", "campaign", "post.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(sourcePath), 0o755))
	require.NoError(t, os.WriteFile(sourcePath, []byte(`/* {"URI":"/v1/api/platform/campaign","Method":"POST","Type":"campaign/patch.Handler"} */`), 0o644))

	rootRouteDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "platform", "campaign", "patch")
	require.NoError(t, os.MkdirAll(filepath.Join(rootRouteDir, "post"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(rootRouteDir, "post.yaml"), []byte(`Resource:
  Parameters:
    - Name: Auth
      In:
        Kind: component
        Name: GET:/v1/api/platform/acl/auth
  Views:
    - Name: post
      Mode: SQLExec
      Connector:
        Ref: ci_ads
      Template:
        SourceURL: post/post.sql
  Types:
    - Name: Input
      DataType: "*Input"
      Package: campaign/patch
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/campaign/patch
    - Name: Handler
      DataType: "*Handler"
      Package: campaign/patch
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/campaign/patch
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(rootRouteDir, "post", "post.sql"), []byte(`$Nop($Unsafe.Id)`), 0o644))

	componentRouteDir := filepath.Join(tempDir, "repo", "dev", "Datly", "routes", "platform", "acl", "auth")
	require.NoError(t, os.MkdirAll(componentRouteDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(componentRouteDir, "auth.yaml"), []byte(`Resource:
  Types:
    - Name: Input
      DataType: "*Input"
      Package: acl/auth
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/acl/auth
    - Name: Handler
      DataType: "*Handler"
      Package: acl/auth
      ModulePath: github.vianttech.com/viant/platform/pkg/platform/acl/auth
`), 0o644))

	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "post",
		Path: sourcePath,
		DQL:  `/* {"URI":"/v1/api/platform/campaign","Method":"POST","Type":"campaign/patch.Handler"} */`,
	})
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)

	typeByName := map[string]*plan.Type{}
	for _, item := range planned.Types {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		typeByName[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}

	inputType, ok := typeByName["input"]
	require.True(t, ok)
	assert.Equal(t, "campaign/patch", inputType.Package)
	assert.Equal(t, "github.vianttech.com/viant/platform/pkg/platform/campaign/patch", inputType.ModulePath)

	handlerType, ok := typeByName["handler"]
	require.True(t, ok)
	assert.Equal(t, "campaign/patch", handlerType.Package)
	assert.Equal(t, "github.vianttech.com/viant/platform/pkg/platform/campaign/patch", handlerType.ModulePath)
}

func TestDQLCompiler_Compile_CustomPathLayout_HandlerFallback(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "sqlsrc", "platform", "campaign", "post.dql")
	require.NoError(t, os.MkdirAll(filepath.Dir(sourcePath), 0o755))
	require.NoError(t, os.WriteFile(sourcePath, []byte(`/* {"URI":"/v1/api/platform/campaign","Method":"POST","Type":"campaign/patch.Handler","Connector":"ci_ads"} */`), 0o644))

	routesDir := filepath.Join(tempDir, "config", "routes", "platform", "campaign", "patch")
	require.NoError(t, os.MkdirAll(filepath.Join(routesDir, "post"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "post.yaml"), []byte(`Resource:
  Views:
    - Name: post
      Mode: SQLExec
      Connector:
        Ref: ci_ads
      Template:
        SourceURL: post/post.sql
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(routesDir, "post", "post.sql"), []byte(`$Nop($Unsafe.Id)`), 0o644))

	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{
		Name: "post",
		Path: sourcePath,
		DQL:  `/* {"URI":"/v1/api/platform/campaign","Method":"POST","Type":"campaign/patch.Handler","Connector":"ci_ads"} */`,
	}, shape.WithDQLPathMarker("sqlsrc"), shape.WithRoutesRelativePath("config/routes"))
	require.NoError(t, err)
	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "post", planned.Views[0].Name)
	assert.Equal(t, "SQLExec", planned.Views[0].Mode)
	assert.Equal(t, "ci_ads", planned.Views[0].Connector)
	assert.Contains(t, planned.Views[0].SQL, "$Nop(")
}
