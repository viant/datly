package preprocess

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
)

func TestPrepare_TypeContext(t *testing.T) {
	dql := "#package('a/b')\n#import('x','github.com/acme/x')\nSELECT id FROM t"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotNil(t, pre.TypeCtx)
	assert.Equal(t, "a/b", pre.TypeCtx.DefaultPackage)
	require.Len(t, pre.TypeCtx.Imports, 1)
	assert.Equal(t, "x", pre.TypeCtx.Imports[0].Alias)
}

func TestPrepare_InvalidDirectiveDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#import('x')"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirImport, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
	assert.Equal(t, 1, pre.Diagnostics[0].Span.Start.Char)
}

func TestMapper_MapOffset_WithSanitizeExpansion(t *testing.T) {
	dql := "SELECT id FROM ORDERS t WHERE t.id = $Id AND ("
	pre := Prepare(dql)
	require.NotNil(t, pre.Mapper)
	// Syntax error location after sanitize rewrite should map back to original source.
	offset := len(pre.SQL) - 1
	pos := pre.Mapper.Position(offset)
	assert.Equal(t, 1, pos.Line)
	assert.Equal(t, 46, pos.Char)
}

func TestPrepare_StripsReadDecorators(t *testing.T) {
	dql := `SELECT t.*,
use_connector(t, system),
allow_nulls(t)
FROM t`
	pre := Prepare(dql)
	require.NotNil(t, pre)
	assert.NotContains(t, pre.DirectSQL, "use_connector")
	assert.NotContains(t, pre.DirectSQL, "allow_nulls")
	assert.Contains(t, pre.DirectSQL, "SELECT t.*")
	assert.Contains(t, pre.DirectSQL, "FROM t")
	assert.NotContains(t, pre.DirectSQL, ",\nFROM")
}

func TestPrepare_PreservesSQLCastProjection(t *testing.T) {
	dql := `SELECT
    CAST($Var3 AS SIGNED) AS Key3,
    cast(status, 'int')
FROM t`
	pre := Prepare(dql)
	require.NotNil(t, pre)
	assert.Contains(t, pre.DirectSQL, "CAST($Var3 AS SIGNED) AS Key3")
	assert.NotContains(t, pre.DirectSQL, "cast(status, 'int')")
}

func TestPrepare_PreservesExecControlDirectives(t *testing.T) {
	dql := "#define($_ = $Ids<[]int>(body/Ids))\n" +
		"#foreach($rec in $Unsafe.Records)\n" +
		"#if($rec.IS_AUTH == 0)\n" +
		"$logger.Fatal('x')\n" +
		"#else\n" +
		"UPDATE PRODUCT SET STATUS = $Status WHERE ID = $rec.ID;\n" +
		"#end\n" +
		"#end"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	assert.Contains(t, pre.SQL, "#foreach($rec in $Unsafe.Records)")
	assert.Contains(t, pre.SQL, "#if($rec.IS_AUTH == 0)")
	assert.Contains(t, pre.SQL, "#else")
	assert.Contains(t, pre.SQL, "#end")
}

func TestPrepare_PreservesLocalSetDirectivesInExecTemplate(t *testing.T) {
	dql := "#define($_ = $Ids<[]int>(query/Ids))\n" +
		"#set($byID = $Unsafe.Rows.IndexBy(\"ID\"))\n" +
		"#foreach($id in $Unsafe.Ids)\n" +
		"  #set($row = $byID[$id])\n" +
		"  UPDATE T SET ACTIVE = 0 WHERE ID = $id;\n" +
		"#end"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	assert.Contains(t, pre.SQL, "#set($byID = $Unsafe.Rows.IndexBy(\"ID\"))")
	assert.Contains(t, pre.SQL, "#set($row = $byID[$id])")
	assert.NotContains(t, pre.SQL, "#define($_ = $Ids<[]int>(query/Ids))")
}

func TestPrepare_ConstDirective_UsesUnsafeSelectors(t *testing.T) {
	dql := "#setting($_ = $const('Vendor','VENDOR'))\n" +
		"SELECT * FROM ${Vendor} t WHERE t.ID = $id"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	assert.Contains(t, pre.SQL, "FROM ${Unsafe.Vendor} t")
	assert.Contains(t, pre.SQL, "$criteria.AppendBinding($Unsafe.id)")
	assert.NotContains(t, pre.SQL, "${criteria.AppendBinding($Unsafe.Vendor)}")
}

func TestPrepare_MultilineSetDirective_TypeContext(t *testing.T) {
	dql := "#package('a/b')\n#import('x','github.com/acme/x')\nSELECT id FROM t"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotNil(t, pre.TypeCtx)
	assert.Equal(t, "a/b", pre.TypeCtx.DefaultPackage)
	require.Len(t, pre.TypeCtx.Imports, 1)
	assert.Equal(t, "x", pre.TypeCtx.Imports[0].Alias)
	assert.Equal(t, "github.com/acme/x", pre.TypeCtx.Imports[0].Package)
	assert.Contains(t, pre.DirectSQL, "SELECT id FROM t")
}

func TestPrepare_InvalidMultilineImportDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#import(\n'x'\n)"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirImport, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
	assert.GreaterOrEqual(t, pre.Diagnostics[0].Span.Start.Char, 1)
}

func TestPrepare_SpecialDirectives(t *testing.T) {
	dql := "#settings($_ = $meta('docs/orders.md'))\n" +
		"#setting($_ = $connector('analytics'))\n" +
		"#setting($_ = $dest('vendor.go'))\n" +
		"#setting($_ = $input_dest('vendor_input.go'))\n" +
		"#setting($_ = $output_dest('vendor_output.go'))\n" +
		"#setting($_ = $router_dest('vendor_router.go'))\n" +
		"#setting($_ = $input_type('VendorInput'))\n" +
		"#setting($_ = $output_type('VendorOutput'))\n" +
		"#settings($_ = $cache(true, '5m'))\n" +
		"#settings($_ = $mcp('orders.search', 'Search orders', 'docs/mcp/orders.md'))\n" +
		"#settings($_ = $marshal('application/json','pkg.OrderJSON'))\n" +
		"#settings($_ = $unmarshal('application/json','pkg.OrderIn'))\n" +
		"#settings($_ = $unmarshal('application/xml','pkg.OrderXMLIn'))\n" +
		"#settings($_ = $format('tabular_json'))\n" +
		"#settings($_ = $date_format('2006-01-02'))\n" +
		"#settings($_ = $case_format('lc'))\n" +
		"#settings($_ = $useTemplate('patch'))\n" +
		"SELECT id FROM ORDERS o"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotNil(t, pre.Directives)
	assert.Equal(t, "docs/orders.md", pre.Directives.Meta)
	assert.Equal(t, "analytics", pre.Directives.DefaultConnector)
	assert.Equal(t, "vendor.go", pre.Directives.Dest)
	assert.Equal(t, "vendor_input.go", pre.Directives.InputDest)
	assert.Equal(t, "vendor_output.go", pre.Directives.OutputDest)
	assert.Equal(t, "vendor_router.go", pre.Directives.RouterDest)
	assert.Equal(t, "VendorInput", pre.Directives.InputType)
	assert.Equal(t, "VendorOutput", pre.Directives.OutputType)
	require.NotNil(t, pre.Directives.Cache)
	assert.True(t, pre.Directives.Cache.Enabled)
	assert.Equal(t, "5m", pre.Directives.Cache.TTL)
	require.NotNil(t, pre.Directives.MCP)
	assert.Equal(t, "orders.search", pre.Directives.MCP.Name)
	assert.Equal(t, "Search orders", pre.Directives.MCP.Description)
	assert.Equal(t, "docs/mcp/orders.md", pre.Directives.MCP.DescriptionPath)
	assert.Equal(t, "pkg.OrderJSON", pre.Directives.JSONMarshalType)
	assert.Equal(t, "pkg.OrderIn", pre.Directives.JSONUnmarshalType)
	assert.Equal(t, "pkg.OrderXMLIn", pre.Directives.XMLUnmarshalType)
	assert.Equal(t, "tabular", pre.Directives.Format)
	assert.Equal(t, "2006-01-02", pre.Directives.DateFormat)
	assert.Equal(t, "lc", pre.Directives.CaseFormat)
	assert.Equal(t, "patch", pre.Directives.TemplateType)
}

func TestPrepare_InvalidDestDirectiveDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#settings($_ = $dest())"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirDest, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
}

func TestPrepare_CacheProviderDirective(t *testing.T) {
	dql := "#setting($_ = $cache('aerospike').WithProvider('aerospike://127.0.0.1:3000/test').WithLocation('${view.Name}').WithTimeToLiveMs(3600000))\nSELECT 1"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotNil(t, pre.Directives)
	require.NotNil(t, pre.Directives.Cache)
	assert.Equal(t, "aerospike", pre.Directives.Cache.Name)
	assert.Equal(t, "aerospike://127.0.0.1:3000/test", pre.Directives.Cache.Provider)
	assert.Equal(t, "${view.Name}", pre.Directives.Cache.Location)
	assert.Equal(t, 3600000, pre.Directives.Cache.TimeToLiveMs)
}

func TestPrepare_InvalidSpecialDirectiveDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#settings($_ = $mcp())"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirMCP, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
}

func TestPrepare_InvalidConnectorDirectiveDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#settings($_ = $connector())"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirConnector, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
}

func TestPrepare_InvalidDirective_UsesExactCallSpan(t *testing.T) {
	lineText := "#settings($_ = $dest())"
	dql := "SELECT 1\n" + lineText
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirDest, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
	assert.Equal(t, strings.Index(lineText, "$dest(")+1, pre.Diagnostics[0].Span.Start.Char)
}

func TestPrepare_MalformedDirective_UsesExactCallSpan(t *testing.T) {
	lineText := "#settings($_ = $dest('x'"
	dql := "SELECT 1\n" + lineText
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirDest, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
	assert.Equal(t, strings.Index(lineText, "$dest(")+1, pre.Diagnostics[0].Span.Start.Char)
}

func TestPrepare_RouteDirective(t *testing.T) {
	dql := "SELECT 1\n#settings($_ = $route('/v1/api/orders', 'GET', 'POST', 'PATCH'))"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotNil(t, pre.Directives)
	require.NotNil(t, pre.Directives.Route)
	assert.Equal(t, "/v1/api/orders", pre.Directives.Route.URI)
	assert.Equal(t, []string{"GET", "POST", "PATCH"}, pre.Directives.Route.Methods)
}

func TestPrepare_InvalidRouteDirectiveDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#settings($_ = $route('/v1/api/orders', 'GOT'))"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirRoute, pre.Diagnostics[0].Code)
	assert.Equal(t, 2, pre.Diagnostics[0].Span.Start.Line)
}

func TestPrepare_InvalidCaseFormatDirectiveDiagnostic(t *testing.T) {
	dql := "SELECT 1\n#settings($_ = $case_format('unknown'))"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirCaseFormat, pre.Diagnostics[0].Code)
}

func TestPrepare_DefineDirective_DoesNotDriveSettingsExtraction(t *testing.T) {
	dql := "#define($_ = $package('a/b'))\nSELECT 1"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	assert.Nil(t, pre.TypeCtx)
}

func TestPrepare_PackageImportInSettings_UnsupportedDiagnostic(t *testing.T) {
	dql := "#settings($_ = $package('x'))\nSELECT 1"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotEmpty(t, pre.Diagnostics)
	assert.Equal(t, dqldiag.CodeDirUnsupported, pre.Diagnostics[0].Code)
	assert.Equal(t, 1, pre.Diagnostics[0].Span.Start.Line)
}

func TestPrepare_TypeContext_CaseInsensitive(t *testing.T) {
	dql := "#Package('a/b')\n#Import('x','github.com/acme/x')\nSELECT id FROM t"
	pre := Prepare(dql)
	require.NotNil(t, pre)
	require.NotNil(t, pre.TypeCtx)
	assert.Equal(t, "a/b", pre.TypeCtx.DefaultPackage)
	require.Len(t, pre.TypeCtx.Imports, 1)
	assert.Equal(t, "x", pre.TypeCtx.Imports[0].Alias)
	assert.Equal(t, "github.com/acme/x", pre.TypeCtx.Imports[0].Package)
}

func TestExtractLegacyTypeImports_BlockAndLine(t *testing.T) {
	dql := "import (\n" +
		"  \"github.com/acme/a.TypeA\"\n" +
		"  \"github.com/acme/b.TypeB\" alias \"b\"\n" +
		")\n" +
		"import \"github.com/acme/c.TypeC\"\n"

	imports, ranges, diags := extractLegacyTypeImports(dql)
	require.Empty(t, diags)
	require.Len(t, ranges, 2)
	require.Len(t, imports, 3)
	assert.Equal(t, "a", imports[0].Alias)
	assert.Equal(t, "github.com/acme/a", imports[0].Package)
	assert.Equal(t, "b", imports[1].Alias)
	assert.Equal(t, "github.com/acme/b", imports[1].Package)
	assert.Equal(t, "c", imports[2].Alias)
	assert.Equal(t, "github.com/acme/c", imports[2].Package)
}

func TestExtractLegacyTypeImports_InvalidBlockDiagnostic(t *testing.T) {
	dql := "import (\n  alias \"oops\"\n)\nSELECT 1"
	_, _, diags := extractLegacyTypeImports(dql)
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeDirImport, diags[0].Code)
}
