package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
)

func TestParser_Parse_TypeContext(t *testing.T) {
	dql := `
#package('mdp/performance')
#import('perf', 'github.com/acme/mdp/performance')
SELECT id FROM ORDERS t
`
	parsed, err := New().Parse(dql)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.NotNil(t, parsed.TypeContext)
	assert.Equal(t, "mdp/performance", parsed.TypeContext.DefaultPackage)
	require.Len(t, parsed.TypeContext.Imports, 1)
	assert.Equal(t, "perf", parsed.TypeContext.Imports[0].Alias)
	assert.Equal(t, "github.com/acme/mdp/performance", parsed.TypeContext.Imports[0].Package)
}

func TestParser_Parse_SpecialDirectives(t *testing.T) {
	dql := `
#settings($_ = $meta('docs/orders.md'))
#setting($_ = $connector('analytics'))
#settings($_ = $cache(true, '5m'))
#settings($_ = $mcp('orders.search', 'Search orders', 'docs/mcp/orders.md'))
SELECT id FROM ORDERS t
`
	parsed, err := New().Parse(dql)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.NotNil(t, parsed.Directives)
	assert.Equal(t, "docs/orders.md", parsed.Directives.Meta)
	assert.Equal(t, "analytics", parsed.Directives.DefaultConnector)
	require.NotNil(t, parsed.Directives.Cache)
	assert.True(t, parsed.Directives.Cache.Enabled)
	assert.Equal(t, "5m", parsed.Directives.Cache.TTL)
	require.NotNil(t, parsed.Directives.MCP)
	assert.Equal(t, "orders.search", parsed.Directives.MCP.Name)
	assert.Equal(t, "Search orders", parsed.Directives.MCP.Description)
	assert.Equal(t, "docs/mcp/orders.md", parsed.Directives.MCP.DescriptionPath)
}

func TestParser_Parse_SyntaxErrorPosition(t *testing.T) {
	dql := "SELECT id FROM ORDERS WHERE ("
	parsed, err := New().Parse(dql)
	require.Error(t, err)
	require.NotNil(t, parsed)
	require.NotEmpty(t, parsed.Diagnostics)
	diag := parsed.Diagnostics[0]
	assert.Equal(t, dqldiag.CodeParseSyntax, diag.Code)
	assert.Equal(t, 1, diag.Span.Start.Line)
	assert.Equal(t, 29, diag.Span.Start.Char)
}

func TestParser_Parse_OnlyDirectives(t *testing.T) {
	dql := "#package('x')\n#import('a','b')"
	parsed, err := New().Parse(dql)
	require.Error(t, err)
	require.NotNil(t, parsed)
	require.NotEmpty(t, parsed.Diagnostics)
	assert.Equal(t, dqldiag.CodeParseEmpty, parsed.Diagnostics[0].Code)
	assert.Equal(t, 1, parsed.Diagnostics[0].Span.Start.Line)
	assert.Equal(t, 1, parsed.Diagnostics[0].Span.Start.Char)
}

func TestParser_Parse_InvalidDirective_HasLineAndChar(t *testing.T) {
	dql := "SELECT id FROM ORDERS t\n#import('alias')\nSELECT id FROM ORDERS t"
	parsed, err := New().Parse(dql)
	require.Error(t, err)
	require.NotNil(t, parsed)
	require.NotEmpty(t, parsed.Diagnostics)
	diag := parsed.Diagnostics[0]
	assert.Equal(t, dqldiag.CodeDirImport, diag.Code)
	assert.Equal(t, 2, diag.Span.Start.Line)
	assert.Equal(t, 1, diag.Span.Start.Char)
}

func TestParser_Parse_DMLOnly_NoError(t *testing.T) {
	dql := "INSERT INTO ORDERS(id) VALUES (1)"
	parsed, err := New().Parse(dql)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Nil(t, parsed.Query)
	assert.Empty(t, parsed.Diagnostics)
}

func TestParser_Parse_Mixed_ReadAndExec_ParsesRead(t *testing.T) {
	dql := "INSERT INTO ORDERS(id) VALUES (1)\nSELECT id FROM ORDERS t"
	parsed, err := New().Parse(dql)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.NotNil(t, parsed.Query)
	assert.Equal(t, "t", parsed.Query.From.Alias)
}

func TestParser_Parse_UnknownNonRead_Warns(t *testing.T) {
	dql := "$Foo.Bar($x)"
	parsed, err := New().Parse(dql)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Nil(t, parsed.Query)
	require.NotEmpty(t, parsed.Diagnostics)
	assert.Equal(t, dqldiag.CodeParseUnknownNonRead, parsed.Diagnostics[len(parsed.Diagnostics)-1].Code)
	assert.Equal(t, dqlshape.SeverityWarning, parsed.Diagnostics[len(parsed.Diagnostics)-1].Severity)
}

func TestParser_Parse_UnknownNonRead_ErrorsWhenConfigured(t *testing.T) {
	dql := "$Foo.Bar($x)"
	parsed, err := New(WithUnknownNonReadMode(UnknownNonReadModeError)).Parse(dql)
	require.Error(t, err)
	require.NotNil(t, parsed)
	assert.Nil(t, parsed.Query)
	require.NotEmpty(t, parsed.Diagnostics)
	assert.Equal(t, dqldiag.CodeParseUnknownNonRead, parsed.Diagnostics[len(parsed.Diagnostics)-1].Code)
	assert.Equal(t, dqlshape.SeverityError, parsed.Diagnostics[len(parsed.Diagnostics)-1].Severity)
}

func TestParser_Parse_UnknownNonRead_InvalidModeDefaultsToWarn(t *testing.T) {
	dql := "$Foo.Bar($x)"
	parsed, err := New(WithUnknownNonReadMode(UnknownNonReadMode("invalid"))).Parse(dql)
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.NotEmpty(t, parsed.Diagnostics)
	assert.Equal(t, dqldiag.CodeParseUnknownNonRead, parsed.Diagnostics[len(parsed.Diagnostics)-1].Code)
	assert.Equal(t, dqlshape.SeverityWarning, parsed.Diagnostics[len(parsed.Diagnostics)-1].Severity)
}
