package inference

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view/state"
)

func TestParameterDeclarationPreservesMCPRouteControls(t *testing.T) {
	mainMCP := false
	pathMCP := true
	parameter := &Parameter{Parameter: state.Parameter{
		Name:    "Id",
		In:      state.NewPathLocation("id"),
		Schema:  &state.Schema{DataType: "int", Cardinality: state.Many},
		URI:     "/things/{id}",
		MCP:     &mainMCP,
		PathMCP: &pathMCP,
	}}

	declaration := parameter.DsqlParameterDeclaration()
	assert.Contains(t, declaration, `.WithURI('/things/{id}')`)
	assert.Contains(t, declaration, `.WithMcp(false)`)
	assert.Contains(t, declaration, `.WithPathMcp(true)`)
}
