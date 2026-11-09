package tool

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestPresenceOnlyRejectsUnprovenMCPAuthority(t *testing.T) {
	type input struct {
		Supplied bool `parameter:"Supplied,kind=body,in=channelV2,presenceOnly=true" internal:"true"`
	}
	field, _ := reflect.TypeFor[input]().FieldByName("Supplied")
	binding, _, err := bindly.BindingSpecFromField(field)
	require.NoError(t, err)
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{binding})
	_, err = NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Presence"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "presence"}, Contract: contract})
	require.ErrorContains(t, err, "presenceOnly source projection has no MCP transport authority")
}
