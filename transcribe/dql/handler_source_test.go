package dql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHandlerSource(t *testing.T) {
	const header = `{"URI":"/convert","Method":"POST","Name":"Convert","Type":"legacy.Handler","InputType":"legacy.Input","OutputType":"legacy.Output","Description":"convert","MCPTool":true}`
	body := "\n#set($_ = $Debug<bool>(form/debug).Optional())"
	result, remaining, err := ParseHandlerSource("/* " + header + " */" + body)
	require.NoError(t, err)
	require.Equal(t, "POST", result.Method)
	require.Equal(t, "legacy.Handler", result.Type)
	require.True(t, result.MCPTool)
	require.Equal(t, body, remaining)
	for _, source := range []string{"SELECT 1", "/* query hint */ SELECT 1", `/* {"URI":"/reader"} */ SELECT 1`, "/* { ordinary sql comment } */ SELECT 1"} {
		result, remaining, err = ParseHandlerSource(source)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, source, remaining)
	}
	for _, broken := range []string{
		strings.Replace(header, `"Method":"POST"`, `"Method":"POST","Method":"GET"`, 1),
		strings.Replace(header, `"Method":"POST"`, `"Method":"POST","method":"GET"`, 1),
		strings.Replace(header, `"MCPTool":true`, `"UnknownPolicy":true`, 1),
		strings.Replace(header, `"InputType":"legacy.Input"`, `"InputType":""`, 1),
		strings.TrimSuffix(header, "}"),
	} {
		_, _, err = ParseHandlerSource("/* " + broken + " */")
		require.Error(t, err, broken)
	}
}
