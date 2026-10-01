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
	authored := strings.Replace(header, `"Type":"legacy.Handler"`, `"Factory":"example.com/app/convert.New"`, 1)
	result, remaining, err = ParseHandlerSource("/* " + authored + " */" + body)
	require.NoError(t, err)
	require.Equal(t, "example.com/app/convert.New", result.Factory)
	require.Empty(t, result.Type)
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
		strings.TrimSuffix(authored, "}"),
		strings.Replace(authored, `"Factory":"example.com/app/convert.New"`, `"Factory":""`, 1),
	} {
		_, _, err = ParseHandlerSource("/* " + broken + " */")
		require.Error(t, err, broken)
	}
}

func TestParseDeclarativeHandlerSource(t *testing.T) {
	source := `#package('example.com/app/generated')
#setting($_ = $handler_factory('business.NewConvert','Convert'))
#setting($_ = $route('/convert','POST'))
#setting($_ = $input_type('business.Input'))
#setting($_ = $output_type('business.Output'))
#setting($_ = $case_format('lc'))
#define($_ = $Debug<bool>(form/debug).Optional())`
	header, body, err := ParseHandlerSource(source)
	require.NoError(t, err)
	require.True(t, header.Declarative)
	require.Equal(t, "business.NewConvert", header.Factory)
	require.Equal(t, "Convert", header.Name)
	require.Equal(t, "POST", header.Method)
	require.Equal(t, source, body)
	for _, invalid := range []string{
		strings.Replace(source, "'business.NewConvert','Convert'", "", 1),
		strings.Replace(source, "'business.NewConvert','Convert'", "business.NewConvert", 1),
		strings.Replace(source, "'business.NewConvert','Convert'", "'business.NewConvert','Convert','extra'", 1),
		source + "\n#setting($_ = $handler_factory('business.Other'))",
		strings.Replace(source, "#setting($_ = $input_type('business.Input'))", "", 1),
		strings.Replace(source, "'/convert','POST'", "'/convert','POST','GET'", 1),
		`/* {"URI":"/convert","Method":"POST","Name":"Convert","Factory":"business.NewConvert","InputType":"business.Input","OutputType":"business.Output"} */` + source,
	} {
		_, _, err := ParseHandlerSource(invalid)
		require.Error(t, err, invalid)
	}
	// Handler-looking text inside an ordinary comment is not a declaration.
	h, _, err := ParseHandlerSource("-- handler_factory is documentation\nSELECT 1")
	require.NoError(t, err)
	require.Nil(t, h)
}
