package translator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
)

func TestResource_extractRuleSetting_RouteDirectiveOverridesHeader(t *testing.T) {
	resource := &Resource{Rule: NewRule(), rule: &options.Rule{}}
	dSQL := "/* {\"URI\":\"/v1/api/legacy\",\"Method\":\"GET\"} */\n" +
		"#settings($_ = $route('/v1/api/orders', 'POST', 'PATCH'))\n" +
		"#settings($_ = $marshal('application/json','pkg.OrderJSON'))\n" +
		"#settings($_ = $unmarshal('application/json','pkg.OrderIn'))\n" +
		"#settings($_ = $unmarshal('application/xml','pkg.OrderXMLIn'))\n" +
		"#settings($_ = $format('tabular_json'))\n" +
		"#settings($_ = $date_format('2006-01-02'))\n" +
		"#settings($_ = $case_format('lc'))\n" +
		"SELECT 1"

	err := resource.extractRuleSetting(&dSQL)
	require.NoError(t, err)
	assert.Equal(t, "/v1/api/orders", resource.Rule.URI)
	assert.Equal(t, "POST,PATCH", resource.Rule.Method)
	assert.Equal(t, "pkg.OrderJSON", resource.Rule.JSONMarshalType)
	assert.Equal(t, "pkg.OrderIn", resource.Rule.JSONUnmarshalType)
	assert.Equal(t, "pkg.OrderXMLIn", resource.Rule.XMLUnmarshalType)
	assert.Equal(t, "tabular", resource.Rule.DataFormat)
	assert.Equal(t, "2006-01-02", resource.Rule.Route.Content.DateFormat)
	assert.Equal(t, "lc", string(resource.Rule.Route.Output.CaseFormat))
	assert.NotContains(t, dSQL, "$route(")
	assert.NotContains(t, dSQL, "$marshal(")
	assert.NotContains(t, dSQL, "$unmarshal(")
	assert.NotContains(t, dSQL, "$format(")
	assert.NotContains(t, dSQL, "$date_format(")
	assert.NotContains(t, dSQL, "$case_format(")
}

func TestResource_extractRuleSetting_InvalidRouteDirective(t *testing.T) {
	resource := &Resource{Rule: NewRule(), rule: &options.Rule{}}
	dSQL := "#settings($_ = $route('/v1/api/orders', 'GOT'))\nSELECT 1"

	err := resource.extractRuleSetting(&dSQL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported method")
}

func TestResource_extractRuleSetting_InvalidCaseFormatDirective(t *testing.T) {
	resource := &Resource{Rule: NewRule(), rule: &options.Rule{}}
	dSQL := "#settings($_ = $case_format('unknown'))\nSELECT 1"

	err := resource.extractRuleSetting(&dSQL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported case format")
}

func TestResource_extractRuleSetting_PackageQualifiesTypes(t *testing.T) {
	resource := &Resource{Rule: NewRule(), rule: &options.Rule{}}
	dSQL := "#package('github.vianttech.com/viant/handson/pkg/platform/acl/auth')\n" +
		"#settings($_ = $handler('Handler'))\n" +
		"#settings($_ = $input('Input'))\n" +
		"#settings($_ = $output('Output'))\n" +
		"#settings($_ = $marshal('application/json','JSONOut'))\n" +
		"#settings($_ = $unmarshal('application/json','JSONIn'))\n" +
		"SELECT 1"

	err := resource.extractRuleSetting(&dSQL)
	require.NoError(t, err)
	assert.Equal(t, "github.vianttech.com/viant/handson/pkg/platform/acl/auth", resource.Rule.Package)
	assert.Equal(t, "github.vianttech.com/viant/handson/pkg/platform/acl/auth.Handler", resource.Rule.Type)
	assert.Equal(t, "github.vianttech.com/viant/handson/pkg/platform/acl/auth.Input", resource.Rule.InputType)
	assert.Equal(t, "github.vianttech.com/viant/handson/pkg/platform/acl/auth.Output", resource.Rule.OutputType)
	assert.Equal(t, "github.vianttech.com/viant/handson/pkg/platform/acl/auth.JSONOut", resource.Rule.JSONMarshalType)
	assert.Equal(t, "github.vianttech.com/viant/handson/pkg/platform/acl/auth.JSONIn", resource.Rule.JSONUnmarshalType)
	assert.NotContains(t, dSQL, "#package(")
	assert.NotContains(t, dSQL, "$handler(")
	assert.NotContains(t, dSQL, "$input(")
	assert.NotContains(t, dSQL, "$output(")
}
