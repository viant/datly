package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuild_ProjectRouteTypeParametersWithTags(t *testing.T) {
	yaml := `
Routes:
  - Name: Example
    URI: /v1/api/example
    Method: GET
    Input:
      Type:
        Name: ExampleInput
        Package: example
        Parameters:
          - Name: Auth
            In:
              Kind: component
              Name: acl/auth
          - Name: Id
            Required: true
            In:
              Kind: query
              Name: id
            Tag: 'json:",omitempty" anonymous:"true"'
            ErrorStatusCode: 401
            Cacheable: true
            Scope: req
            Connector: ci_ads
            Limit: 25
            Schema:
              DataType: int
              Cardinality: One
    Output:
      Type:
        Name: ExampleOutput
        Package: example
        Parameters:
          - Name: Data
            In:
              Kind: output
              Name: view
            Output:
              Name: Json
              Args: ["a", "b"]
              Schema:
                DataType: string
                Cardinality: One
`
	result, err := Build([]byte(yaml))
	require.NoError(t, err)
	require.NotNil(t, result)

	routes, ok := result.Canonical["Routes"].([]any)
	require.True(t, ok)
	require.Len(t, routes, 1)

	route, ok := routes[0].(map[string]any)
	require.True(t, ok)

	input, ok := route["Input"].(map[string]any)
	require.True(t, ok)
	params, ok := input["Parameters"].([]any)
	require.True(t, ok)
	require.Len(t, params, 1, "component-kind parameter should be excluded from canonical input shape")

	param, ok := params[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Id", param["Name"])
	require.Equal(t, "json:\",omitempty\" anonymous:\"true\"", param["Tag"])
	require.EqualValues(t, 401, param["ErrorStatusCode"])
	require.Equal(t, true, param["Cacheable"])
	require.Equal(t, "req", param["Scope"])
	require.Equal(t, "ci_ads", param["Connector"])
	require.EqualValues(t, 25, param["Limit"])

	tagMeta, ok := param["TagMeta"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "json:\",omitempty\" anonymous:\"true\"", tagMeta["Raw"])
	pairs, ok := tagMeta["Pairs"].(map[string]string)
	require.True(t, ok)
	require.Equal(t, ",omitempty", pairs["json"])
	require.Equal(t, "true", pairs["anonymous"])

	output, ok := route["Output"].(map[string]any)
	require.True(t, ok)
	outParams, ok := output["Parameters"].([]any)
	require.True(t, ok)
	require.Len(t, outParams, 1)
	outParam, ok := outParams[0].(map[string]any)
	require.True(t, ok)
	outMeta, ok := outParam["Output"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Json", outMeta["Name"])
}

func TestValidateRelations_AliasAndColumnsWithLineDetails(t *testing.T) {
	routeYAML := `
Resource:
  Views:
    - Name: Parent
      Template:
        Source: |-
          SELECT p.ID, p.CAMPAIGN_ID FROM CI_PARENT p
      With:
        - Name: campaign
          Holder: Campaign
          Cardinality: One
          On:
            - Column: MISSING_PARENT
              Namespace: p
          Of:
            Ref: Child
            On:
              - Column: MISSING_CHILD
                Namespace: missing_alias
    - Name: Child
      Template:
        Source: |-
          SELECT c.ID FROM CI_CHILD c
`
	err := ValidateRelations([]byte(routeYAML))
	require.Error(t, err)
	require.Contains(t, err.Error(), "dql plan relation validation failed")
	require.Contains(t, err.Error(), "line")
	require.Contains(t, err.Error(), "alias=\"missing_alias\"")
	require.Contains(t, err.Error(), "column=\"MISSING_PARENT\"")
	require.Contains(t, err.Error(), "column=\"MISSING_CHILD\"")
	require.Contains(t, err.Error(), "column not projected")
	require.Contains(t, err.Error(), "alias not present in SQL/selector namespace")
}

func TestValidateRelations_AllowsValidRelationAliasAndColumns(t *testing.T) {
	routeYAML := `
Resource:
  Views:
    - Name: Parent
      Template:
        Source: |-
          SELECT p.ID, p.CAMPAIGN_ID FROM CI_PARENT p
      With:
        - Name: campaign
          Holder: Campaign
          Cardinality: One
          On:
            - Column: CAMPAIGN_ID
              Namespace: p
          Of:
            Ref: Child
            On:
              - Column: ID
                Namespace: c
    - Name: Child
      Template:
        Source: |-
          SELECT c.ID FROM CI_CHILD c
`
	err := ValidateRelations([]byte(routeYAML))
	require.NoError(t, err)
}
