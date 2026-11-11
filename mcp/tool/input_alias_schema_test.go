package tool

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
)

type aliasSchemaConstraint struct {
	Limit int `json:"limit"`
}

type aliasSchemaBody struct {
	Frequency *aliasSchemaConstraint `json:"frequencyCapping,omitempty" format:"inputNames=frequencyCap"`
}

type aliasSchemaInput struct {
	Body *aliasSchemaBody `parameter:"Body,kind=body"`
}

type requiredAliasSchemaBody struct {
	Frequency *aliasSchemaConstraint `json:"frequencyCapping" required:"true" format:"inputNames=frequencyCap"`
}

type requiredAliasSchemaInput struct {
	Body *requiredAliasSchemaBody `parameter:"Body,kind=body"`
}

type policyAliasSchemaBody struct {
	Frequency *aliasSchemaConstraint     `json:"frequencyCapping,omitempty" format:"inputNames=frequencyCap,inputNamesPriority=canonical,inputNamesKinds=object|null,inputNamesCase=fold"`
	Has       *policyAliasSchemaPresence `json:"-" setMarker:"true"`
}

type policyAliasSchemaPresence struct{ Frequency bool }

type policyAliasSchemaInput struct {
	Body *policyAliasSchemaBody `parameter:"Body,kind=body"`
}

func TestInputAliasPolicyNativeToolBinding(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[policyAliasSchemaInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "PolicyAliases"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "policyAliases"}, Contract: contract})
	require.NoError(t, err)
	for _, tc := range []struct {
		name              string
		body              map[string]any
		limit             int
		present, nilValue bool
	}{
		{"object alias", map[string]any{"frequencyCap": map[string]any{"limit": 2}}, 2, true, false},
		{"folded alias", map[string]any{"FREQUENCYCAP": map[string]any{"limit": 3}}, 3, true, false},
		{"ignored scalar", map[string]any{"frequencyCap": 5}, 0, false, true},
		{"alias null", map[string]any{"frequencyCap": nil}, 0, true, true},
		{"canonical wins", map[string]any{"frequencyCapping": map[string]any{"limit": 4}, "frequencyCap": map[string]any{"limit": "invalid"}}, 4, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope, err := plan.Scope(map[string]any{"body": tc.body})
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*policyAliasSchemaBody](), "")
			require.NoError(t, err)
			require.True(t, found)
			actual := value.(*policyAliasSchemaBody)
			require.NotNil(t, actual.Has)
			require.Equal(t, tc.present, actual.Has.Frequency)
			require.Equal(t, tc.nilValue, actual.Frequency == nil)
			if actual.Frequency != nil {
				require.Equal(t, tc.limit, actual.Frequency.Limit)
			}
		})
	}
}

func TestRequiredInputAliasToolSchemaAdmission(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[requiredAliasSchemaInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "RequiredAliases"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "requiredAliases"}, Contract: contract})
	require.NoError(t, err)
	for _, name := range []string{"frequencyCapping", "frequencyCap"} {
		t.Run(name, func(t *testing.T) {
			args := map[string]any{"body": map[string]any{name: map[string]any{"limit": 2}}}
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
			require.NoError(t, err)
			require.True(t, result.Valid(), "canonical or native alias satisfies required input: %v", result.Errors())
		})
	}
	missing, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(map[string]any{"body": map[string]any{}}))
	require.NoError(t, err)
	require.False(t, missing.Valid(), "alias support must not discard requiredness")
}

func TestInputAliasToolSchemaMatchesNativeBinding(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[aliasSchemaInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Aliases"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aliases"}, Contract: contract})
	require.NoError(t, err)
	args := map[string]any{"body": map[string]any{"frequencyCap": map[string]any{"limit": 2}}}
	scope, err := plan.Scope(args)
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*aliasSchemaBody](), "")
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, value.(*aliasSchemaBody).Frequency)
	require.Equal(t, 2, value.(*aliasSchemaBody).Frequency.Limit)
	result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
	require.NoError(t, err)
	require.True(t, result.Valid(), "schema must admit a body accepted by native binding: %v", result.Errors())
	// Discovery must describe the alias, rather than admitting it only as an
	// unconstrained unknown property that hides its accepted nested contract.
	body := plan.Metadata().InputSchema.Properties["body"]
	properties, ok := body["properties"].(map[string]interface{})
	require.True(t, ok, "body schema: %v", body)
	require.Contains(t, properties, "frequencyCap")
}

func TestNativeInputAliasOwnerRequiredMetadata(t *testing.T) {
	type body struct {
		Name *string `json:"name,omitempty" format:"inputNames=oldName"`
	}
	schema, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.Name": {Required: true}}}).argument(reflect.TypeFor[body](), "Body", "", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		body  map[string]any
		valid bool
	}{
		{map[string]any{"name": "ok"}, true}, {map[string]any{"oldName": "ok"}, true},
		{map[string]any{}, false}, {map[string]any{"oldName": nil}, false},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewGoLoader(tc.body))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "%v: %v", tc.body, result.Errors())
	}
	type folded struct {
		Name *string `json:"name,omitempty" format:"inputNames=oldName,inputNamesCase=fold"`
	}
	schema, err = (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.Name": {Required: true}}}).argument(reflect.TypeFor[folded](), "Body", "", nil)
	require.NoError(t, err)
	result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewGoLoader(map[string]any{"OLDNAME": "ok"}))
	require.NoError(t, err)
	require.True(t, result.Valid(), "folded required input: %v", result.Errors())
}

func TestAliasPriorityRefinementsDoNotRestoreLosingCanonical(t *testing.T) {
	type body struct {
		ID *int `json:"flightId" format:"inputNames=CampaignFlightId,inputNamesPriority=alias,inputNamesCase=fold"`
	}
	schema, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.ID": {Type: "integer", Required: true}}}).argument(reflect.TypeFor[body](), "Body", "", nil)
	require.NoError(t, err)
	result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewGoLoader(map[string]any{"flightId": "discarded", "CampaignFlightId": 2}))
	require.NoError(t, err)
	require.True(t, result.Valid(), "discarded canonical must not regain constraints: %v", result.Errors())
}

type priorityAliasToolBody struct {
	ID   *int `json:"flightId" format:"inputNames=CampaignFlightId,inputNamesPriority=alias,inputNamesCase=fold"`
	Mode *int `json:"mode" format:"inputNames=legacyMode,inputNamesPriority=alias,inputNamesCase=fold" choice:"1:ONE" choice:"2:TWO" enumAccept:"scalar" enumStyle:"ordinal"`
}
type priorityAliasToolInput struct {
	Body *priorityAliasToolBody `parameter:"Body,kind=body"`
}

func TestAliasPriorityToolSchemaAndInvocation(t *testing.T) {
	contract := testRouteContract(t, reflect.TypeFor[priorityAliasToolInput](), []bindly.BindingSpec{{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Contract: contract, Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Priority"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "priority"}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		body     map[string]any
		fail     bool
		id, mode *int
	}{
		{"discarded canonical", map[string]any{"flightId": "bad", "CampaignFlightId": 2}, false, newInt(2), nil},
		{"alias null", map[string]any{"flightId": 1, "CampaignFlightId": nil}, false, nil, nil},
		{"folded alias", map[string]any{"flightId": "bad", "CAMPAIGNFLIGHTID": 2}, false, newInt(2), nil},
		{"winning alias invalid", map[string]any{"flightId": 1, "CampaignFlightId": "bad"}, true, nil, nil},
		{"enum refinement discarded", map[string]any{"mode": map[string]any{}, "legacyMode": "TWO"}, false, nil, newInt(2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := map[string]any{"body": tc.body}
			result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
			require.NoError(t, err)
			if !tc.fail {
				require.True(t, result.Valid(), "%v", result.Errors())
			}
			scope, err := plan.Scope(args)
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*priorityAliasToolBody](), "")
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, found)
			actual := value.(*priorityAliasToolBody)
			require.Equal(t, tc.id, actual.ID)
			require.Equal(t, tc.mode, actual.Mode)
		})
	}
}

func newInt(value int) *int { return &value }

func TestNativeAliasMapRequiredRefinements(t *testing.T) {
	type row struct {
		Name *string `json:"name,omitempty" format:"inputNames=oldName"`
	}
	type body struct {
		Mapping map[string]row `json:"mapping"`
	}
	schema, err := (&schemaProjector{nativeBody: true, schemas: map[string]*spec.WireSchema{"Body.mapping.name": {Required: true}}}).argument(reflect.TypeFor[body](), "Body", "", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		value map[string]any
		valid bool
	}{
		{map[string]any{}, false}, {map[string]any{"oldName": nil}, false},
		{map[string]any{"oldName": "ok"}, true}, {map[string]any{"name": "ok"}, true},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewGoLoader(map[string]any{"mapping": map[string]any{"one": tc.value}}))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "map value %v: %v", tc.value, result.Errors())
	}
}
