package compile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	"github.com/viant/datly/repository/shape/plan"
)

func TestAppendDeclaredStates(t *testing.T) {
	dql := `
#set($_ = $Jwt<string>(header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
#set($_ = $Claims<string,*JwtClaims>(header/Authorization).WithCodec(JwtClaim).WithTag('json:"claims,omitempty"'))
#set($_ = $Name<string>(query/name).WithPredicate(0,'contains','sl','NAME').Optional())
#set($_ = $Fields<[]string>(query/fields).QuerySelector(site_list))
#set($_ = $Meta<?>(output/summary))
SELECT id FROM SITE_LIST sl`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.NotEmpty(t, result.States)

	byName := map[string]*plan.State{}
	for _, item := range result.States {
		if item != nil {
			byName[item.Name] = item
		}
	}
	require.NotNil(t, byName["Jwt"])
	assert.Equal(t, "header", byName["Jwt"].KindString())
	assert.Equal(t, "string", byName["Jwt"].Schema.DataType)
	assert.Equal(t, "JwtClaim", byName["Jwt"].Output.Name)
	assert.Equal(t, 401, byName["Jwt"].ErrorStatusCode)
	require.NotNil(t, byName["Jwt"].Required)
	assert.True(t, *byName["Jwt"].Required)

	require.NotNil(t, byName["Claims"])
	assert.Equal(t, "string", byName["Claims"].Schema.DataType)
	assert.Equal(t, "*JwtClaims", byName["Claims"].OutputDataType)
	assert.Equal(t, `json:"claims,omitempty"`, byName["Claims"].Tag)

	require.NotNil(t, byName["Name"])
	assert.Equal(t, "query", byName["Name"].KindString())
	require.NotNil(t, byName["Name"].Required)
	assert.False(t, *byName["Name"].Required)
	require.Len(t, byName["Name"].Predicates, 1)
	assert.Equal(t, "contains", byName["Name"].Predicates[0].Name)
	assert.Equal(t, 0, byName["Name"].Predicates[0].Group)

	require.NotNil(t, byName["Fields"])
	assert.Equal(t, "site_list", byName["Fields"].QuerySelector)
	require.NotNil(t, byName["Fields"].Cacheable)
	assert.False(t, *byName["Fields"].Cacheable)
}

func TestAppendDeclaredStates_DuplicateDeclaration_FirstWins(t *testing.T) {
	dql := `
#set($_ = $Active<boolean>(query/active).WithPredicate(0,'equal','tas','IS_TARGETABLE').Optional())
#set($_ = $Active<boolean>(query/active).WithPredicate(0,'equal','tas','ACTIVE').Optional())
SELECT id FROM CI_TV_AFFILIATE_STATION tas`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.Len(t, result.States, 1)
	require.Len(t, result.States[0].Predicates, 1)
	assert.Equal(t, "Active", result.States[0].Name)
	assert.Equal(t, "IS_TARGETABLE", result.States[0].Predicates[0].Args[1])
}

func TestAppendDeclaredStates_SupportsDefineDirective(t *testing.T) {
	dql := `
#define($_ = $Auth<string>(header/Authorization).Required())
SELECT id FROM USERS u`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.Len(t, result.States, 1)
	assert.Equal(t, "Auth", result.States[0].Name)
	assert.Equal(t, "header", result.States[0].KindString())
	require.NotNil(t, result.States[0].Required)
	assert.True(t, *result.States[0].Required)
}

func TestAppendDeclaredStates_ViewDeclarationBecomesViewInput(t *testing.T) {
	dql := `
#define($_ = $Authorization<?>(view/authorization).Required().WithStatusCode(403) /* SELECT Authorized FROM AUTH */)
SELECT id FROM USERS u`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.Len(t, result.States, 1)
	state := result.States[0]
	require.NotNil(t, state)
	assert.Equal(t, "Authorization", state.Name)
	assert.Equal(t, "view", state.KindString())
	assert.Equal(t, "Authorization", state.In.Name)
	require.NotNil(t, state.Required)
	assert.True(t, *state.Required)
	assert.Equal(t, 403, state.ErrorStatusCode)
}

func TestAppendDeclaredStates_SkipsSummaryAttachedViewDeclaration(t *testing.T) {
	dql := `
#define($_ = $ProductsMeta<?>(view/products_meta) /* SELECT COUNT(1) CNT FROM ($View.products.SQL) t */)
SELECT vendor.*, products.*
FROM (SELECT * FROM VENDOR t) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID`
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "vendor"},
			{Name: "products", SummaryName: "ProductsMeta"},
		},
	}

	appendDeclaredStates(dql, result)

	require.Empty(t, result.States)
}

func TestAppendDeclaredStates_EmbedSetsAnonymousTag(t *testing.T) {
	dql := `
#set($_ = $Data<?>(output/view).Embed())
SELECT id FROM USERS u`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.Len(t, result.States, 1)
	assert.Equal(t, "Data", result.States[0].Name)
	assert.Equal(t, "output", result.States[0].KindString())
	assert.Contains(t, result.States[0].Tag, `anonymous:"true"`)
}

func TestAppendDeclaredStates_OutputViewCardinalityIsParsed(t *testing.T) {
	dql := `
#define($_ = $Data<?>(output/view).Cardinality('One').Embed())
SELECT id FROM USERS u`
	result := &plan.Result{}

	appendDeclaredStates(dql, result)

	require.Len(t, result.States, 1)
	require.NotNil(t, result.States[0].Schema)
	assert.Equal(t, "output", result.States[0].KindString())
	assert.Equal(t, "One", string(result.States[0].Schema.Cardinality))
	assert.Contains(t, result.States[0].Tag, `anonymous:"true"`)
}

func TestAppendDeclaredStates_OutputOptionMarksStateForOutput(t *testing.T) {
	dql := `
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))
SELECT * FROM FOOS`
	result := &plan.Result{}

	appendDeclaredStates(dql, result)

	require.Len(t, result.States, 1)
	assert.Equal(t, "Foos", result.States[0].Name)
	assert.Equal(t, "body", result.States[0].KindString())
	assert.True(t, result.States[0].EmitOutput)
}

func TestAppendDeclaredStates_DuplicateDeclarationMergesOutputMarker(t *testing.T) {
	dql := `
#set($_ = $Foos<?>(body/).Cardinality('One').Tag('anonymous:"true"'))
#set($_ = $Foos<?>(body/).Output().Tag('anonymous:"true"'))
SELECT * FROM FOOS`
	result := &plan.Result{}

	appendDeclaredStates(dql, result)

	require.Len(t, result.States, 1)
	assert.Equal(t, "Foos", result.States[0].Name)
	assert.Equal(t, "body", result.States[0].KindString())
	assert.True(t, result.States[0].EmitOutput)
	require.NotNil(t, result.States[0].Schema)
	assert.Equal(t, "One", string(result.States[0].Schema.Cardinality))
}

func TestAppendDeclaredStates_InvalidOption_ReportsExactSpan(t *testing.T) {
	dql := `
#set($_ = $Auth<string>(header/Authorization).Cacheable('x').UnknownFlag())
SELECT id FROM USERS u`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.NotEmpty(t, result.Diagnostics)
	require.GreaterOrEqual(t, len(result.Diagnostics), 2)
	assert.Equal(t, dqldiag.CodeDeclOptionArgs, result.Diagnostics[0].Code)
	assert.Equal(t, dqldiag.CodeDeclOptionArgs, result.Diagnostics[1].Code)

	cacheableOffset := strings.Index(dql, ".Cacheable")
	require.GreaterOrEqual(t, cacheableOffset, 0)
	cacheablePos := dqlpre.PointSpan(dql, cacheableOffset).Start
	assert.Equal(t, cacheablePos.Line, result.Diagnostics[0].Span.Start.Line)
	assert.Equal(t, cacheablePos.Char, result.Diagnostics[0].Span.Start.Char)

	unknownOffset := strings.Index(dql, ".UnknownFlag")
	require.GreaterOrEqual(t, unknownOffset, 0)
	unknownPos := dqlpre.PointSpan(dql, unknownOffset).Start
	assert.Equal(t, unknownPos.Line, result.Diagnostics[1].Span.Start.Line)
	assert.Equal(t, unknownPos.Char, result.Diagnostics[1].Span.Start.Char)
}

func TestAppendDeclaredStates_InferPathStatesFromRouteDirective(t *testing.T) {
	dql := `
#setting($_ = $route('/v1/api/shape/dev/team/{teamID}', 'DELETE'))
DELETE FROM TEAM WHERE ID = ${teamID}`
	result := &plan.Result{}

	appendDeclaredStates(dql, result)

	require.Len(t, result.States, 1)
	assert.Equal(t, "teamID", result.States[0].Name)
	assert.Equal(t, "path", result.States[0].KindString())
	assert.Equal(t, "teamID", result.States[0].In.Name)
	require.NotNil(t, result.States[0].Schema)
	assert.Equal(t, "string", result.States[0].Schema.DataType)
}

func TestAppendDeclaredStates_ExplicitPathStateWinsOverInferredRouteParam(t *testing.T) {
	dql := `
#setting($_ = $route('/v1/api/shape/dev/vendors/{vendorID}', 'GET'))
#define($_ = $VendorID<int>(path/vendorID))
SELECT * FROM VENDOR WHERE ID = $VendorID`
	result := &plan.Result{}

	appendDeclaredStates(dql, result)

	require.Len(t, result.States, 1)
	assert.Equal(t, "VendorID", result.States[0].Name)
	assert.Equal(t, "path", result.States[0].KindString())
	assert.Equal(t, "vendorID", result.States[0].In.Name)
	assert.Equal(t, "int", result.States[0].Schema.DataType)
}
