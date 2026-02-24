package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/plan"
)

func TestAppendDeclaredStates(t *testing.T) {
	dql := `
#set($_ = $Jwt<string>(header/Authorization).WithCodec(JwtClaim).WithStatusCode(401))
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
	assert.Equal(t, "header", byName["Jwt"].Kind)
	require.NotNil(t, byName["Jwt"].Required)
	assert.True(t, *byName["Jwt"].Required)

	require.NotNil(t, byName["Name"])
	assert.Equal(t, "query", byName["Name"].Kind)
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
	assert.Equal(t, "IS_TARGETABLE", result.States[0].Predicates[0].Arguments[1])
}

func TestAppendDeclaredStates_SupportsDefineDirective(t *testing.T) {
	dql := `
#define($_ = $Auth<string>(header/Authorization).Required())
SELECT id FROM USERS u`
	result := &plan.Result{}
	appendDeclaredStates(dql, result)
	require.Len(t, result.States, 1)
	assert.Equal(t, "Auth", result.States[0].Name)
	assert.Equal(t, "header", result.States[0].Kind)
	require.NotNil(t, result.States[0].Required)
	assert.True(t, *result.States[0].Required)
}
