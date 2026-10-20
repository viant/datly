package compile

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeDeclarationTagsPreservesValuesAndConflicts(t *testing.T) {
	const original = `json:"" sqlx:"id,primaryKey=true,required=false"`
	const addition = `internal:"true" example:"quote\" slash\\"`
	merged, err := mergeDeclarationTags(original, addition, "row.id")
	require.NoError(t, err)
	require.Equal(t, original+" "+addition, merged)
	value, present := reflect.StructTag(merged).Lookup("json")
	require.True(t, present)
	require.Empty(t, value)
	again, err := mergeDeclarationTags(merged, addition, "row.id")
	require.NoError(t, err)
	require.Equal(t, merged, again)
	_, err = mergeDeclarationTags(merged, `internal:"false"`, "row.id")
	require.ErrorContains(t, err, "conflicting internal values")
	_, err = mergeDeclarationTags(original, `internal:"true" internal:"false"`, "row.id")
	require.ErrorContains(t, err, "conflicting internal values")
	_, err = mergeDeclarationTags(original, `bad`, "row.id")
	require.Error(t, err)
}
