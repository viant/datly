package generate

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestDeclaredSourceSelectorTagPreserved(t *testing.T) {
	tag := reflect.StructTag(scalarColumnFieldTag(&spec.Column{Name: "TAXONOMY_ID", Tag: `source:"ID" json:"taxonomyId"`}, "ID", false))
	require.Equal(t, "ID", tag.Get("source"))
	require.Equal(t, "ID|TAXONOMY_ID", tag.Get("sqlx"))
	require.Equal(t, "taxonomyId", tag.Get("json"))
	inferred := reflect.StructTag(scalarColumnFieldTag(&spec.Column{Name: "TAXONOMY_ID"}, "ID", false))
	require.Empty(t, inferred.Get("source"), "physical SQLX mapping alone is not declared source-alias authority")
}
