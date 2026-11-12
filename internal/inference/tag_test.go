package inference

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/metadata/sink"
)

func TestSpecBuildType_PreservesSourceTagForAliasedProjection(t *testing.T) {
	spec := &Spec{
		Table: "CI_TAXONOMY_DISQUALIFIED",
		Columns: sqlparser.Columns{
			&sqlparser.Column{
				Name:       "TAXONOMY_ID",
				Alias:      "TAXONOMY_ID",
				Expression: "dq.SEGMENT_ID",
				Namespace:  "dq",
				Type:       "string",
			},
			&sqlparser.Column{
				Name: "IS_DISQUALIFIED",
				Type: "int",
			},
		},
		pk: map[string]sink.Key{},
		Fk: map[string]sink.Key{},
	}

	err := spec.BuildType("taxonomy", "DisqualifiedView", state.Many, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, spec.Type)
	require.Len(t, spec.Type.columnFields, 2)

	field := spec.Type.columnFields[0]
	require.Equal(t, `sqlx:"TAXONOMY_ID" source:"SEGMENT_ID" validate:"required"`, field.Tag)

	structField := field.StructField(WithStructTag())
	require.Equal(t, "SEGMENT_ID", reflect.StructTag(structField.Tag).Get("source"))
	require.Equal(t, "TAXONOMY_ID", reflect.StructTag(structField.Tag).Get("sqlx"))
}
