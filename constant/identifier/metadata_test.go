package identifier

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
)

func TestRuntimeMetadataUsesDetachedInstanceIdentifiers(t *testing.T) {
	authored := &spec.Component{RootView: &spec.View{Name: "Rules", Source: &spec.ViewSource{Table: "$Unsafe.Table", SQL: "SELECT * FROM $Unsafe.Table"}, Columns: []*spec.Column{{Name: "Owner", Tag: `sqlx:"owner_id,refDb=${schema},refTable=OWNERS${suffix},refColumn=id,required=false" json:"owner"`}}}}
	before, err := json.Marshal(authored)
	require.NoError(t, err)
	for _, suffix := range []string{"_SOAK01", "_PROD"} {
		t.Run(suffix, func(t *testing.T) {
			t.Parallel()
			values, err := constant.New(map[string]string{"Table": "RULES" + suffix, "schema": "main", "suffix": suffix})
			require.NoError(t, err)
			result, err := (Renderer{Values: values}).Component(authored)
			require.NoError(t, err)
			require.Equal(t, "RULES"+suffix, result.RootView.Source.Table)
			require.Equal(t, authored.RootView.Source.SQL, result.RootView.Source.SQL)
			require.Equal(t, `owner_id,refDb=main,refTable=OWNERS`+suffix+`,refColumn=id,required=false`, reflect.StructTag(result.RootView.Columns[0].Tag).Get("sqlx"))
			after, err := json.Marshal(authored)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestMetadataIdentifierResolution(t *testing.T) {
	values, err := constant.New(map[string]string{"project": "my-project", "dataset": "analytics", "suffix": "_SOAK01", "Table": "RULES_SOAK01"})
	require.NoError(t, err)
	renderer := Renderer{Values: values}
	for _, tc := range []struct{ source, want string }{
		{"CI_TARGETING_RULE${suffix}", "CI_TARGETING_RULE_SOAK01"},
		{"`${project}.${dataset}.table`", "my-project.analytics.table"},
		{"$Unsafe.Table", "RULES_SOAK01"},
		{"RULES_SOAK01", "RULES_SOAK01"},
	} {
		actual, err := renderer.Metadata(tc.source)
		require.NoError(t, err)
		require.Equal(t, tc.want, actual)
	}
	for _, invalid := range []string{"$Unsafe.Absent", "${missing}", "CI_TARGETING_RULE${missing}"} {
		_, err := renderer.Metadata(invalid)
		require.Error(t, err, invalid)
	}
}

func TestEmptyIdentifierSuffix(t *testing.T) {
	values, err := constant.New(map[string]string{"suffix": ""})
	require.NoError(t, err)
	renderer := Renderer{Values: values}
	for _, sql := range []string{"SELECT * FROM CI_TARGETING_RULE${suffix}", "SELECT * FROM `CI_TARGETING_RULE${suffix}`"} {
		resolved, err := renderer.Render(sql)
		require.NoError(t, err)
		require.NotContains(t, resolved, "${suffix}")
		require.Contains(t, resolved, "CI_TARGETING_RULE")
	}
}
