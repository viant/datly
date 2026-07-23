package view

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
)

func TestView_EnsureColumns_UsesTypedSchemaForGroupedRelationAlias(t *testing.T) {
	ctx := context.Background()
	resource := NewResource(nil)
	resource.Types = []*TypeDefinition{
		{
			Name:       "DisqualifiedView",
			Package:    "taxonomy",
			ModulePath: "github.vianttech.com/viant/platform/pkg/platform/taxonomy",
			DataType:   `struct{TaxonomyId string ` + "`sqlx:\"TAXONOMY_ID\" source:\"SEGMENT_ID\" velty:\"names=TAXONOMY_ID|TaxonomyId\"`" + `; IsDisqualified int ` + "`sqlx:\"IS_DISQUALIFIED\" internal:\"true\" json:\"-\" velty:\"names=IS_DISQUALIFIED|IsDisqualified\"`" + `; }`,
		},
	}
	require.NoError(t, resource.Init(ctx))

	aView := &View{
		Name:     "disqualified",
		Table:    "CI_TAXONOMY_DISQUALIFIED",
		Alias:    "t",
		Mode:     ModeQuery,
		Schema:   &state.Schema{Name: "DisqualifiedView", Package: "taxonomy", Cardinality: state.Many},
		Template: &Template{Source: "SELECT dq.SEGMENT_ID AS TAXONOMY_ID, 1 AS IS_DISQUALIFIED FROM CI_TAXONOMY_DISQUALIFIED dq GROUP BY dq.SEGMENT_ID"},
		ColumnsConfig: map[string]*ColumnConfig{
			"IS_DISQUALIFIED": {
				Name: "IS_DISQUALIFIED",
				Tag:  ptrString(`json:"-" internal:"true"`),
			},
		},
	}

	require.NoError(t, aView.ensureColumns(ctx, resource))
	require.Len(t, aView.Columns, 2)
	aView.CaseFormat = text.CaseFormatLowerUnderscore
	aView._columns = Columns(aView.Columns).Index(aView.CaseFormat)

	column, ok := aView.ColumnByName("TaxonomyId")
	require.True(t, ok)
	require.Equal(t, "TAXONOMY_ID", column.Name)

	column, ok = aView.ColumnByName("SEGMENT_ID")
	require.True(t, ok)
	require.Equal(t, "TAXONOMY_ID", column.Name)
}

func ptrString(value string) *string {
	return &value
}
