package reader

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
)

func TestBuilder_appendColumns_UsesAliasesForDiscoveredExpressions(t *testing.T) {
	builder := NewBuilder()
	useCases := []struct {
		description string
		sql         string
		expectedSQL string
	}{
		{
			description: "case expression keeps outer alias projection",
			sql:         "SELECT (CASE WHEN 'user_name' = 'user_name' THEN u.STR_ID ELSE NULL END) AS VALUE FROM CI_EVENT ev LEFT JOIN CI_CONTACTS u ON ev.CREATED_USER = u.ID",
			expectedSQL: " t.VALUE",
		},
		{
			description: "coalesce expression keeps discovered alias projection",
			sql:         "SELECT COALESCE(sl.APPROVED_SITE_CNT,0) AS NUMBER_OF_SITES FROM CI_SITE_LIST sl",
			expectedSQL: " t.NUMBER_OF_SITES",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			parsed, err := sqlparser.ParseQuery(useCase.sql)
			require.NoError(t, err)
			columns := view.NewColumns(sqlparser.NewColumns(parsed.List), nil)
			for _, column := range columns {
				if strings.TrimSpace(column.DataType) == "" {
					column.DataType = "string"
				}
			}
			aView := view.NewView("projection", "projection",
				view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
				view.WithColumns(columns),
			)
			require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))

			sb := &strings.Builder{}
			projected, err := builder.appendColumns(sb, aView, view.NewStatelet())
			require.NoError(t, err)
			require.Nil(t, projected)
			require.Equal(t, useCase.expectedSQL, sb.String())
		})
	}
}
