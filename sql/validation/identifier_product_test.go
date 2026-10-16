package validation

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

type productValidationRow struct {
	ID    int    `sqlx:"id,primaryKey"`
	Owner int    `sqlx:"owner_id,refTable=${project}.automation.owners,refDb=${project},refColumn=id"`
	Code  string `sqlx:"code,unique,table=${project}.scope3.emissions_data"`
}

type productValidationSource struct{}

func (productValidationSource) ValidationConnection(_ context.Context, connector string) (Connection, error) {
	product := "MySQL"
	if connector == "metrics" || connector == "" {
		product = "BigQuery"
	}
	return Connection{Dialect: &info.Dialect{Product: database.Product{Name: product}}}, nil
}

func TestConstraintIdentifierProductIsolation(t *testing.T) {
	service := New(productValidationSource{})
	typ := reflect.TypeFor[productValidationRow]()
	before := typ.Field(1).Tag
	for _, project := range []string{"viant-e2e", "viant-prod"} {
		t.Run(project, func(t *testing.T) {
			t.Parallel()
			values, err := constant.New(map[string]string{"project": project})
			require.NoError(t, err)
			ctx := constant.WithContext(t.Context(), values)
			for i := 0; i < 2; i++ {
				plan, err := service.plan(ctx, typ, "metrics")
				require.NoError(t, err)
				owner, ok := plan.sqlType.FieldByName("Owner")
				require.True(t, ok)
				code, ok := plan.sqlType.FieldByName("Code")
				require.True(t, ok)
				require.Contains(t, string(owner.Tag), "refTable="+project+".automation.owners")
				require.Contains(t, string(code.Tag), "table="+project+".scope3.emissions_data")
				_, err = service.plan(ctx, typ, "ads")
				require.ErrorContains(t, err, "unexpected table identifier suffix")
			}
			mapping, err := service.mapping(typ)
			require.NoError(t, err)
			again, err := service.mapping(typ)
			require.NoError(t, err)
			require.Same(t, mapping, again)
			require.Equal(t, before, typ.Field(1).Tag)
		})
	}
}
