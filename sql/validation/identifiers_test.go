package validation_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

type identifierRow struct {
	ID    int    `sqlx:"id,primaryKey"`
	Owner int    `sqlx:"owner_id,refDb=${schema},refTable=OWNERS${suffix},refColumn=id"`
	Code  string `sqlx:"code,unique,table=RULES${suffix}"`
}

type IdentifierEmbedded struct {
	Owner int `sqlx:"owner_id,refTable=OWNERS${suffix},refColumn=id"`
}

type identifierEmbeddedRow struct {
	ID int `sqlx:"id,primaryKey"`
	*IdentifierEmbedded
}

func TestSQLConstraintIdentifiersPreserveEmbeddedFields(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE OWNERS_TEST(id INTEGER PRIMARY KEY)", "INSERT INTO OWNERS_TEST VALUES(7)"))
	values, err := constant.New(map[string]string{"suffix": "_TEST"})
	require.NoError(t, err)
	ctx := constant.WithContext(t.Context(), values)
	service := validation.New(identifierValidationSource{db: h.DB})
	for _, tc := range []struct {
		embedded *IdentifierEmbedded
		failed   bool
	}{{&IdentifierEmbedded{Owner: 7}, false}, {&IdentifierEmbedded{Owner: 8}, true}, {nil, true}} {
		row := &identifierEmbeddedRow{ID: 1, IdentifierEmbedded: tc.embedded}
		result, err := service.Validate(ctx, row, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
		require.NoError(t, err)
		require.Equal(t, tc.failed, result.Failed)
		require.Same(t, tc.embedded, row.IdentifierEmbedded)
	}
}

type identifierValidationSource struct{ db *sql.DB }

func (s identifierValidationSource) ValidationConnection(context.Context, string) (validation.Connection, error) {
	return validation.Connection{DB: s.db}, nil
}

func TestSQLConstraintIdentifiersAreInstanceLocal(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(),
		"CREATE TABLE OWNERS_SOAK01(id INTEGER PRIMARY KEY)", "INSERT INTO OWNERS_SOAK01 VALUES(1)",
		"CREATE TABLE OWNERS_PROD(id INTEGER PRIMARY KEY)", "INSERT INTO OWNERS_PROD VALUES(2)",
		"CREATE TABLE RULES_SOAK01(id INTEGER PRIMARY KEY,owner_id INTEGER,code TEXT UNIQUE)", "INSERT INTO RULES_SOAK01 VALUES(10,1,'soak-existing')",
		"CREATE TABLE RULES_PROD(id INTEGER PRIMARY KEY,owner_id INTEGER,code TEXT UNIQUE)", "INSERT INTO RULES_PROD VALUES(10,2,'prod-existing')"))
	service := validation.New(identifierValidationSource{db: h.DB})
	before := reflect.TypeFor[identifierRow]().Field(1).Tag
	for _, tc := range []struct {
		suffix, existing, other string
		owner, absent           int
	}{
		{"_SOAK01", "soak-existing", "prod-existing", 1, 2},
		{"_PROD", "prod-existing", "soak-existing", 2, 1},
	} {
		t.Run(tc.suffix, func(t *testing.T) {
			t.Parallel()
			values, err := constant.New(map[string]string{"schema": "main", "suffix": tc.suffix})
			require.NoError(t, err)
			ctx := constant.WithContext(t.Context(), values)
			policy := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}
			for i := 0; i < 5; i++ {
				result, err := service.Validate(ctx, &identifierRow{ID: 20, Owner: tc.owner, Code: tc.other}, policy)
				require.NoError(t, err)
				require.False(t, result.Failed, "%+v", result)
				result, err = service.Validate(ctx, &identifierRow{ID: 20, Owner: tc.absent, Code: tc.existing}, policy)
				require.NoError(t, err)
				require.True(t, result.Failed)
				require.Len(t, result.Violations, 2)
			}
			batch, err := service.Validate(ctx, []*identifierRow{{ID: 21, Owner: tc.owner, Code: "new"}, {ID: 22, Owner: tc.owner, Code: tc.existing}}, []xhandler.ValidationOptions{policy, policy})
			require.NoError(t, err)
			require.True(t, batch.Failed)
			require.Len(t, batch.Violations, 1)
			update := xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, Fields: fields{"Code": true}, Previous: &identifierRow{ID: 10, Owner: tc.owner, Code: tc.existing}, PreviousFields: fields{"ID": true, "Owner": true, "Code": true}}
			result, err := service.Validate(ctx, &identifierRow{ID: 10, Owner: tc.owner, Code: tc.existing}, update)
			require.NoError(t, err)
			require.False(t, result.Failed, "%+v", result)
			require.Equal(t, before, reflect.TypeFor[identifierRow]().Field(1).Tag)
		})
	}
}
