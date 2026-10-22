package dml

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
)

func TestBufferedUpdateProof(t *testing.T) {
	dialect := &info.Dialect{Product: database.Product{Name: "MySQL"}}
	for _, useCase := range []struct {
		description, sql string
		expected         bool
	}{
		{"bulk", "UPDATE settings SET value = ?, other = ? WHERE id IN (?, ?)", true},
		{"singleton", "UPDATE settings SET value = ? WHERE id IN (?)", true},
		{"quoted", "UPDATE `settings` SET value = ? WHERE id = ?", true},
		{"whitespace", " UPDATE settings SET value = ? WHERE id = ? \n", true},
		{"same parent", "UPDATE parents SET value = ? WHERE id = ?", false},
		{"same case alias", "UPDATE PARENTS SET value = ? WHERE id = ?", false},
		{"qualified ambiguity", "UPDATE db.settings SET value = ? WHERE id = ?", false},
		{"multiple", "UPDATE settings SET value = ? WHERE id = ?; DELETE FROM parents", false},
		{"trailing", "UPDATE settings SET value = ? WHERE id = ? garbage", false},
		{"executable comment", "UPDATE settings SET value = ? WHERE id = ? /*!; DELETE FROM parents */", false},
		{"function", "UPDATE settings SET value = side_effect() WHERE id = ?", false},
		{"predicate function", "UPDATE settings SET value = ? WHERE id = side_effect()", false},
		{"subquery", "UPDATE settings SET value = ? WHERE id IN (SELECT id FROM parents)", false},
		{"alias", "UPDATE settings s SET value = ? WHERE id = ?", false},
		{"multiple targets", "UPDATE settings, parents SET value = ? WHERE id = ?", false},
		{"join", "UPDATE settings JOIN parents ON settings.id=parents.id SET value = ?", false},
		{"cte", "WITH x AS (SELECT 1) UPDATE settings SET value = ? WHERE id = ?", false},
		{"delete", "DELETE FROM settings WHERE id = ?", false},
		{"malformed", "UPDATE settings SET value = ? WHERE id IN (?, ?", false},
		{"empty", "", false},
	} {
		t.Run(useCase.description, func(t *testing.T) {
			require.Equal(t, useCase.expected, distinctBufferedUpdateSQL("parents", useCase.sql, dialect))
			require.False(t, distinctBufferedUpdateSQL("parents", useCase.sql, nil))
		})
	}
}

func TestBufferedReferenceAcrossBulkUpdate(t *testing.T) {
	for _, singleton := range []bool{false, true} {
		for _, rollback := range []bool{false, true} {
			name := "commit"
			if rollback {
				name = "rollback"
			}
			if singleton {
				name += "/singleton"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				h.DB.SetMaxOpenConns(1)
				require.NoError(t, h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER NOT NULL REFERENCES parents(id))", "CREATE TABLE settings(id INTEGER PRIMARY KEY,value INTEGER)", "INSERT INTO settings VALUES(1,0),(2,0)"))
				owner := NewData(h.DB)
				require.NoError(t, owner.BeginInvocation())
				defer owner.Complete(ctx, errors.New("cleanup"))
				require.NoError(t, owner.Start(ctx))
				parent := owner.ComponentData(ComponentImperative, "").(*Data)
				require.NoError(t, parent.Insert("parents", &bufferedParentRow{7}))
				parent.SealComponent()
				middle := owner.ComponentData(ComponentImperative, "").(*Data)
				sql := "UPDATE settings SET value = ? WHERE id IN (?, ?)"
				args := []any{1, 1, 2}
				rows := 2
				if singleton {
					sql = "UPDATE settings SET value = ? WHERE id IN (?)"
					args = []any{1, 1}
					rows = 1
				}
				require.NoError(t, middle.Execute(sql, args...))
				middle.SealComponent()
				child := owner.ComponentData(ComponentImperative, "").(*Data)
				row := &bufferedChildRow{11, 7}
				result, err := child.FrameworkValidator().Validate(ctx, row, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
				require.NoError(t, err)
				require.False(t, result.Failed, "%+v", result.Violations)
				var count int
				require.NoError(t, owner.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count))
				require.Zero(t, count, "validation must not drain")
				require.Len(t, middle.queue, 1)
				require.Equal(t, sql, middle.queue[0].dml)
				require.Equal(t, args, middle.queue[0].args)
				require.NoError(t, child.Insert("children", row))
				child.SealComponent()
				if rollback {
					require.NoError(t, owner.Execute("INSERT INTO missing_table VALUES(1)"))
				}
				err = owner.Complete(ctx, nil)
				if rollback {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				want := 1
				if rollback {
					want = 0
				}
				require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count))
				require.Equal(t, want, count)
				require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM children").Scan(&count))
				require.Equal(t, want, count)
				require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT SUM(value) FROM settings").Scan(&count))
				require.Equal(t, rows*want, count)
			})
		}
	}

}
