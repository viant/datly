package dml

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	sqldialect "github.com/viant/sqlx/metadata/info/dialect"
)

func TestAdjacentCollectionInsertsPreserveRowsOrderAndRollback(t *testing.T) {
	type row struct {
		Id    int    `sqlx:"id,primaryKey"`
		Label string `sqlx:"label"`
	}
	for _, duplicate := range []bool{false, true} {
		name := "success"
		if duplicate {
			name = "duplicate rollback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			h := testharness.NewSQLiteHarness(t)
			require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT)", "CREATE TABLE journal(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER)", "CREATE TRIGGER record_order AFTER INSERT ON records BEGIN INSERT INTO journal(id) VALUES(new.id); END"))
			data := NewData(h.DB)
			dialect, err := data.dialectFor(ctx, h.DB)
			require.NoError(t, err)
			forced := *dialect
			forced.Insert = sqldialect.InsertWithMultiValues
			data.dialect = &forced
			require.NoError(t, data.Execute("INSERT INTO journal(id) VALUES(99)"))
			first := []*row{{Id: 1, Label: "first"}, {Id: 2, Label: "second"}}
			second := []*row{{Id: 3, Label: "third"}, {Id: 4, Label: "fourth"}}
			if duplicate {
				second[1].Id = 1
			}
			require.NoError(t, data.Insert("records", first))
			require.NoError(t, data.Insert("records", second))
			err = data.Flush(ctx, "")
			if duplicate {
				require.ErrorContains(t, err, "UNIQUE constraint")
				for _, table := range []string{"records", "journal"} {
					var count int
					require.NoError(t, h.DB.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count))
					require.Zero(t, count)
				}
				return
			}
			require.NoError(t, err)
			rows, err := h.DB.Query("SELECT id FROM journal ORDER BY seq")
			require.NoError(t, err)
			defer rows.Close()
			var order []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				order = append(order, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{99, 1, 2, 3, 4}, order)
		})
	}
}

func TestOrdinaryInsertBatchExcludesCollectionPayloads(t *testing.T) {
	type row struct{ Name string }
	record := row{Name: "one"}
	list := []row{record}
	array := [1]row{record}
	for _, tc := range []struct {
		name     string
		payload  any
		eligible bool
	}{
		{"record", record, true},
		{"record pointer", &record, true},
		{"slice", list, false},
		{"slice pointer", &list, false},
		{"array", array, false},
		{"array pointer", &array, false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operations := []*dataOperation{{kind: dataOpInsert, table: "records", data: tc.payload}, {kind: dataOpInsert, table: "records", data: tc.payload}}
			require.Equal(t, tc.eligible, canBatchInsert(operations))
		})
	}
}
