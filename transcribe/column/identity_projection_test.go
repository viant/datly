package column

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

func TestNamedIdentityProjectionSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, `CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY,NAME TEXT)`, `INSERT INTO ITEMS VALUES(1,'visible'),(2,'hidden')`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, SQL, key string
		internal, fail bool
	}{
		{"retain", "SELECT items.NAME FROM (SELECT i.* FROM ITEMS i WHERE i.ID=1) items", "ID", true, false},
		{"CTE source", "WITH visible AS (SELECT i.* FROM ITEMS i WHERE i.ID=1) SELECT items.NAME FROM visible items", "ID", true, false},
		{"source alias", "SELECT items.NAME FROM (SELECT i.ID AS RowKey,i.NAME FROM ITEMS i WHERE i.ID=1) items", "RowKey", true, false},
		{"projected alias", "SELECT items.ID AS RowKey,items.NAME FROM (SELECT i.* FROM ITEMS i WHERE i.ID=1) items", "RowKey", false, false},
		{"inner projection omission", "SELECT items.NAME FROM (SELECT i.NAME FROM ITEMS i WHERE i.ID=1) items", "", false, false},
		{"computed source key", "SELECT items.NAME FROM (SELECT i.ID+10 AS ID,i.NAME FROM ITEMS i WHERE i.ID=1) items", "", false, false},
		{"shadowed key", "SELECT items.NAME,items.NAME AS ID FROM (SELECT i.* FROM ITEMS i WHERE i.ID=1) items", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Items", Namespace: "items", Source: &spec.ViewSource{Table: "ITEMS", SQL: tc.SQL}}
			err := New(Connections{"main": db.DB}).RefineRoot(ctx, &spec.Component{Name: "Items", RootView: view, Settings: &spec.Settings{DefaultConnector: "main"}}, nil, nil)
			if tc.fail {
				if err == nil || !strings.Contains(err.Error(), "backing identity") {
					t.Fatalf("shadowed identity: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, column := range view.Columns {
				if column.PrimaryKey {
					found = true
					if column.Name != tc.key || column.Source != "ID" || (reflect.StructTag(column.Tag).Get("internal") == "true") != tc.internal {
						t.Fatalf("key=%+v", column)
					}
				}
			}
			if found != (tc.key != "") {
				t.Fatalf("identity broadened or lost: %+v", view.Columns)
			}
			var count int
			if err := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+view.Source.SQL+")").Scan(&count); err != nil || count != 1 {
				t.Fatalf("source scope: count=%d err=%v SQL=%s", count, err, view.Source.SQL)
			}
		})
	}
}
