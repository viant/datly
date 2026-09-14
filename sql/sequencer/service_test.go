package sequencer

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

func TestServiceNextSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sequencer.db")
	_ = os.Remove(dbPath)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()

	type Emp struct {
		ID   int64  `sqlx:"ID,primaryKey=true"`
		Name string `sqlx:"NAME"`
	}

	testCases := []struct {
		description string
		table       string
		initSQL     []string
		value       interface{}
		expect      []int64
	}{
		{
			description: "empty table",
			table:       "EMP",
			initSQL: []string{
				"DROP TABLE IF EXISTS EMP",
				"CREATE TABLE EMP (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT)",
			},
			value:  []*Emp{{Name: "abc"}, {Name: "def"}, {Name: "xyz"}},
			expect: []int64{1, 2, 3},
		},
		{
			description: "non empty table",
			table:       "EMP1",
			initSQL: []string{
				"DROP TABLE IF EXISTS EMP1",
				"CREATE TABLE EMP1(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT)",
				"INSERT INTO EMP1(ID, NAME) VALUES(121, 'xxx')",
			},
			value:  []*Emp{{Name: "abc"}, {Name: "def"}, {Name: "xyz"}},
			expect: []int64{122, 123, 124},
		},
	}

	for _, testCase := range testCases {
		for _, stmt := range testCase.initSQL {
			if _, err := db.Exec(stmt); err != nil {
				t.Fatalf("%s: init failed: %v", testCase.description, err)
			}
		}
		srv := New(db)
		if err := srv.Allocate(context.Background(), testCase.table, testCase.value, "ID"); err != nil {
			t.Fatalf("%s: next failed: %v", testCase.description, err)
		}
		typed := testCase.value.([]*Emp)
		for i, exp := range testCase.expect {
			if typed[i].ID != exp {
				t.Fatalf("%s: expected id %d at index %d, got %d", testCase.description, exp, i, typed[i].ID)
			}
		}
	}
}
