package sequencer

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"os"
	"testing"
)

func TestService_Next(t *testing.T) {

	_ = os.Remove("/tmp/datly_sequnece_test.db")
	dsn := "/tmp/datly_sequnece_test.db"
	db, err := sql.Open("sqlite3", dsn)
	if !assert.Nil(t, err) {
		return
	}
	type Emp struct {
		ID   int64  `sqlx:"ID,primaryKey=true"`
		Name string `sqlx:"NAME"`
	}

	var testCases = []struct {
		description string
		table       string
		initSQL     []string
		selector    string
		value       interface{}
		expect      interface{}
	}{
		{
			description: "Emp selector - empty table",
			table:       "EMP",
			initSQL: []string{
				"DROP TABLE IF EXISTS EMP",
				"CREATE TABLE EMP (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT)",
			},
			value: []*Emp{
				{
					Name: "abc",
				},
				{
					Name: "def",
				},
				{
					Name: "xyz",
				},
			},
			expect: []*Emp{
				{
					ID:   1,
					Name: "abc",
				},
				{
					ID:   2,
					Name: "def",
				},
				{
					ID:   3,
					Name: "xyz",
				},
			},
		},
		{
			description: "Emp selector - non empty table",
			table:       "EMP1",
			initSQL: []string{
				"DROP TABLE IF EXISTS EMP1",
				"CREATE TABLE EMP1(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT)",
				"INSERT INTO EMP1(ID, NAME) VALUES(121, 'xxx')",
			},
			value: []*Emp{
				{
					Name: "abc",
				},
				{
					Name: "def",
				},
				{
					Name: "xyz",
				},
			},
			expect: []*Emp{
				{
					ID:   122,
					Name: "abc",
				},
				{
					ID:   123,
					Name: "def",
				},
				{
					ID:   124,
					Name: "xyz",
				},
			},
		},
	}
	for _, testCase := range testCases {
		for i, _ := range testCase.initSQL {
			_, err = db.Exec(testCase.initSQL[i])
			if !assert.Nil(t, err, testCase.description) {
				return
			}
		}
		srv := New(context.Background(), db)
		err = srv.Next(testCase.table, testCase.value, "ID")
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

	}
}

func TestService_NextMySQL(t *testing.T) {
	//os.Setenv("TEST_MYSQL_DSN", "root:dev@tcp(127.0.0.1)/dev")
	dsn, skip := getTestConfig(t)
	if skip {
		return
	}
	db, err := sql.Open("mysql", dsn)
	if !assert.Nil(t, err) {
		return
	}
	type Emp struct {
		ID   int64  `sqlx:"ID,primaryKey=true"`
		Name string `sqlx:"NAME"`
	}

	var testCases = []struct {
		description string
		table       string
		initSQL     []string
		selector    string
		value       interface{}
		expect      interface{}
	}{
		{
			description: "Emp selector - empty table",
			table:       "EMP",
			initSQL: []string{
				"DROP TABLE IF EXISTS EMP",
				"CREATE TABLE EMP (ID INTEGER AUTO_INCREMENT PRIMARY KEY, NAME TEXT)",
			},
			value: []*Emp{
				{
					Name: "abc",
				},
				{
					Name: "def",
				},
				{
					Name: "xyz",
				},
			},
			expect: []*Emp{
				{
					ID:   1,
					Name: "abc",
				},
				{
					ID:   2,
					Name: "def",
				},
				{
					ID:   3,
					Name: "xyz",
				},
			},
		},
		{
			description: "Emp selector - non empty table",
			table:       "EMP",
			initSQL: []string{
				"DROP TABLE IF EXISTS EMP",
				"CREATE TABLE EMP (ID INTEGER AUTO_INCREMENT PRIMARY KEY, NAME TEXT)",
				"INSERT INTO EMP(ID, NAME) VALUES(121, 'xxx')",
			},
			value: []*Emp{
				{
					Name: "abc",
				},
				{
					Name: "def",
				},
				{
					Name: "xyz",
				},
			},
			expect: []*Emp{
				{
					ID:   122,
					Name: "abc",
				},
				{
					ID:   123,
					Name: "def",
				},
				{
					ID:   124,
					Name: "xyz",
				},
			},
		},
	}
	for _, testCase := range testCases {

		for _, SQL := range testCase.initSQL {
			_, err = db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				return
			}
		}
		srv := New(context.Background(), db)
		err = srv.Next("EMP", testCase.value, "ID")
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

	}
}

func getTestConfig(t *testing.T) (dsn string, shallSkip bool) {
	dsn = os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set TEST_MYSQL_DSN before running test")
		return "", true
	}
	return dsn, false
}
