package io

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/datly/config"
	iocontext "github.com/viant/datly/io/context"
	"github.com/viant/datly/metadata/view"
	"github.com/viant/toolbox/format"
	"reflect"

	"os"
	"path"
	"testing"
)

func TestReadInto(t *testing.T) {

	tempDir := os.TempDir()
	type T1 struct {
		ID   string
		Name string
	}
	var t1s []T1
	var t2s []*T1

	type Line1 struct {
		Id    int
		DocId int `sqlx:"doc_id"`
		Line  string
	}

	type Doc1 struct {
		Id   int
		Name  string
		Lines []Line1`datly:"table=lines,on=lines.doc_id=id"`
	}

	type Doc2 struct {
		Id   int
		Name  string
		Lines []*Line1`datly:"table=lines,on=lines.doc_id=id"`
	}

	var docs1 []Doc1
	var docs2 []Doc2

	type Item struct {
		Name string
		Action string
		ParentId int
		Sub []Item
	}
	type Menu struct {
		Item []Item
	}

	var testCases = []struct {
		description string
		connector   *config.Connector
		target      interface{}
		nameOrSQL   string
		initSQLs    []string
		expect      interface{}
	}{
		{
			initSQLs: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			connector: &config.Connector{
				Name:   "test",
				Driver: "sqlite3",
				DSN:    path.Join(tempDir, "/datly_reader.db"),
			},
			target:    &t1s,
			nameOrSQL: "t1",
			expect: `[{"Name":"1","Name":"John"},{"Name":"2","Name":"Bruce"}]`,
		},
		{
			initSQLs: []string{
				"CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from t1",
				"insert into t1 values(1, \"John\")",
				"insert into t1 values(2, \"Bruce\")",
			},
			connector: &config.Connector{
				Name:   "test",
				Driver: "sqlite3",
				DSN:    path.Join(tempDir, "/datly_reader.db"),
			},
			target:    &t2s,
			nameOrSQL: "t1",
			expect: ` [{"Name":"1","Name":"John"},{"Name":"2","Name":"Bruce"}]`,
		},
		{
			initSQLs: []string{
				"CREATE TABLE IF NOT EXISTS docs (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from docs",
				"insert into docs values(1, \"Doc1\")",
				"insert into docs values(2, \"Doc2\")",
				"CREATE TABLE IF NOT EXISTS lines (id INTEGER PRIMARY KEY, doc_id INTEGER, line TEXT)",
				"delete from lines",
				"insert into lines values(1,1, \"Line 1.1\")",
				"insert into lines values(2,1, \"Line 1.2\")",
				"insert into lines values(3,2, \"Line 2.1\")",
				"insert into lines values(4,2, \"Line 2.2\")",
			},
			connector: &config.Connector{
				Name:   "test",
				Driver: "sqlite3",
				DSN:    path.Join(tempDir, "/datly_reader.db"),
			},
			target:    &docs1,
			nameOrSQL: "docs",
			expect: `[{"Id":1,"Name":"Doc1","Lines":[{"Id":1,"DocId":1,"Line":"Line 1.1"},{"Id":2,"DocId":1,"Line":"Line 1.2"}]},{"Id":2,"Name":"Doc2","Lines":[{"Id":3,"DocId":2,"Line":"Line 2.1"},{"Id":4,"DocId":2,"Line":"Line 2.2"}]}]`,
		},
		{
			initSQLs: []string{
				"CREATE TABLE IF NOT EXISTS docs (id INTEGER PRIMARY KEY, name TEXT)",
				"delete from docs",
				"insert into docs values(1, \"Doc1\")",
				"insert into docs values(2, \"Doc2\")",
				"CREATE TABLE IF NOT EXISTS lines (id INTEGER PRIMARY KEY, doc_id INTEGER, line TEXT)",
				"delete from lines",
				"insert into lines values(1,1, \"Line 1.1\")",
				"insert into lines values(2,1, \"Line 1.2\")",
				"insert into lines values(3,2, \"Line 2.1\")",
				"insert into lines values(4,2, \"Line 2.2\")",
			},
			connector: &config.Connector{
				Name:   "test",
				Driver: "sqlite3",
				DSN:    path.Join(tempDir, "/datly_reader.db"),
			},
			target:    &docs2,
			nameOrSQL: "docs",
			expect: `[{"Id":1,"Name":"Doc1","Lines":[{"Id":1,"DocId":1,"Line":"Line 1.1"},{"Id":2,"DocId":1,"Line":"Line 1.2"}]},{"Id":2,"Name":"Doc2","Lines":[{"Id":3,"DocId":2,"Line":"Line 2.1"},{"Id":4,"DocId":2,"Line":"Line 2.2"}]}]`,
		},
	}

	for _, testCase := range testCases {
		err := initDB(testCase.connector, testCase.initSQLs)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		ctx, err := iocontext.WithConnectors(context.Background(), testCase.connector)
		if !assert.Nil(t, err, testCase.description) {
			fmt.Printf("%v\n", err)
			continue
		}
		ctx = iocontext.WithSession(ctx, iocontext.NewSession())
		aView, err := view.FromStruct(testCase.nameOrSQL, reflect.TypeOf(testCase.target), format.CaseLowerUnderscore)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		err = ReadInto(ctx, testCase.target, aView)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		if !assertly.AssertValues(t, testCase.expect, testCase.target, testCase.description) {
			data, _ := json.Marshal(testCase.target)
			fmt.Printf("data: %s\n", data)
		}
	}
}

func initDB(connector *config.Connector, initSQLs []string) error {
	db, err := sql.Open(connector.Driver, connector.DSN)

	if err != nil {
		return err
	}
	defer db.Close()
	for _, SQL := range initSQLs {
		_, err := db.Exec(SQL)
		if err != nil {
			return err
		}
	}
	return nil
}
