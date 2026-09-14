package reader_test

import (
	"context"
	"database/sql"

	"fmt"

	"reflect"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
)

type obsRewriteRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type obsRewriteOutput struct {
	Rows []*obsRewriteRow `parameter:"Rows,kind=output,in=view" view:"records" sql:"SELECT id,name FROM records ORDER BY id"`
}
type obsRewriteFixture struct {
	app     *runtime.Runtime
	request exec.ComponentRequest
	db      *sql.DB
}

func newObsRewriteFixture(options ...runtime.Option) *obsRewriteFixture {
	db, err := sql.Open("sqlite3", "file:rewrite-observability?mode=memory&cache=shared")
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	harness := &sqlite.Harness{DB: db}
	if err = harness.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<16) INSERT INTO records SELECT i,'row' FROM n"); err != nil {
		panic(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Observability"}, Name: "Observability", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}
	app, artifact, err := (typedGraphFixture{db: harness, component: component, output: reflect.TypeOf(obsRewriteOutput{})}).compile(options...)
	if err != nil {
		panic(err)
	}
	return &obsRewriteFixture{app: app, db: db, request: exec.ComponentRequest{Target: exec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}}}
}
func (f *obsRewriteFixture) invoke(ctx context.Context) error {
	v, err := f.app.InvokeComponent(ctx, f.request)
	if err != nil {
		return err
	}
	out, ok := v.(*obsRewriteOutput)
	if !ok || len(out.Rows) != 16 || out.Rows[0].ID != 1 {
		return fmt.Errorf("unexpected typed result %T", v)
	}
	return nil
}
