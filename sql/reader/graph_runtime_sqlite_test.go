package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/io/read/cache"
	xshape "github.com/viant/x/shape"
)

type typedGraphFixture struct {
	caches    func(*bootstrap.Artifact) map[*data.View]cache.Cache
	db        *sqlite.Harness
	component *spec.Component
	output    reflect.Type
	types     *typecatalog.Catalog
}

func (f typedGraphFixture) invoke(t *testing.T, ctx context.Context) any {
	t.Helper()
	app, artifact, err := f.compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (f typedGraphFixture) compile(options ...runtime.Option) (*runtime.Runtime, *bootstrap.Artifact, error) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: reflect.TypeOf(struct{}{}), OutputType: f.output, Types: f.types})
	if err != nil {
		return nil, nil, err
	}
	var caches map[*data.View]cache.Cache
	if f.caches != nil {
		caches = f.caches(artifact)
	}
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeOf(struct{}{}), OutputType: f.output, Plan: artifact.Reader, ReadCaches: caches, SQL: &dsql.SQLComponent{DB: f.db.DB}})
	if err != nil {
		return nil, nil, err
	}
	app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: f.output, Reader: execution}}, options...)
	if err != nil {
		return nil, nil, err
	}
	return app, artifact, nil
}

func TestDeepGoShapeGraphThroughRuntimeSQLite(t *testing.T) {
	for _, depth := range []int{1, 16, 64} {
		for _, links := range []string{"ID:id=ParentID:parent_id", "id=parent_id"} {
			t.Run(fmt.Sprint(depth)+"/"+links, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE nodes(id INTEGER,parent_id INTEGER,level INTEGER)", fmt.Sprintf("WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<%d) INSERT INTO nodes SELECT i+1,i,i FROM n", depth-1)); err != nil {
					t.Fatal(err)
				}
				var child reflect.Type
				for level := depth - 1; level >= 0; level-- {
					fields := []xshape.RuntimeField{{Name: "ID", Type: reflect.TypeOf(0), Tag: `sqlx:"id"`}, {Name: "ParentID", Type: reflect.TypeOf(0), Tag: `sqlx:"parent_id"`}}
					if child != nil {
						fields = append(fields, xshape.RuntimeField{Name: "Children", Type: reflect.SliceOf(reflect.PointerTo(child)), Tag: reflect.StructTag(fmt.Sprintf("sqlx:\"-\" view:\"Level%d\" on:%q sql:%q", level+1, links, fmt.Sprintf("SELECT id,parent_id FROM nodes WHERE level=%d AND $COLUMN_IN", level+1)))})
					}
					var err error
					child, err = (xshape.Runtime{}).Struct(fields)
					if err != nil {
						t.Fatal(err)
					}
				}
				output, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "Rows", Type: reflect.SliceOf(reflect.PointerTo(child)), Tag: `parameter:"Rows,kind=output,in=view" view:"Level0" sql:"SELECT id,parent_id FROM nodes WHERE level=0"`}})
				if err != nil {
					t.Fatal(err)
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Deep"}, Name: "Deep", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}
				actual := (typedGraphFixture{db: db, component: component, output: output}).invoke(t, ctx)
				rowsAccessor, err := xshape.Linked(output).Accessor("Rows")
				if err != nil {
					t.Fatal(err)
				}
				rows, err := rowsAccessor.Get(actual)
				if err != nil {
					t.Fatal(err)
				}
				for level := 0; level < depth; level++ {
					if rows.Len() != 1 {
						t.Fatalf("depth %d has %d rows", level, rows.Len())
					}
					row := rows.Index(0).Interface()
					shape := xshape.Linked(reflect.TypeOf(row))
					idAccessor, err := shape.Accessor("ID")
					if err != nil {
						t.Fatal(err)
					}
					id, err := idAccessor.Get(row)
					if err != nil || id.Int() != int64(level+1) {
						t.Fatalf("depth %d ID %v error %v", level, id, err)
					}
					if level+1 < depth {
						children, err := shape.Accessor("Children")
						if err != nil {
							t.Fatal(err)
						}
						rows, err = children.Get(row)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
			})
		}
	}
}
