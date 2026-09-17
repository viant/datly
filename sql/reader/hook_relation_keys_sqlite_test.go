package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
)

type hookKeyChild struct {
	ID     int    `sqlx:"id"`
	Code   string `sqlx:"code"`
	Tenant int    `sqlx:"tenant"`
}

type hookKeyParent struct {
	ID        int             `sqlx:"id"`
	Keys      []int           `sqlx:"-" relationKey:"hook"`
	Codes     []string        `sqlx:"-" relationKey:"hook"`
	Tenant    int             `sqlx:"-" relationKey:"hook"`
	Children  []*hookKeyChild `view:"Children" on:"Keys:keys=ID:id" sql:"SELECT id,code,tenant FROM children ORDER BY id"`
	Named     []*hookKeyChild `view:"Named" on:"Codes:codes=Code:code" sql:"SELECT id,code,tenant FROM children ORDER BY id"`
	Composite []*hookKeyChild `view:"Composite" on:"Tenant:tenant=Tenant:tenant,Keys:keys=ID:id" sql:"SELECT id,code,tenant FROM children ORDER BY id"`
}

func (r *hookKeyParent) OnFetch(context.Context) error {
	switch r.ID {
	case 1:
		r.Keys, r.Codes, r.Tenant = []int{17, 23}, []string{"alpha"}, 7
	case 2:
		r.Keys, r.Codes, r.Tenant = []int{31}, []string{"beta"}, 8
	}
	return nil
}

func TestHookPopulatedRelationKeysSQLite(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "populated", true: "empty_skips_missing_child_table"}[empty], func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER)"))
			if empty {
				require.NoError(t, h.ExecStatements(ctx, "INSERT INTO parents VALUES(3)"))
			} else {
				require.NoError(t, h.ExecStatements(ctx,
					"INSERT INTO parents VALUES(1),(2),(3)",
					"CREATE TABLE children(id INTEGER,code TEXT,tenant INTEGER)",
					"INSERT INTO children VALUES(0,'',0),(17,'alpha',7),(23,'other',99),(31,'beta',8)"))
			}
			typ := reflect.TypeFor[[]*hookKeyParent]()
			component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,'' AS keys,'' AS codes,0 AS tenant FROM parents ORDER BY id"}}}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			for pass := 0; pass < 2; pass++ {
				result, err := execution.Read(ctx, &struct{}{}, nil, nil)
				require.NoError(t, err)
				rows := result.([]*hookKeyParent)
				if empty {
					require.Len(t, rows, 1)
				} else {
					require.Len(t, rows, 3)
					require.Equal(t, []int{17, 23}, hookChildIDs(rows[0].Children))
					require.Equal(t, []int{17}, hookChildIDs(rows[0].Named))
					require.Equal(t, []int{17}, hookChildIDs(rows[0].Composite))
					for _, children := range [][]*hookKeyChild{rows[1].Children, rows[1].Named, rows[1].Composite} {
						require.Equal(t, []int{31}, hookChildIDs(children))
					}
				}
				last := rows[len(rows)-1]
				require.Empty(t, last.Children)
				require.Empty(t, last.Named)
				require.Empty(t, last.Composite)
			}
		})
	}
}

func hookChildIDs(rows []*hookKeyChild) []int {
	var ids []int
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	return ids
}
