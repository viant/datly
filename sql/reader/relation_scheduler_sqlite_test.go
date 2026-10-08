package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/testutil/sqlfault"
)

type scheduledParent struct {
	ID          int                `sqlx:"id"`
	Memory      []*scheduledMemory `view:"memory" on:"ID:id=Owner:owner_id"`
	Independent []*scheduledLookup `view:"independent" on:"ID:id=ID:id" sql:"SELECT id FROM independent WHERE $COLUMN_IN"`
	Finished    bool               `sqlx:"-"`
}

type scheduledMemory struct {
	Owner    int              `sqlx:"owner_id"`
	Key      int              `sqlx:"key_id"`
	Lookup   *scheduledLookup `view:"nested" on:"Key:key_id=ID:id" sql:"SELECT id FROM nested WHERE $COLUMN_IN"`
	Finished bool             `sqlx:"-"`
}

type scheduledLookup struct {
	ID int `sqlx:"id"`
}

func (p *scheduledParent) OnFetch(context.Context) error {
	p.Memory = []*scheduledMemory{{Owner: p.ID, Key: 11}, {Owner: p.ID, Key: 22}}
	return nil
}

func (p *scheduledParent) OnRelation(context.Context) {
	p.Finished = len(p.Independent) == 1 && p.Independent[0].ID == p.ID && len(p.Memory) == 2
	for _, child := range p.Memory {
		p.Finished = p.Finished && child.Finished
	}
}

func (p *scheduledMemory) OnRelation(context.Context) {
	p.Finished = p.Lookup != nil && p.Lookup.ID == p.Key
}

func TestRelationSchedulerInMemoryNestedLookupSQLite(t *testing.T) {
	for _, concurrency := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(ctx,
				"CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(1)",
				"CREATE TABLE nested(id INTEGER)", "INSERT INTO nested VALUES(11),(22)",
				"CREATE TABLE independent(id INTEGER)", "INSERT INTO independent VALUES(1)"))
			nestedStarted := make(chan struct{}, 1)
			independentStarted := make(chan struct{}, 1)
			releaseNested := make(chan struct{})
			connection := db.FaultDB(t, func(ctx context.Context, call sqlfault.Call) error {
				if call.Phase != "query" || concurrency == 1 {
					return nil
				}
				if strings.Contains(call.SQL, "FROM nested") {
					nestedStarted <- struct{}{}
					select {
					case <-releaseNested:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				if strings.Contains(call.SQL, "FROM independent") {
					// This reader occupies a worker until the in-memory branch has
					// published and started its descendant on another worker.
					select {
					case <-nestedStarted:
					case <-ctx.Done():
						return ctx.Err()
					}
					independentStarted <- struct{}{}
				}
				return nil
			})
			typ := reflect.TypeFor[[]*scheduledParent]()
			component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}}}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: connection}}, reader.WithRelationFetchConcurrency(concurrency))
			require.NoError(t, err)
			var result any
			done := make(chan error, 1)
			go func() {
				var readErr error
				result, readErr = execution.Read(ctx, &struct{}{}, nil, nil)
				done <- readErr
			}()
			if concurrency > 1 {
				select {
				case <-independentStarted:
				case <-ctx.Done():
					t.Fatal("independent lookup blocked by nested lookup", ctx.Err())
				}
				select {
				case err := <-done:
					t.Fatalf("reader completed before nested SQL was released: %v", err)
				default:
				}
				close(releaseNested)
			}
			require.NoError(t, <-done)
			rows := result.([]*scheduledParent)
			require.Len(t, rows, 1)
			require.True(t, rows[0].Finished, "parent hook must see both lookups and completed child hooks")
			require.Len(t, rows[0].Memory, 2)
			for _, child := range rows[0].Memory {
				require.True(t, child.Finished)
				require.NotNil(t, child.Lookup)
				require.Equal(t, child.Key, child.Lookup.ID)
			}
		})
	}
}
