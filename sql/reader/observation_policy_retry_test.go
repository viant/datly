package reader_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/testutil/sqlfault"
	xexec "github.com/viant/xdatly/exec"
)

// Reuses the original native retry matrix and assertions with Source11 capture.
func TestObservationSourceOwnerRetryPathsSQLite(t *testing.T) {
	for _, path := range []string{"root", "root default", "relation", "partition", "relation partition", "output", "projection"} {
		for _, mode := range []string{"prepare", "query", "exhaust", "nonrecoverable", "cancel", "partial", "hook", "concurrent"} {
			if mode == "concurrent" && !strings.Contains(path, "partition") {
				continue
			}
			if path == "projection" && mode == "hook" {
				continue
			}
			t.Run(path+"/"+mode, func(t *testing.T) {
				h := sqlite.New(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				require.NoError(t, h.ExecStatements(ctx,
					"CREATE TABLE parents(id INTEGER)", "INSERT INTO parents VALUES(1),(2),(3),(4)",
					"CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO children VALUES(1,1),(2,1),(3,2),(4,2),(5,3),(6,3),(7,4),(8,4)"))
				child := &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT id,parent_id FROM children WHERE $COLUMN_IN ORDER BY id", Bindings: &spec.ViewBindings{Connector: "selected"}}}
				root := &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents ORDER BY id", Bindings: &spec.ViewBindings{Connector: "selected"}}}
				if path == "root default" {
					root.Source.Bindings = nil
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Retry"}, RootView: root}
				if strings.HasPrefix(path, "relation") {
					root.Relations = []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: child}}
				}
				partitioned := strings.Contains(path, "partition")
				if partitioned {
					view := root
					if path == "relation partition" {
						view = child
					}
					view.Partitioning = &spec.Partitioning{Type: "retryPartitions", Concurrency: 1}
					if mode == "concurrent" {
						view.Partitioning.Concurrency = 2
					}
				}
				if path == "output" {
					// Two rows let the partial-row failure occur after an output
					// hook, even though the output holder keeps only its first row.
					root.Relations = []*spec.Relation{{Name: "total", Holder: "Total", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "total", Source: &spec.ViewSource{SQL: "SELECT id AS total FROM parents ORDER BY id", Bindings: &spec.ViewBindings{Connector: "selected"}}}}}
				}
				plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(retryOutput{}), DirectViewField: "Rows", TypeLookup: func(string) (reflect.Type, error) { return reflect.TypeOf(retryPartitions{}), nil }})
				require.NoError(t, err)
				var mu sync.Mutex
				prepares, queries, failures := 0, 0, 0
				perSQL := map[string]int{}
				var lastFailure time.Time
				cause := errors.New("driver: invalid connection")
				if mode == "nonrecoverable" {
					cause = errors.New("permission denied")
				}
				target := func(SQL string) bool {
					if strings.HasPrefix(path, "relation") {
						return strings.Contains(SQL, "children")
					}
					if path == "output" {
						return strings.Contains(SQL, "AS total")
					}
					return strings.Contains(SQL, "parents")
				}
				faultDB := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
					if !target(call.SQL) {
						return nil
					}
					mu.Lock()
					defer mu.Unlock()
					if call.Phase == "prepare" {
						prepares++
					}
					if call.Phase == "query" {
						queries++
					}
					if mode == "concurrent" && call.Phase == "query" && perSQL[call.SQL] < 2 {
						perSQL[call.SQL]++
						failures++
						lastFailure = time.Now()
						return cause
					}
					phase, row := "query", 0
					if mode == "prepare" {
						phase = "prepare"
					}
					if mode == "partial" {
						phase, row = "next", 1
					}
					limit := 1
					if mode == "exhaust" {
						limit = 3
					}
					if mode != "hook" && mode != "concurrent" && failures < limit && call.Phase == phase && call.Row == row {
						failures++
						lastFailure = time.Now()
						if mode == "cancel" {
							cancel()
						}
						return cause
					}
					return nil
				})
				// The default is deliberately a different, empty database. Both
				// initial execution and retry must honor the authored named source.
				other := sqlite.New(t)
				source := &dsql.SQLComponent{DB: other.DB}
				require.NoError(t, source.RegisterConnector("selected", faultDB))
				if path == "root default" {
					source.DB = faultDB
				}
				execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(retryOutput{}), Plan: plan, SQL: source})
				require.NoError(t, err)
				policy := &observability.Policy{}
				for _, target := range execution.ObservationTargets() {
					identity, identityErr := target.View.Identity()
					require.NoError(t, identityErr)
					policy.Views = append(policy.Views, observability.ViewObservation{Component: component.Key, ViewIdentity: identity, Operation: &observability.OperationDescriptor{Name: "platform.retry." + target.View.Name, Location: "platform/retry", Description: target.View.Name + " performance", Provider: observability.Source11}})
				}
				owner := observability.NewRecorder(nil, observability.WithPolicy(policy))
				require.NoError(t, owner.ValidateTargets(execution.ObservationTargets()))
				execution = execution.WithRecorder(owner).(*reader.Execution)
				hooks := &retryHooks{rows: map[string]map[int]int{}}
				if mode == "hook" {
					hooks.fail = "root"
					if strings.HasPrefix(path, "relation") {
						hooks.fail = "relation"
					}
					if path == "output" {
						hooks.fail = "output"
					}
				}
				ec := xexec.New()
				ctx = context.WithValue(xexec.WithContext(ctx, ec), retryHooksKey{}, hooks)
				success := mode == "prepare" || mode == "query" || mode == "concurrent"
				if path == "projection" {
					prepared, prepareErr := execution.PrepareQuery(ctx, &struct{}{}, nil, nil)
					require.NoError(t, prepareErr)
					var value any
					value, err = prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: prepared.SQL, Args: prepared.Args, RowType: reflect.TypeOf(retryParent{})})
					if success {
						require.Len(t, value.([]*retryParent), 4)
					}
				} else {
					var result *reader.ReadResult
					result, err = execution.ReadResult(ctx, &struct{}{}, nil, nil)
					if success {
						require.NoError(t, err)
						out := result.Data.(*retryOutput)
						require.Len(t, out.Rows, 4)
						for i, row := range out.Rows {
							require.Equal(t, i+1, row.ID)
							evidence, e := result.Projection.Row(i)
							require.NoError(t, e)
							require.True(t, evidence.Fields().Has("ID"))
							require.False(t, evidence.Fields().Has("Missing"))
							if strings.HasPrefix(path, "relation") {
								require.Len(t, row.Children, 2)
								for j, child := range row.Children {
									require.Equal(t, i+1, child.ParentID)
									fields, e := evidence.RelationRow("Children", j)
									require.NoError(t, e)
									require.True(t, fields.Fields().Has("ID"))
									require.True(t, fields.Fields().Has("ParentID"))
								}
							}
						}
						if path == "output" {
							require.Equal(t, 1, out.Total.Total)
						}
					} else {
						require.Nil(t, result)
					}
				}
				if success {
					require.NoError(t, err)
				} else if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				} else if mode == "partial" || mode == "hook" {
					require.ErrorIs(t, err, sqlxread.ErrRetryUnsafe)
				} else {
					require.ErrorIs(t, err, cause)
				}
				mu.Lock()
				defer mu.Unlock()
				wantPrepares, wantQueries := 1, 1
				if mode == "exhaust" {
					wantPrepares, wantQueries = 3, 3
				}
				if success {
					wantPrepares = 2
					if mode == "query" {
						wantQueries = 2
					}
					if partitioned {
						wantPrepares++
						wantQueries++
					}
				}
				if mode == "concurrent" {
					wantPrepares, wantQueries = 6, 6
					require.Equal(t, 4, failures)
				}
				if mode == "concurrent" {
					// database/sql may prepare the same statement on another
					// pooled connection. Query attempts remain exactly bounded.
					require.GreaterOrEqual(t, prepares, wantPrepares)
					require.Len(t, perSQL, 2)
					for _, count := range perSQL {
						require.Equal(t, 2, count)
					}
				} else {
					require.Equal(t, wantPrepares, prepares)
				}
				require.Equal(t, wantQueries, queries)
				for _, rows := range hooks.rows {
					for _, count := range rows {
						require.Equal(t, 1, count, "row hook duplicated")
					}
				}
				if path != "projection" && success {
					require.Len(t, hooks.rows["root"], 4)
				}
				// One final observation per logical query, with failed attempts
				// included in its timing and only the final error reported.
				logical := 0
				for _, metric := range ec.Metrics {
					for _, query := range metric.Executions {
						if !target(query.SQL) {
							continue
						}
						logical++
						if mode != "concurrent" {
							require.False(t, query.EndTime.Before(lastFailure))
						}
						require.False(t, query.EndTime.Before(query.StartTime))
						require.Equal(t, !success, query.Error != "")
					}
				}
				wantLogical := 1
				if partitioned && success {
					wantLogical = 2
				}
				require.Equal(t, wantLogical, logical)
				for _, metric := range ec.Metrics {
					name := "platform.retry." + metric.View
					values := owner.Values(name)
					require.Len(t, values, 11)
					require.Zero(t, values["Pending"])
					require.EqualValues(t, 1, owner.Cumulative(name, "count"), "each logical view begins once across SQL retries")
					if metric.Error == "" {
						require.EqualValues(t, 1, values["Success"])
						require.Zero(t, values["Error"])
					} else {
						require.EqualValues(t, 1, values["Error"])
						require.Zero(t, values["Success"])
					}
					require.Empty(t, owner.Values(component.Key.String()+"/"+metric.View), "source capture cannot duplicate native counts")
				}
			})
		}
	}
}
