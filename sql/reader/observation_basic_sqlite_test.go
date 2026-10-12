package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
)

func TestBasicObservationSuccessfulAndFailedSQLRead(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Basic"}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
	typ := reflect.TypeFor[[]*row]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	policy := &observability.Policy{}
	for _, target := range e.ObservationTargets() {
		id, err := target.View.Identity()
		require.NoError(t, err)
		policy.Views = append(policy.Views, observability.ViewObservation{Component: component.Key, ViewIdentity: id, Operation: &observability.OperationDescriptor{Name: "original.records", Location: "original", Description: "records performance", Provider: observability.Basic2}})
	}
	owner := observability.NewRecorder(nil, observability.WithPolicy(policy))
	e = e.WithRecorder(owner).(*reader.Execution)
	value, err := e.Read(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 7, value.([]*row)[0].ID)
	require.Equal(t, map[string]int64{"error": 0, "pending": 0}, owner.Values("original.records"))
	require.NoError(t, h.ExecStatements(ctx, "DROP TABLE records"))
	_, err = e.Read(ctx, &struct{}{}, nil, nil)
	require.Error(t, err)
	require.Equal(t, map[string]int64{"error": 1, "pending": 0}, owner.Values("original.records"))
	require.EqualValues(t, 2, owner.Cumulative("original.records", "count"))
}
