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

type viewPrecedenceEvaluation struct {
	ID int `sqlx:"id"`
}
type viewPrecedenceChild struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}
type viewPrecedenceRow struct {
	ID       int                    `sqlx:"id"`
	Name     string                 `sqlx:"name"`
	Children []*viewPrecedenceChild `view:"children" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id FROM children WHERE $COLUMN_IN ORDER BY id"`
}
type viewPrecedenceOutput struct {
	Evaluations []*viewPrecedenceEvaluation `parameter:"Evaluations,kind=output,in=body"`
	RawData     []*viewPrecedenceRow        `parameter:"RawData,kind=output,in=view"`
}

func TestExplicitViewOutputPreservesRowsAndRelationsAheadOfBodyFieldsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT)", "INSERT INTO parents VALUES(1,'first'),(2,'second')", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO children VALUES(11,1),(12,1),(21,2)"))
	component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,name FROM parents ORDER BY id"}}, Parameters: []*spec.Parameter{
		{Name: "Evaluations", Source: spec.BindSource{Kind: "output", Name: "body"}},
		{Name: "RawData", Source: spec.BindSource{Kind: "output", Name: "view"}},
	}}
	inputType, outputType := reflect.TypeFor[struct{}](), reflect.TypeFor[viewPrecedenceOutput]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	output := result.Data.(*viewPrecedenceOutput)
	require.Nil(t, output.Evaluations, "body fields belong to application hooks, not the root SQL result")
	require.Len(t, output.RawData, 2)
	require.Equal(t, "first", output.RawData[0].Name)
	require.Equal(t, "second", output.RawData[1].Name)
	require.Equal(t, []*viewPrecedenceChild{{ID: 11, ParentID: 1}, {ID: 12, ParentID: 1}}, output.RawData[0].Children)
	require.Equal(t, []*viewPrecedenceChild{{ID: 21, ParentID: 2}}, output.RawData[1].Children)
}
