package reader_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx/testutil/sqlfault"
	structjson "github.com/viant/structology/encoding/json"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

type membershipRow struct {
	ID        int    `sqlx:"ID" json:"id"`
	Name      string `sqlx:"NAME" internal:"true" json:"-"`
	Label     string `sqlx:"label" source:"legacy" json:"label"`
	Canonical string `sqlx:"canonical" source:"canonical" json:"canonical"`
}
type membershipInput struct{ IncludeName bool }

type membershipSeedKey struct{}
type membershipParent struct {
	ID       *int             `sqlx:"ID" json:"id"`
	Children []*membershipRow `sqlx:"-" json:"children"`
}

func (p *membershipParent) OnFetch(ctx context.Context) error {
	if seeded, _ := ctx.Value(membershipSeedKey{}).(bool); seeded {
		p.Children = []*membershipRow{{ID: 1, Label: "must remain hidden", Name: "internal"}}
	}
	return nil
}

type membershipEvaluator struct {
	native  sqltemplate.Evaluator
	calls   atomic.Int32
	failure error
}

type membershipBinder struct {
	sourceGuardBinder
	input any
}

func (b membershipBinder) Bind(ctx context.Context, value any, options ...bindly.BindOption) error {
	return b.sourceGuardBinder.Bind(ctx, value, options...)
}
func (b membershipBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == xhandler.InputKey {
		return b.input, true, nil
	}
	return b.sourceGuardBinder.Lookup(ctx, key)
}

func (e *membershipEvaluator) Evaluate(ctx context.Context, invocation sqltemplate.Invocation) (sqltemplate.Result, error) {
	e.calls.Add(1)
	if e.failure != nil {
		return sqltemplate.Result{}, e.failure
	}
	return e.native.Evaluate(ctx, invocation)
}

func TestOutputOnlyCheckedAndEvaluatedMembershipSQLite(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		t.Run(map[bool]string{false: "predicate", true: "dynamic"}[dynamic], func(t *testing.T) {
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE membership_records(ID INTEGER,NAME TEXT)", "INSERT INTO membership_records VALUES(1,'hidden')"))
			var queries atomic.Int32
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "query" && strings.Contains(call.SQL, "membership_records") {
					queries.Add(1)
				}
				return nil
			})
			source := `SELECT ID,NAME,'visible' AS label,'canonical' AS canonical FROM membership_records ${predicate.Builder().CombineAnd($predicate.FilterGroup(99,"AND")).Build("WHERE")}`
			if dynamic {
				source = `SELECT ID,'visible' AS label,'canonical' AS canonical #if($Unsafe.IncludeName),NAME #end FROM membership_records`
			}
			component := &spec.Component{RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true, FieldSelection: spec.FieldSelectionOutputOnly}, Source: &spec.ViewSource{SQL: source}}}
			typ := reflect.TypeFor[[]*membershipRow]()
			predicate, err := readerpredicate.Compile(readerpredicate.CompileInput{})
			require.NoError(t, err)
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[membershipInput](), OutputType: typ, DirectViewType: typ, Predicate: predicate})
			require.NoError(t, err)
			evaluator := &membershipEvaluator{native: plan.Root.Template}
			plan.Root.Template = evaluator
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[membershipInput](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}})
			require.NoError(t, err)
			read := func(input *membershipInput, selector xstate.Selector) (any, []byte, error) {
				call, complete := dexec.ScopeOutputSelection(dexec.CaptureOutputSelection(t.Context()))
				dexec.BeginOutputSelection(call)
				value, err := execution.Read(call, input, membershipBinder{sourceGuardBinder: sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "records", Selector: selector}}}, input: input}, nil)
				if err != nil {
					return nil, nil, err
				}
				complete(value, nil)
				filter, available := dexec.CurrentOutputSelection(call, value)
				require.True(t, available)
				encoded, err := structjson.MarshalStandard(value, structjson.WithPathFieldExcluder(filter))
				return value, encoded, err
			}
			for iteration := 0; iteration < 2; iteration++ {
				queries.Store(0)
				evaluator.calls.Store(0)
				_, encoded, err := read(&membershipInput{IncludeName: true}, xstate.Selector{Fields: []string{"legacy"}})
				require.NoError(t, err, "source-only aliases use the same checked/evaluated membership for masking")
				require.JSONEq(t, `[{}]`, string(encoded))
				require.EqualValues(t, 1, queries.Load())
				require.EqualValues(t, 1, evaluator.calls.Load())
			}
			_, encoded, err := read(&membershipInput{IncludeName: true}, xstate.Selector{Fields: []string{"canonical"}})
			require.NoError(t, err, "canonical public metadata never reparses dynamic SQL for a same-name source tag")
			require.JSONEq(t, `[{"canonical":"canonical"}]`, string(encoded))
			for iteration := 0; iteration < 3; iteration++ {
				queries.Store(0)
				evaluator.calls.Store(0)
				value, encoded, err := read(&membershipInput{IncludeName: true}, xstate.Selector{Fields: []string{"NAME", "id"}})
				require.NoError(t, err)
				require.Equal(t, "hidden", value.([]*membershipRow)[0].Name)
				require.JSONEq(t, `[{"id":1}]`, string(encoded))
				require.EqualValues(t, 1, queries.Load())
				require.EqualValues(t, 1, evaluator.calls.Load(), "one native template evaluation per complete read")
			}
			queries.Store(0)
			evaluator.calls.Store(0)
			_, _, err = read(&membershipInput{IncludeName: true}, xstate.Selector{Fields: []string{"MiSsInG"}})
			var response *xresponse.Error
			var unknown *dsql.UnknownProjectionColumnError
			require.ErrorAs(t, err, &response)
			require.Equal(t, 400, response.Code)
			require.ErrorAs(t, err, &unknown)
			require.Equal(t, "MiSsInG", unknown.RequestedColumn)
			require.Zero(t, queries.Load())
			if dynamic {
				require.EqualValues(t, 1, evaluator.calls.Load())
			} else {
				require.Zero(t, evaluator.calls.Load(), "checked membership rejects before evaluation")
			}
			// Trusted Columns still narrows internal reads through the normal builder.
			value, _, err := read(&membershipInput{IncludeName: true}, xstate.Selector{Fields: []string{"id"}, Columns: []string{"ID"}})
			require.NoError(t, err)
			require.Empty(t, value.([]*membershipRow)[0].Name)
			_, _, err = read(&membershipInput{IncludeName: true}, xstate.Selector{Columns: []string{"missing"}})
			require.ErrorAs(t, err, &unknown)
			if dynamic {
				// The evaluated output contract, not static Go metadata, owns membership.
				_, _, err = read(&membershipInput{}, xstate.Selector{Fields: []string{"NAME"}})
				require.ErrorAs(t, err, &unknown)
				evaluator.failure = context.Canceled
				_, _, err = read(&membershipInput{}, xstate.Selector{Fields: []string{"missing"}})
				require.True(t, errors.Is(err, context.Canceled))
				require.False(t, errors.As(err, &response))
			}
		})
	}
}

func TestOutputOnlyUnevaluatedChildUsesNativeCollectorEvidenceSQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE membership_records(ID INTEGER,NAME TEXT)"))
	var childQueries atomic.Int32
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "membership_records") {
			childQueries.Add(1)
		}
		return nil
	})
	component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: `SELECT NULL AS ID`}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ID"}}, View: &spec.View{Name: "children", Selector: &spec.Selector{AllowFields: true, FieldSelection: spec.FieldSelectionOutputOnly}, Source: &spec.ViewSource{SQL: `SELECT ID,'visible' AS label #if($Unsafe.IncludeName),NAME #end FROM membership_records`}}}}}}
	typ := reflect.TypeFor[[]*membershipParent]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[membershipInput](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[membershipInput](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}})
	require.NoError(t, err)
	for _, seeded := range []bool{false, true} {
		ctx, complete := dexec.ScopeOutputSelection(dexec.CaptureOutputSelection(context.WithValue(t.Context(), membershipSeedKey{}, seeded)))
		dexec.BeginOutputSelection(ctx)
		input := &membershipInput{IncludeName: true}
		value, err := execution.Read(ctx, input, membershipBinder{sourceGuardBinder: sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "children", Selector: xstate.Selector{Fields: []string{"legacy"}}}}}, input: input}, nil)
		if seeded {
			var unresolved *dsql.UnresolvedProjectionError
			require.ErrorAs(t, err, &unresolved, "populated hook holders cannot bypass an unresolved response mask")
			require.Nil(t, value)
		} else {
			require.NoError(t, err, "native empty collector evidence preserves legitimate empty child results")
			complete(value, nil)
			filter, available := dexec.CurrentOutputSelection(ctx, value)
			require.True(t, available)
			encoded, err := structjson.MarshalStandard(value, structjson.WithPathFieldExcluder(filter))
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "must remain hidden")
		}
		require.Zero(t, childQueries.Load(), "nil parent keys do not execute child SQL")
	}
}
