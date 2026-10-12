package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	engine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
)

type recoveryReadRow struct {
	ID    int              `sqlx:"id,primaryKey"`
	Label string           `sqlx:"label"`
	Has   *recoveryReadHas `setMarker:"true" sqlx:"-"`
}
type recoveryReadHas struct{ ID, Label bool }
type recoveryReadInput struct {
	Rows    []*recoveryReadRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=records"`
	Current []*recoveryReadRow `parameter:"Current,kind=view,in=Current" view:"Current,table=records"`
}
type recoveryReadOutput struct {
	Data   []*recoveryReadRow `parameter:"Data,kind=output,in=body"`
	Winner string
}
type recoveryWinnerInput struct{}
type recoveryWinnerOutput struct {
	Data []*recoveryReadRow `parameter:"Data,kind=output,in=view" view:"Winner"`
}
type actualRecoveryReader struct {
	*writer.Handler
	target    dexec.ComponentTarget
	held      dexec.ComponentInvoker
	ctx       context.Context
	calls     int
	mode      string
	forbidden dexec.ComponentTarget
}

func (*actualRecoveryReader) SupportsMutationRecovery() bool { return true }
func (h *actualRecoveryReader) RecoverMutation(ctx context.Context, inv rh.Invocation, result any, outcome rh.MutationOutcome) (rh.Recovery, error) {
	h.calls++
	h.ctx = ctx
	if !outcome.CommitConfirmed() || outcome.Mutation.Affected != 0 {
		return rh.RecoveryNone, fmt.Errorf("unexpected recovery evidence")
	}
	var dep struct {
		Invoker dexec.ComponentInvoker `bind:"kind=component_invoker,required"`
	}
	if err := inv.Binder.Bind(ctx, &dep); err != nil {
		return rh.RecoveryNone, err
	}
	h.held = dep.Invoker
	if h.mode == "cancel" {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := dep.Invoker.InvokeComponent(canceled, dexec.ComponentRequest{Target: h.target, Input: &recoveryWinnerInput{}})
		return rh.RecoveryNone, err
	}
	if h.mode == "writer" || h.mode == "custom" {
		_, err := dep.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: h.forbidden, Input: &struct{}{}})
		if !errors.Is(err, engine.ErrRootRecoveryReadOnly) {
			return rh.RecoveryNone, fmt.Errorf("mutation target admitted: %v", err)
		}
		// Ignoring the denied operation must still fail the recovered root.
		return rh.RecoveryAccept, nil
	}
	value, err := dep.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: h.target, Input: &recoveryWinnerInput{}})
	if err != nil {
		return rh.RecoveryNone, err
	}
	rows := value.(*recoveryWinnerOutput).Data
	if len(rows) != 1 {
		return rh.RecoveryNone, fmt.Errorf("winner count%d", len(rows))
	}
	result.(*recoveryReadOutput).Winner = rows[0].Label
	return rh.RecoveryAccept, nil
}
func TestDefaultBufferedRootRecoveryReadsFreshCanonicalWinner(t *testing.T) {
	for _, mode := range []string{"read", "writer", "custom", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(1,'stored winner')", "CREATE TRIGGER ignore_update BEFORE UPDATE ON records BEGIN SELECT RAISE(IGNORE); END"); err != nil {
				t.Fatal(err)
			}
			readSpec := componentSpec("RecoveryWinner", "GET", "/recovery-winner", nil)
			readSpec.RootView = &spec.View{Name: "Winner", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records WHERE id=1"}}
			readArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: readSpec, InputType: reflect.TypeFor[recoveryWinnerInput](), OutputType: reflect.TypeFor[recoveryWinnerOutput](), DirectViewField: "Data"})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := readArtifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			writeSpec := componentSpec("RecoveryWrite", "PATCH", "/recovery-write", nil)
			writeSpec.Settings = &spec.Settings{Mutation: "patch"}
			writeSpec.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
			writeArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: writeSpec, InputType: reflect.TypeFor[recoveryReadInput](), OutputType: reflect.TypeFor[recoveryReadOutput]()})
			if err != nil {
				t.Fatal(err)
			}
			native, err := writer.New(writeArtifact.Component, reflect.TypeFor[recoveryReadInput](), reflect.TypeFor[recoveryReadOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			views, err := writeArtifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			probe := &actualRecoveryReader{mode: mode, Handler: native, target: dexec.ComponentTarget{Component: readSpec.Key, Route: spec.RouteRef{Method: "GET", Path: "/recovery-winner"}}}
			customCalls := 0
			customSpec := componentSpec("RecoveryCustom", "GET", "/recovery-custom", nil)
			customArtifact := componentArtifact(t, customSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
			probe.forbidden = dexec.ComponentTarget{Component: customSpec.Key, Route: spec.RouteRef{Method: "GET", Path: "/recovery-custom"}}
			if mode == "writer" {
				probe.forbidden = dexec.ComponentTarget{Component: writeSpec.Key, Route: spec.RouteRef{Method: "PATCH", Path: "/recovery-write"}}
			}
			rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: writeArtifact.Component, Input: writeArtifact.Input, Output: writeArtifact.Output, OutputType: reflect.TypeFor[recoveryReadOutput](), Handler: probe, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: db.DB}}, {Component: readArtifact.Component, Input: readArtifact.Input, Output: readArtifact.Output, OutputType: reflect.TypeFor[recoveryWinnerOutput](), Reader: reader}, {Component: customArtifact.Component, Input: customArtifact.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { customCalls++; return &struct{}{}, nil })}})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			result, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: writeSpec.Key, Route: spec.RouteRef{Method: "PATCH", Path: "/recovery-write"}}, Input: &recoveryReadInput{Rows: []*recoveryReadRow{{ID: 1, Label: "losing requested", Has: &recoveryReadHas{ID: true, Label: true}}}}})
			if mode != "read" {
				expected := engine.ErrRootRecoveryReadOnly
				if mode == "cancel" {
					expected = context.Canceled
				}
				if !errors.Is(err, expected) || probe.calls != 1 || customCalls != 0 {
					t.Fatalf("guard failure: %v calls%d effects%d", err, probe.calls, customCalls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if probe.calls != 1 || result.(*recoveryReadOutput).Winner != "stored winner" {
				t.Fatal("actual winner not adopted")
			}
			for _, late := range []context.Context{probe.ctx, context.Background()} {
				if _, err := probe.held.InvokeComponent(late, dexec.ComponentRequest{Target: probe.target, Input: &recoveryWinnerInput{}}); !errors.Is(err, engine.ErrRootRecoveryReadOnly) {
					t.Fatalf("recovery reader escaped hook lifetime: %v", err)
				}
			}
			var label string
			if err := db.DB.QueryRow("SELECT label FROM records WHERE id=1").Scan(&label); err != nil || label != "stored winner" {
				t.Fatal("winner changed")
			}
		})
	}
}
