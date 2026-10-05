// Package capturederror supplies a generic native-writer transport regression.
// It uses the native Current view binder against disposable SQLite.
package capturederror

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
	xl "github.com/viant/xdatly/logger"
	"github.com/viant/xdatly/response"
)

type Record struct {
	ID int `sqlx:"id,primaryKey=true" json:"id"`
}
type Input struct {
	Rows        []*Record `parameter:"Rows,kind=body,in=data" json:"data" view:"Rows,table=records,auxiliary=true"`
	CurrentRows []*Record `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records" sql:"SELECT id FROM records ORDER BY id" json:"-"`
}

// SQLFailureInput has no initializer. Its empty Current view requests a native
// insert of an already-existing key, so failure occurs during SQL flush.
type SQLFailureInput struct {
	Rows        []*Record `parameter:"Rows,kind=body,in=data" json:"data" view:"Rows,table=records"`
	CurrentRows []*Record `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records" sql:"SELECT id FROM records WHERE 1=0" json:"-"`
}

type Output struct {
	Data   []*Record `parameter:"Data,kind=output,in=body" json:"data"`
	Status string    `json:"status"`
	Logger xl.Logger `bind:"kind=logger,required" json:"-"`
}
type Logger struct{ identity byte }

func (*Logger) Debug(string, ...any) {}
func (*Logger) Info(string, ...any)  {}
func (*Logger) Warn(string, ...any)  {}
func (*Logger) Error(string, ...any) {}

var sourceLogger = &Logger{}
var BusinessCause = errors.New("PRIVATE business diagnostic")

func (i *Input) Init(context.Context) error {
	if len(i.CurrentRows) != 2 || i.CurrentRows[0].ID != 7 || i.CurrentRows[1].ID != 8 {
		return fmt.Errorf("native Current binding failed: %+v", i.CurrentRows)
	}
	return &response.Error{Code: 400, Payload: &Output{Data: i.Rows, Status: "business", Logger: sourceLogger}, Cause: BusinessCause}
}

type Evidence struct {
	Captures, Executes, Bridges, Finalizes int
	Canonical, Payload, Finalized          *Output
	Err                                    error
	Trusted                                xl.Logger
}
type Handler struct {
	*writer.Handler
	mu            sync.Mutex
	evidence      Evidence
	remap         bool
	executeNative bool
	discardOutput bool
	DB            *sql.DB
}

func (h *Handler) CaptureInput(ctx context.Context, input any) (any, error) {
	h.mu.Lock()
	h.evidence.Captures++
	h.mu.Unlock()
	return h.Handler.CaptureInput(ctx, input)
}
func (h *Handler) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	h.mu.Lock()
	h.evidence.Executes++
	h.mu.Unlock()
	if h.executeNative {
		value, err := h.Handler.Execute(ctx, inv)
		if h.discardOutput {
			return nil, err
		}
		return value, err
	}
	panic("failed initialization must never execute")
}

func (h *Handler) CapturedErrorOutput(ctx context.Context, inv rh.Invocation, cause error) (any, error) {
	value, err := h.Handler.CapturedErrorOutput(ctx, inv, cause)
	var public *response.Error
	errors.As(cause, &public)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.evidence.Bridges++
	h.evidence.Canonical, _ = value.(*Output)
	if public != nil {
		h.evidence.Payload, _ = public.Payload.(*Output)
	}
	return value, err
}
func (h *Handler) FinalizeOutcome(ctx context.Context, inv rh.Invocation, result any, outcome xh.Outcome) error {
	if err := h.Handler.FinalizeOutcome(ctx, inv, result, outcome); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.evidence.Finalizes++
	h.evidence.Finalized, _ = result.(*Output)
	h.evidence.Err = outcome.Error
	if output, ok := result.(*Output); ok && output != nil {
		output.Status = "finalized"
		if h.remap {
			return &response.Error{Code: 409, Payload: output, Cause: outcome.Error}
		}
	}
	return nil
}
func (h *Handler) Observe() Evidence { h.mu.Lock(); defer h.mu.Unlock(); return h.evidence }
func New(t testing.TB, remap bool) (*registry.RegisteredComponent, *Handler, error) {
	return newFixture(t, remap, reflect.TypeFor[Input](), true)
}
func NewSQLFailure(t testing.TB, sourceNil bool) (*registry.RegisteredComponent, *Handler, error) {
	registered, handler, err := newFixture(t, false, reflect.TypeFor[SQLFailureInput](), false)
	if handler != nil {
		handler.discardOutput = sourceNil
	}
	return registered, handler, err
}
func newFixture(t testing.TB, remap bool, inputType reflect.Type, auxiliary bool) (*registry.RegisteredComponent, *Handler, error) {
	fixture := sqlite.New(t)
	if err := fixture.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(7),(8)"); err != nil {
		return nil, nil, err
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.test/capture", Name: "Failure"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/captured-error", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "captured.error"}}}}, RootView: &spec.View{Name: "Rows", Auxiliary: auxiliary, Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[Output]()})
	if err != nil {
		return nil, nil, err
	}
	native, err := writer.New(artifact.Component, inputType, reflect.TypeFor[Output](), "patch")
	if err != nil {
		return nil, nil, err
	}
	views, err := artifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: fixture.DB}})
	if err != nil {
		return nil, nil, err
	}
	trusted := &Logger{identity: 1}
	h := &Handler{Handler: native, remap: remap, DB: fixture.DB, executeNative: !auxiliary, evidence: Evidence{Trusted: trusted}}
	return &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[Output](), Handler: h, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: fixture.DB}, Capabilities: rh.InvocationCapabilities{Logger: trusted}}, h, nil
}

// AssertState verifies that the genuine native binding/initialization path leaves
// both requested and unrelated rows intact and retains the original table.
func (h *Handler) AssertState(t testing.TB) {
	t.Helper()
	rows, err := h.DB.Query("SELECT id FROM records ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []int{7, 8}) {
		t.Fatalf("initialization failure changed records: %v", ids)
	}
	var ddl string
	if err = h.DB.QueryRow("SELECT sql FROM sqlite_master WHERE type='table' AND name='records'").Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if ddl != "CREATE TABLE records(id INTEGER PRIMARY KEY)" {
		t.Fatalf("unexpected records DDL: %s", ddl)
	}
}
