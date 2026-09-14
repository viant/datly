package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type captureRecordMarker struct{ ID bool }
type captureRecord struct {
	ID  int                  `json:"id"`
	Has *captureRecordMarker `setMarker:"true" json:"-"`
}
type captureInput struct {
	Record      *captureRecord `bind:"kind=body"`
	Initialized int
}

func (i *captureInput) Init(context.Context) error {
	i.Initialized++
	i.Record.ID = 100
	i.Record.Has = &captureRecordMarker{ID: true}
	return nil
}
func (i *captureInput) InitMCP(context.Context, xmcp.Context) error {
	i.Initialized += 10
	i.Record.ID = 101
	return nil
}

type captureResult struct {
	ID                 int
	Present, Available bool
}
type captureContract struct {
	fail       error
	queueAudit bool
}

func (h *captureContract) CaptureInput(_ context.Context, input *captureInput) (any, error) {
	if h.fail != nil {
		return nil, h.fail
	}
	if input.Initialized != 0 || input.Record == nil {
		return nil, fmt.Errorf("capture must follow binding and precede initialization")
	}
	result := captureResult{ID: input.Record.ID, Available: input.Record.Has != nil}
	if input.Record.Has != nil {
		result.Present = input.Record.Has.ID
	}
	return result, nil
}
func (h *captureContract) Exec(ctx context.Context, session xhandler.Session, input *captureInput, output *captureResult) error {
	if input.Initialized != 11 || input.Record.ID != 101 {
		return fmt.Errorf("initializers did not run after capture")
	}
	value, found, err := session.Binder().Lookup(ctx, xhandler.InputSnapshotKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("missing captured input")
	}
	result, ok := value.(captureResult)
	if !ok {
		return fmt.Errorf("unexpected snapshot %T", value)
	}
	*output = result
	if h.queueAudit {
		value, found, err := session.Binder().Lookup(ctx, xhandler.DMLKey)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("missing DML capability")
		}
		return value.(xhandler.DML).Execute("INSERT INTO capture_audit(original_id,present,available) VALUES(?,?,?)", result.ID, result.Present, result.Available)
	}
	return nil
}

func TestInputCaptureDrivesTransactionalHandlerSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx := xmcp.WithContext(context.Background(), engineMCPContext{})
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE capture_audit(original_id INTEGER,present BOOLEAN,available BOOLEAN)"); err != nil {
				t.Fatal(err)
			}
			contract := &captureContract{queueAudit: true}
			if fail {
				contract.fail = errors.New("capture failure")
			}
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(captureInput{})),
				Scope:   testharness.Request{}.WithBody([]byte(`{"id":0}`), "application/json"),
				Handler: custom.New[captureInput, captureResult](contract), DataSource: sqldml.Source{DB: h.DB},
			})
			if (err != nil) != fail {
				t.Fatalf("capture error=%v", err)
			}
			type row struct {
				OriginalID int  `sqlx:"original_id"`
				Present    bool `sqlx:"present"`
				Available  bool `sqlx:"available"`
			}
			want := []row{}
			if !fail {
				want = append(want, row{OriginalID: 0, Present: true, Available: true})
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT original_id,present,available FROM capture_audit"}, want)
		})
	}
}

func TestInputCapturePrecedesBothInitializers(t *testing.T) {
	for _, test := range []struct {
		name, body string
		bound      *captureInput
		want       captureResult
	}{
		{name: "explicit zero", body: `{"id":0}`, want: captureResult{ID: 0, Present: true, Available: true}},
		{name: "omitted identity", body: `{}`, want: captureResult{Available: true}},
		{name: "spoofed marker", body: `{"id":7,"Has":{"ID":false}}`, want: captureResult{ID: 7, Present: true, Available: true}},
		{name: "bound supplied identity", bound: &captureInput{Record: &captureRecord{ID: 0, Has: &captureRecordMarker{ID: true}}}, want: captureResult{Present: true, Available: true}},
		{name: "bound missing marker", bound: &captureInput{Record: &captureRecord{ID: 9}}, want: captureResult{ID: 9}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := Request{Input: testRouteInput(t, reflect.TypeOf(captureInput{})), Handler: custom.New[captureInput, captureResult](&captureContract{})}
			if test.bound != nil {
				request.BoundInput = test.bound
			} else {
				request.Scope = testharness.Request{}.WithBody([]byte(test.body), "application/json")
			}
			result, err := New().Execute(xmcp.WithContext(context.Background(), engineMCPContext{}), request)
			if err != nil {
				t.Fatal(err)
			}
			if actual := *result.(*captureResult); actual != test.want {
				t.Fatalf("capture=%+v want=%+v", actual, test.want)
			}
		})
	}
}

func TestInputCaptureErrorStopsInitialization(t *testing.T) {
	expected := errors.New("capture failed")
	input := &captureInput{Record: &captureRecord{ID: 7}}
	_, err := New().Execute(xmcp.WithContext(context.Background(), engineMCPContext{}), Request{
		Input: testRouteInput(t, reflect.TypeOf(captureInput{})), BoundInput: input,
		Handler: custom.New[captureInput, captureResult](&captureContract{fail: expected}),
	})
	if !errors.Is(err, expected) || input.Initialized != 0 || input.Record.ID != 7 {
		t.Fatalf("error=%v input=%+v", err, input)
	}
}

func TestInputCaptureIsInvocationScoped(t *testing.T) {
	inputContract := testRouteInput(t, reflect.TypeOf(captureInput{}))
	handler := custom.New[captureInput, captureResult](&captureContract{})
	var workers sync.WaitGroup
	for id := 0; id < 16; id++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			input := &captureInput{Record: &captureRecord{ID: id, Has: &captureRecordMarker{ID: true}}}
			result, err := New().Execute(xmcp.WithContext(context.Background(), engineMCPContext{}), Request{Input: inputContract, BoundInput: input, Handler: handler})
			if err != nil {
				t.Error(err)
				return
			}
			if result.(*captureResult).ID != id {
				t.Errorf("snapshot crossed invocations: %+v", result)
			}
		}(id)
	}
	workers.Wait()
}

func TestInputSnapshotProviderCannotBeOverridden(t *testing.T) {
	fake := []locator.Provider{provider.Static(xhandler.InputSnapshotKey, "forged")}
	for _, input := range []providerComposition{{component: fake}, {protocol: fake}, {child: fake}} {
		if _, err := (providerComposer{}).compose(input); err == nil {
			t.Fatal("untrusted snapshot provider accepted")
		}
	}
}

func TestInputSnapshotAbsentWithoutOptIn(t *testing.T) {
	_, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			_, found, err := invocation.Binder.Lookup(ctx, xhandler.InputSnapshotKey)
			if err != nil {
				return nil, err
			}
			if found {
				return nil, fmt.Errorf("unexpected input snapshot")
			}
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
}
