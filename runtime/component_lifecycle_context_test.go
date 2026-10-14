package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	xh "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
)

type componentMarkerKey struct{}
type componentMarkerLogger struct{}

func (*componentMarkerLogger) Debug(string, ...any) {}
func (*componentMarkerLogger) Info(string, ...any)  {}
func (*componentMarkerLogger) Warn(string, ...any)  {}
func (*componentMarkerLogger) Error(string, ...any) {}

type componentMarkerInput struct {
	Logger xh.Logger `parameter:"Logger,kind=logger,required" json:"-"`
	Seen   bool      `json:"-"`
}

func (i *componentMarkerInput) Init(ctx context.Context) error {
	i.Seen, _ = ctx.Value(componentMarkerKey{}).(bool)
	if _, ok := i.Logger.(*componentMarkerLogger); !ok || xlogger.FromContext(ctx) != i.Logger {
		return errors.New("call context replaced canonical input capability")
	}
	return nil
}

type componentMarkerRow struct {
	ID    int    `sqlx:"id,primaryKey=true" json:"id"`
	Label string `sqlx:"label" json:"label"`
	Seen  bool   `sqlx:"-" json:"-"`
}

func (r *componentMarkerRow) OnFetch(ctx context.Context) error {
	r.Seen, _ = ctx.Value(componentMarkerKey{}).(bool)
	if _, ok := xlogger.FromContext(ctx).(*componentMarkerLogger); !ok {
		return errors.New("call context replaced canonical row capability")
	}
	return nil
}

type componentMarkerOutput struct {
	Data      []*componentMarkerRow `parameter:"Data,kind=output,in=view" json:"data"`
	Seen      bool                  `json:"-"`
	Finalized bool                  `json:"-"`
}

func (o *componentMarkerOutput) Finalize(ctx context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	input := ctx.Value(reflect.TypeFor[*componentMarkerInput]()).(*componentMarkerInput)
	o.Seen, _ = ctx.Value(componentMarkerKey{}).(bool)
	if len(o.Data) != 1 || input.Seen != o.Seen || o.Data[0].Seen != o.Seen {
		return errors.New("input, row and output markers differ")
	}
	o.Finalized = true
	return nil
}
func TestLifecyclePerCallValuesReachNativeReaderHooks(t *testing.T) {
	for _, phase := range []string{"init", "finalize", "error finalize"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.WithValue(t.Context(), componentMarkerKey{}, false)
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')"); err != nil {
				t.Fatal(err)
			}
			child := readerFinalizerNative(t, db.DB, "ContextValueChild", "/context-value-child", reflect.TypeFor[componentMarkerInput](), reflect.TypeFor[componentMarkerOutput]())
			child.Capabilities.Logger = &componentMarkerLogger{}
			outputType := reflect.TypeFor[optionalReaderOutput]()
			if phase == "error finalize" {
				outputType = reflect.TypeFor[optionalReaderErrorOutput]()
			}
			root := readerFinalizerNative(t, db.DB, "ContextValueRoot", "/context-value-root", reflect.TypeFor[optionalReaderInput](), outputType)
			probe := &optionalReaderProbe{initialize: phase == "init", run: func(hookCtx context.Context, invoker xh.ComponentInvoker) error {
				// A lexical false->true override must survive. The child resolves its own
				// configured capability even when the call replaces the context logger.
				first := xlogger.WithContext(context.WithValue(hookCtx, componentMarkerKey{}, true), &optionalReaderProbe{})
				result, err := invoker.Invoke(first, readerFinalizerPublicRequest(child.Component, nil))
				if err != nil {
					return err
				}
				out := result.(*componentMarkerOutput)
				if !out.Seen || !out.Finalized {
					return errors.New("per-call marker missing in native hooks")
				}
				result, err = invoker.Invoke(context.Background(), readerFinalizerPublicRequest(child.Component, nil))
				if err != nil {
					return err
				}
				out = result.(*componentMarkerOutput)
				if out.Seen || !out.Finalized {
					return errors.New("marker leaked to a subsequent call")
				}
				if hookCtx.Value(componentMarkerKey{}) != false {
					return errors.New("call mutated callback context")
				}
				return nil
			}}
			root.Capabilities.Logger = probe
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, child})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			if _, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)}); err != nil {
				t.Fatal(err)
			}
			if probe.calls != 1 {
				t.Fatalf("callback calls %d", probe.calls)
			}
		})
	}
}
