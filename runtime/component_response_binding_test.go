package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/runtime/handler/custom"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rh "github.com/viant/datly/runtime/handler"
	writerhandler "github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	xh "github.com/viant/xdatly/handler"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type independentBoundInput optionalReaderInput
type independentBoundOutput optionalReaderOutput

type independentSeedInput struct {
	Selection string              `parameter:"Selection,kind=query,in=selection"`
	Has       *independentSeedHas `setMarker:"true"`
}

type independentSeedHas struct{ Selection bool }

func (i *independentSeedInput) Init(context.Context) error {
	if i.Selection != "selected-record" {
		return fmt.Errorf("seed not preserved: %q", i.Selection)
	}
	return nil
}

type boundReaderOutput struct {
	Status legacy.Status      `parameter:"Status,kind=output,in=status" json:"status"`
	Data   []*bufferedCallRow `parameter:"Data,kind=output,in=view" json:"data"`
	Logger xh.Logger          `parameter:"Logger,kind=logger,required" json:"-"`
}

func (o *boundReaderOutput) Finalize(ctx context.Context, cause error) error {
	p := o.Logger.(*optionalReaderProbe)
	p.calls++
	if p.run != nil {
		return p.run(ctx, nil)
	}
	if cause != nil {
		return p.failure
	}
	if p.failure != nil {
		return p.failure
	}
	for _, r := range o.Data {
		r.Label += " finalized"
	}
	return nil
}

type boundReaderOutputOther boundReaderOutput
type boundReaderInvokerOutput boundReaderOutput

func (o *boundReaderInvokerOutput) Finalize(ctx context.Context, _ xh.ComponentInvoker, cause error) error {
	return (*boundReaderOutput)(o).Finalize(ctx, cause)
}

type undeclaredBoundReaderOutput struct {
	Status legacy.Status
	Data   []*bufferedCallRow `parameter:"Data,kind=output,in=view" json:"data"`
	Logger xh.Logger          `parameter:"Logger,kind=logger,required" json:"-"`
}

func (o *undeclaredBoundReaderOutput) Finalize(ctx context.Context, cause error) error {
	return (*boundReaderOutput)(o).Finalize(ctx, cause)
}

type boundSwallowInput struct {
	Invoker xh.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required"`
}

func (i *boundSwallowInput) Init(ctx context.Context) error {
	_, _ = i.Invoker.Invoke(context.Background(), xh.ComponentRequest{Target: xh.ComponentTarget{Scope: "example.com/components", Name: "BoundChild", Method: "GET", Path: "/unowned-grandchild"}})
	return nil
}

type boundAuthInput struct {
	Auth *bufferedCallOutput `parameter:"Auth,kind=component,in=GET:/bound-failed-grandchild,required"`
}
type boundFailureRow struct {
	ID    int    `sqlx:"id,primaryKey=true"`
	Label string `sqlx:"label"`
}

func (*boundFailureRow) OnFetch(context.Context) error { return errors.New("row callback failure") }

type boundRowFailureOutput struct {
	Status legacy.Status      `parameter:"Status,kind=output,in=status"`
	Data   []*boundFailureRow `parameter:"Data,kind=output,in=view"`
	Logger xh.Logger          `parameter:"Logger,kind=logger,required"`
}

func (o *boundRowFailureOutput) Finalize(_ context.Context, _ error) error {
	p := o.Logger.(*optionalReaderProbe)
	p.calls++
	return p.failure
}

type boundReaderInput struct {
	Invoker xh.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required"`
}

func TestComponentBindResponseNativeSQLite(t *testing.T) {
	cases := []string{"business", "success", "wrong destination", "route mismatch", "prepared input", "sql failure", "input failure projected403", "private cause", "private object", "view", "parameter", "5xx", "wrapped", "joined", "modern", "panic", "canceled", "prior writer", "later writer", "later writer failure", "custom", "unsupported output", "child failure", "pre-engine child failure", "deadline", "invoker business", "during cancellation", "published writer", "nested bind rejection", "swallowed grandchild", "ordinary prior writer", "prior writer failure", "published writer failure", "JWT", "auth dependency", "input initializer", "row callback", "deferred commit failure"}
	for _, mode := range []string{"business", "success", "sql failure", "private cause", "child failure", "canceled", "during cancellation"} {
		cases = append(cases, "independent/"+mode)
	}
	for _, mode := range cases {
		t.Run(mode, func(t *testing.T) {
			independent := strings.HasPrefix(mode, "independent/")
			mode = strings.TrimPrefix(mode, "independent/")
			ctx := t.Context()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			if mode == "deferred commit failure" {
				db.DB.SetMaxOpenConns(1)
				if err := db.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE deferred_checks(id INTEGER REFERENCES records(id) DEFERRABLE INITIALLY DEFERRED)", "CREATE TRIGGER deferred_invalid AFTER INSERT ON records WHEN NEW.id=2 BEGIN INSERT INTO deferred_checks VALUES(-999); END"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "prior writer" {
				if err := db.ExecStatements(ctx, "CREATE TABLE bound_write_order(seq INTEGER PRIMARY KEY AUTOINCREMENT,id INTEGER NOT NULL)", "CREATE TRIGGER bound_order AFTER INSERT ON records BEGIN INSERT INTO bound_write_order(id) VALUES(new.id); END"); err != nil {
					t.Fatal(err)
				}
			}
			candidate := &legacy.Error{Code: 403, Message: "requested record unavailable"}
			var failure error = candidate
			switch mode {
			case "success":
				failure = nil
			case "private cause":
				candidate.Err = errors.New("private SQL cause")
			case "private object":
				candidate.Object = map[string]any{"secret": true}
			case "view":
				candidate.View = "records"
			case "parameter":
				candidate.Parameter = "ID"
			case "5xx":
				candidate.Code = 500
			case "wrapped":
				failure = fmt.Errorf("wrapped: %w", candidate)
			case "joined":
				failure = errors.Join(candidate, errors.New("second failure"))
			case "modern":
				failure = &modern.Error{Code: 403, Cause: errors.New("modern")}
			}
			probe := &optionalReaderProbe{failure: failure}
			if mode == "panic" {
				probe.run = func(context.Context, xh.ComponentInvoker) error { panic("private panic") }
			}
			params := []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}, {Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}}
			c := componentSpec("BoundChild", "GET", "/bound-child", params)
			c.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records WHERE id=99"}}
			if mode == "sql failure" {
				c.RootView.Source.SQL = "SELECT id,label FROM missing_records"
			}
			inputType := reflect.TypeFor[struct{}]()
			if independent && (mode == "business" || mode == "success") {
				c.Routes = append(c.Routes, &spec.Route{Method: "GET", Path: "/bound-child/{id}"})
				inputType = reflect.TypeFor[independentSeedInput]()
			}
			if mode == "JWT" {
				inputType = reflect.TypeFor[injectorJWTInput]()
			}
			if mode == "auth dependency" {
				inputType = reflect.TypeFor[boundAuthInput]()
			}
			if mode == "input initializer" {
				inputType = reflect.TypeFor[optionalReaderInitFailureInput]()
			}
			if mode == "input failure projected403" {
				inputType = reflect.TypeFor[optionalReaderRequiredInput]()
			}
			if mode == "child failure" || mode == "pre-engine child failure" || mode == "nested bind rejection" || mode == "swallowed grandchild" {
				inputType = reflect.TypeFor[boundReaderInput]()
			}

			outputType := reflect.TypeFor[boundReaderOutput]()
			if mode == "invoker business" {
				outputType = reflect.TypeFor[boundReaderInvokerOutput]()
			}
			if mode == "unsupported output" {
				outputType = reflect.TypeFor[undeclaredBoundReaderOutput]()
				c.Parameters = c.Parameters[:1]
			}
			if mode == "row callback" {
				outputType = reflect.TypeFor[boundRowFailureOutput]()
			}
			build := bootstrap.ArtifactInput{Component: c, InputType: inputType, OutputType: outputType}
			if mode == "JWT" {
				key := testharness.NewJWT(t)
				factory, err := runtimeauth.New(ctx, &runtimeauth.Config{JWTValidator: key.Config()})
				if err != nil {
					t.Fatal(err)
				}
				build.CodecFactory = factory
			}
			a, err := bootstrap.BuildArtifact(build)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: inputType, OutputType: outputType, Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			child := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: outputType, Reader: reader, Capabilities: rh.InvocationCapabilities{Logger: probe}}
			if mode == "unsupported output" {
				child.Output = nil
			}
			if mode == "custom" {
				child.Reader = nil
				child.Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { return nil, candidate })
			}
			grandchildInput := reflect.TypeFor[optionalReaderRequiredInput]()
			if mode == "swallowed grandchild" {
				grandchildInput = reflect.TypeFor[boundSwallowInput]()
			}
			failedChild := readerFinalizerNative(t, db.DB, "BoundFailedGrandchild", "/bound-failed-grandchild", grandchildInput, reflect.TypeFor[bufferedCallOutput]())
			// A real native input failure may be ordinarily retired, but cannot
			// supply provenance to its parent's modeled business response.
			if mode == "child failure" || mode == "pre-engine child failure" || mode == "nested bind rejection" || mode == "swallowed grandchild" {
				probe.run = func(call context.Context, _ xh.ComponentInvoker) error {
					input := call.Value(reflect.TypeFor[*boundReaderInput]()).(*boundReaderInput)
					request := readerFinalizerPublicRequest(failedChild.Component, nil)
					if mode == "pre-engine child failure" {
						request.Target.Path = "/not-owned"
					}
					if mode == "nested bind rejection" {
						_, _ = input.Invoker.(xh.ComponentResponseBinder).BindResponse(context.Background(), request, &struct{}{})
					} else {
						_, nestedErr := input.Invoker.Invoke(context.Background(), request)
						if mode == "swallowed grandchild" && nestedErr != nil {
							t.Errorf("intermediate should succeed: %v", nestedErr)
						}
					}
					return candidate
				}
			}
			commits := 0
			writer := readerFinalizerWriter(t, db.DB, func(context.Context) { commits++ })
			if mode == "prior writer" || mode == "ordinary prior writer" {
				writer.Component.Settings.ProtectedFlushTables = []string{"records"}
			}
			fatalWriter := errors.New("managed writer failure")
			if mode == "prior writer failure" {
				writer.Handler = &bufferedInjectorFaultNative{Handler: writer.Handler.(*writerhandler.Handler), cause: fatalWriter}
			}
			var publicationSource *optionalBlockedSource
			if mode == "published writer" || mode == "published writer failure" {
				publicationSource = &optionalBlockedSource{db: db.DB, entered: make(chan struct{}), release: make(chan struct{})}
				if mode == "published writer failure" {
					publicationSource.failure = fatalWriter
				}
				writer.DataSource = publicationSource
			}
			writerRequest := readerFinalizerPublicRequest(writer.Component, &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 2, Label: "later write"}}})
			out := boundReaderOutput{Data: []*bufferedCallRow{{ID: -1, Label: "untouched"}}}
			var binding xh.BoundResponse
			var bindErr error
			rootInput := reflect.TypeFor[optionalReaderInput]()
			if mode == "published writer" || mode == "published writer failure" {
				rootInput = reflect.TypeFor[boundReaderInput]()
			}
			rootOutput := reflect.TypeFor[optionalReaderOutput]()
			if independent {
				rootInput = reflect.TypeFor[independentBoundInput]()
				rootOutput = reflect.TypeFor[independentBoundOutput]()
			}
			root := readerFinalizerNative(t, db.DB, "BoundRoot", "/bound-root", rootInput, rootOutput)
			root.Capabilities.Logger = &optionalReaderProbe{run: func(call context.Context, inv xh.ComponentInvoker) error {
				stagePrior := mode != "business" && mode != "success" && mode != "later writer" && mode != "later writer failure" && mode != "invoker business" && mode != "published writer" && mode != "published writer failure" && mode != "swallowed grandchild"
				if stagePrior && !independent {
					_, err := inv.Invoke(call, writerRequest)
					if mode != "prior writer failure" && err != nil {
						return err
					}
					if mode == "prior writer failure" && !errors.Is(err, fatalWriter) {
						return fmt.Errorf("prior writer lost failure: %v", err)
					}
				}
				request := readerFinalizerPublicRequest(child.Component, nil)
				if independent && (mode == "business" || mode == "success") {
					request.Target.Path = "/bound-child/{id}"
					request.ExtraInput = &xh.InputSeed{Input: &independentSeedInput{Selection: "selected-record", Has: &independentSeedHas{Selection: true}}}
				}
				var destination any = &out
				if mode == "row callback" {
					destination = &boundRowFailureOutput{}
				}
				var invokerOut boundReaderInvokerOutput
				if mode == "invoker business" {
					destination = &invokerOut
				}
				if mode == "unsupported output" {
					destination = &undeclaredBoundReaderOutput{}
				}
				if mode == "wrong destination" {
					destination = &boundReaderOutputOther{}
				}
				if mode == "route mismatch" {
					request.Target.Path = "/wrong"
				}
				if mode == "prepared input" {
					request.Input = &struct{}{}
				}
				if mode == "deadline" {
					var cancel context.CancelFunc
					call, cancel = context.WithDeadline(call, time.Now().Add(-time.Second))
					defer cancel()
				}
				if mode == "canceled" {
					var cancel context.CancelFunc
					call, cancel = context.WithCancel(call)
					cancel()
				}
				var releaseWriter chan struct{}
				var writerDone chan error
				if mode == "during cancellation" {
					var cancel context.CancelFunc
					call, cancel = context.WithCancel(call)
					defer cancel()
					probe.run = func(context.Context, xh.ComponentInvoker) error { cancel(); return candidate }
				}
				if mode == "published writer" || mode == "published writer failure" {
					releaseWriter = publicationSource.release
					writerDone = make(chan error, 1)
					source := publicationSource
					probe.run = func(context.Context, xh.ComponentInvoker) error {
						raw := call.Value(reflect.TypeFor[*boundReaderInput]()).(*boundReaderInput).Invoker
						go func() { _, err := raw.Invoke(call, writerRequest); writerDone <- err }()
						select {
						case <-source.entered:
						case <-time.After(5 * time.Second):
							return errors.New("writer publication timeout")
						}
						return candidate
					}
				}
				if mode == "ordinary prior writer" {
					_, bindErr = inv.Invoke(call, request)
				} else {
					binding, bindErr = inv.(xh.ComponentResponseBinder).BindResponse(call, request, destination)
				}
				if mode == "invoker business" {
					out = boundReaderOutput(invokerOut)
				}
				if releaseWriter != nil {
					if bindErr != nil || binding.Cause != candidate {
						t.Errorf("published writer prevented classification: %+v %v", binding, bindErr)
					}
					close(releaseWriter)
					writerErr := <-writerDone
					if mode == "published writer failure" && !errors.Is(writerErr, fatalWriter) {
						t.Errorf("concurrent writer failure lost: %v", writerErr)
					}
				}
				if mode == "later writer" || mode == "later writer failure" {
					if bindErr != nil {
						return bindErr
					}
					if mode == "later writer failure" {
						writerRequest.Input = &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 99, Label: "duplicate"}}}
					}
					_, err := inv.Invoke(call, writerRequest)
					return err
				}
				if mode == "prior writer" {
					var visible int
					if err := db.DB.QueryRowContext(call, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&visible); err != nil || visible != 0 || commits != 0 {
						return fmt.Errorf("eager flush: visible %d commits %d error %v", visible, commits, err)
					}
					later := writerRequest
					later.Input = &bufferedCallInput{Rows: []*bufferedCallRow{{ID: 3, Label: "status work after response"}}}
					if _, err := inv.Invoke(call, later); err != nil {
						return err
					}
				}
				// Returning errors preserves normal sticky completion and root outcome.
				return bindErr
			}}
			if independent {
				root.Component.Settings = &spec.Settings{ComponentCallPolicy: "imperative", IndependentChildTransactions: true}
				root.DataSource = nil
				root.Reader = nil
				run := root.Capabilities.Logger.(*optionalReaderProbe).run
				root.Handler = custom.New[independentBoundInput, independentBoundOutput](xh.ContractFunc[independentBoundInput, independentBoundOutput](func(ctx context.Context, session xh.Session, _ *independentBoundInput, _ *independentBoundOutput) error {
					deps := &struct {
						Invoker xh.ComponentInvoker `bind:"kind=component_invoker,required"`
					}{}
					if err := session.Binder().Bind(ctx, deps); err != nil {
						return err
					}
					return run(ctx, deps.Invoker)
				}))
			}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, child, writer, failedChild})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			_, rootErr := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)})
			converted := mode == "deferred commit failure" || mode == "prior writer" || mode == "published writer" || mode == "published writer failure" || mode == "invoker business" || mode == "business" || mode == "later writer" || mode == "later writer failure"
			if converted {
				if bindErr != nil || binding.StatusCode != 403 || binding.Cause != candidate {
					t.Fatalf("binding %+v err %v", binding, bindErr)
				}
				if len(out.Data) != 0 || out.Status.Message != candidate.Message || len(out.Status.Errors.([]string)) != 1 {
					t.Fatalf("declared response %+v", out)
				}
				if probe.calls != 1 {
					t.Fatalf("finalizer calls %d", probe.calls)
				}
				if mode == "later writer failure" || mode == "published writer failure" || mode == "deferred commit failure" {
					if rootErr == nil || commits != 0 {
						t.Fatalf("writer failure lost err %v commits %d", rootErr, commits)
					}
				} else if rootErr != nil {
					t.Fatal(rootErr)
				}
				if mode == "prior writer" {
					var order string
					if err := db.DB.QueryRowContext(ctx, "SELECT group_concat(id, ',') FROM bound_write_order ORDER BY seq").Scan(&order); err != nil || order != "2,3" {
						t.Fatalf("shared buffered order %q err %v", order, err)
					}
				}
				if (mode == "later writer" || mode == "prior writer") && commits != 1 {
					t.Fatalf("commits %d", commits)
				}
			} else if mode == "success" {
				if rootErr != nil || bindErr != nil || binding.Cause != nil || binding.StatusCode != 0 || len(out.Data) != 1 || out.Data[0].Label != "native read finalized" || probe.calls != 1 {
					t.Fatalf("success %+v %+v bind %v root %v calls %d", out, binding, bindErr, rootErr, probe.calls)
				}
			} else {
				if bindErr == nil || rootErr == nil || binding.Cause != nil || len(out.Data) != 1 || out.Data[0].ID != -1 {
					t.Fatalf("ineligible converted: %+v %+v bind %v root %v", out, binding, bindErr, rootErr)
				}
				if (mode == "wrong destination" || mode == "route mismatch" || mode == "canceled") && probe.calls != 0 {
					t.Fatalf("validation allowed effects: %d", probe.calls)
				}
				if commits != 0 {
					t.Fatalf("writer committed %d", commits)
				}
			}
			// Root errors must leave no physically durable partial writes,
			// including failures that arrive after response classification.
			if rootErr != nil {
				var persisted int
				if err := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id IN (2,3)").Scan(&persisted); err != nil || persisted != 0 {
					t.Fatalf("partial root write persisted: rows %d error %v", persisted, err)
				}
				if mode == "deferred commit failure" {
					if err := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM deferred_checks").Scan(&persisted); err != nil || persisted != 0 {
						t.Fatalf("partial deferred write persisted: rows %d error %v", persisted, err)
					}
				}
			}
		})
	}
}

func TestComponentBindResponseClosedLifecycleSQLite(t *testing.T) {
	ctx := t.Context()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "INSERT INTO records VALUES(99,'native read')"); err != nil {
		t.Fatal(err)
	}
	root := readerFinalizerNative(t, db.DB, "BoundClosedRoot", "/bound-closed-root", reflect.TypeFor[optionalReaderInput](), reflect.TypeFor[optionalReaderOutput]())
	var retained xh.ComponentResponseBinder
	probe := &optionalReaderProbe{run: func(_ context.Context, inv xh.ComponentInvoker) error {
		retained = inv.(xh.ComponentResponseBinder)
		return nil
	}}
	root.Capabilities.Logger = probe
	rt, err := NewRuntime([]*registry.RegisteredComponent{root})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(ctx)
	if _, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: readerFinalizerTarget(root.Component)}); err != nil {
		t.Fatal(err)
	}
	destination := optionalReaderOutput{Data: []*bufferedCallRow{{ID: -1}}}
	bound, err := retained.BindResponse(ctx, readerFinalizerPublicRequest(root.Component, nil), &destination)
	if err == nil || bound.Cause != nil || len(destination.Data) != 1 || destination.Data[0].ID != -1 || probe.calls != 1 {
		t.Fatalf("closed lease allowed binding: %+v %v calls %d", bound, err, probe.calls)
	}
}
