package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/internal/testharness/sqlite"
	rcontract "github.com/viant/datly/runtime/contract"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	rreader "github.com/viant/datly/runtime/handler/reader"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx"
	"github.com/viant/x"
	"github.com/viant/x/shape"
	"github.com/viant/xdatly"
	xcontract "github.com/viant/xdatly/contract"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/viant/xdatly/response"
)

type handoffAuthInput struct {
	Tenant int `parameter:",kind=header,in=X-Tenant,required=true"`
}
type handoffAuth struct{ Tenant int }
type handoffInput struct {
	Auth     *handoffAuth                                    `parameter:",kind=component,in=GET:/handoff-auth,required=true"`
	Trigger  string                                          `parameter:",kind=header,in=X-Bind,required=false"`
	Period   string                                          `parameter:",kind=query,in=period,required=false,value=day"`
	Optional *string                                         `parameter:",kind=query,in=optional,required=false"`
	Has      *struct{ Auth, Trigger, Period, Optional bool } `setMarker:"true" json:"-"`
}
type handoffMode struct {
	Mode string `parameter:",kind=header,in=X-Policy,required=true"`
}

func (*handoffInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state handoffMode
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if !strings.Contains(state.Mode, "nullable") && !strings.Contains(state.Mode, "combined") {
		return nil, nil
	}
	return &xcontract.Policy{Name: state.Mode, NoCache: strings.Contains(state.Mode, "nocache"), Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[handoffInput]() && field.Name == "Period" {
			return field.SetTag("format", "nullable=true"), nil
		}
		return nil, nil
	}}, nil
}

type handoffRow struct {
	ID    int     `sqlx:"id"`
	Label *string `sqlx:"label"`
}
type handoffOutput struct {
	Data []*handoffRow `parameter:",kind=output,in=view" view:"rows,type=handoffRow,limit=2" sql:"SELECT id,label FROM records WHERE tenant = $Auth.Tenant AND period = $Period"`
}

func (*handoffOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var mode handoffMode
	if err := binder.Bind(ctx, &mode); err != nil {
		return nil, err
	}
	if strings.Contains(mode.Mode, "fail") {
		return nil, handoffPolicyFailure
	}
	if strings.Contains(mode.Mode, "conditional") {
		var extra struct {
			Source string `parameter:",kind=header,in=X-Conditional,required=true"`
		}
		if err := binder.Bind(ctx, &extra); err != nil {
			return nil, err
		}
	}
	if !strings.Contains(mode.Mode, "authority") && !strings.Contains(mode.Mode, "combined") {
		return nil, nil
	}
	var state struct {
		Auth *handoffAuth `parameter:",kind=component,in=GET:/handoff-auth,required=true"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	return &xcontract.Policy{Name: fmt.Sprintf("tenant/%d", state.Auth.Tenant), Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[handoffRow]() && field.Name == "Label" && state.Auth.Tenant == 2 {
			return field.SetTag("mcp", "-"), nil
		}
		return nil, nil
	}}, nil
}

type handoffSelectors struct {
	Fields []string `parameter:",kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:",kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type handoffHolder struct {
	Contract xdatly.Component[handoffInput, handoffOutput] `component:"Handoff,path=/handoff,method=GET"`
	Flight   xdatly.Component[handoffInput, handoffOutput] `component:"Handoff,path=/handoff-flight,method=GET"`
}

func (handoffHolder) DatlyQuerySelectors() handoffSelectors { panic("metadata only") }

type handoffReader struct {
	mu sync.Mutex
	dexec.Reader
	preparer dexec.QueryPreparer
	calls    atomic.Int64
	last     *handoffInput
}

func (r *handoffReader) PrepareQuery(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*dexec.PreparedQuery, error) {
	r.calls.Add(1)
	r.mu.Lock()
	r.last = input.(*handoffInput)
	r.mu.Unlock()
	return r.preparer.PrepareQuery(ctx, input, binder, resolver)
}

func (r *handoffReader) lastInput() *handoffInput { r.mu.Lock(); defer r.mu.Unlock(); return r.last }

type handoffLoader struct{ entries []*RegisteredComponent }

func (l handoffLoader) LoadComponent(_ context.Context, key spec.Key) (*RegisteredComponent, error) {
	for _, entry := range l.entries {
		if entry.Component.Key == key {
			return entry, nil
		}
	}
	return nil, nil
}

type handoffFixture struct {
	preparations atomic.Int64
	rt           *Runtime
	reader       *handoffReader
	auth         atomic.Int64
	unavailable  atomic.Bool
	revoked      atomic.Bool
	target       dexec.ComponentTarget
}

var handoffUnavailable = errors.New("handoff fixture dependency unavailable")
var handoffPolicyFailure = errors.New("handoff fixture policy failure")

func newHandoffFixture(t testing.TB, lazy bool) *handoffFixture {
	t.Helper()
	f := new(handoffFixture)
	auth, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "HandoffAuth"}, Routes: []*spec.Route{{Method: "GET", Path: "/handoff-auth"}}}, InputType: reflect.TypeFor[handoffAuthInput](), OutputType: reflect.TypeFor[handoffAuth](), Handler: custom.NewFunc(func(_ context.Context, in *handoffAuthInput) (*handoffAuth, error) {
		f.auth.Add(1)
		if f.unavailable.Load() {
			return nil, &response.Error{Code: 503, Cause: handoffUnavailable}
		}
		if f.revoked.Load() {
			return nil, &response.Error{Code: 403, Cause: fmt.Errorf("fixture revoked")}
		}
		return &handoffAuth{Tenant: in.Tenant}, nil
	})})
	require.NoError(t, err)
	association, err := model.New(x.NewType(reflect.TypeFor[handoffHolder]()), "Contract", x.NewType(reflect.TypeFor[handoffSelectors]()))
	require.NoError(t, err)
	source := &spec.Component{Name: "Handoff", Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[handoffHolder]().PkgPath(), Name: "Handoff"}, Routes: []*spec.Route{{Method: "GET", Path: "/handoff"}, {Method: "GET", Path: "/handoff-flight"}}, SelectorCompanion: association}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: source, InputType: reflect.TypeFor[handoffInput](), OutputType: reflect.TypeFor[handoffOutput]()})
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,label TEXT,tenant INTEGER,period TEXT)", "INSERT INTO records VALUES(1,'one',1,'day'),(2,'two',1,'day'),(3,NULL,1,'day'),(4,'other',2,'day')"))
	execution, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[handoffInput](), OutputType: reflect.TypeFor[handoffOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	f.reader = &handoffReader{Reader: execution, preparer: execution}
	a, err := auth.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	b, err := artifact.Registration(registry.RegisteredComponent{Reader: f.reader})
	require.NoError(t, err)
	if lazy {
		f.rt, err = NewIndexedRuntime([]*spec.Component{a.Component, b.Component}, nil, handoffLoader{[]*RegisteredComponent{a, b}})
	} else {
		f.rt, err = NewRuntime([]*RegisteredComponent{a, b})
	}
	require.NoError(t, err)
	f.target = dexec.ComponentTarget{Component: b.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/handoff"}}
	// Instrument the existing variant preparation callback, without a second
	// compile path, to measure native cold/warm route retention end to end.
	entry, err := f.rt.registeredComponent(context.Background(), b.Component.Key)
	require.NoError(t, err)
	for _, route := range b.Component.Routes {
		ref := spec.RouteRef{Method: route.Method, Path: route.Path}
		canonical, ok := entry.Input.ForRoute(ref)
		require.True(t, ok)
		plans, err := f.rt.contractPlansForContext(context.Background(), entry, ref.String())
		require.NoError(t, err)
		native := prepareInputExecution(canonical)
		plans.input, err = rcontract.NewInput(entry.Input.Type(), 64, func(metadata *shape.Representation) (any, error) { f.preparations.Add(1); return native(metadata) })
		require.NoError(t, err)
	}
	t.Cleanup(func() { require.NoError(t, f.rt.Shutdown(context.Background())) })
	return f
}
func handoffScope(t testing.TB, mode string, tenant int, query string) *requestprovider.Scope {
	t.Helper()
	req := httptest.NewRequest("GET", "/handoff"+query, nil)
	req.Header.Set("X-Policy", mode)
	req.Header.Set("X-Tenant", fmt.Sprint(tenant))
	req.Header.Set("X-Bind", "bind")
	if strings.Contains(mode, "conditional") {
		req.Header.Set("X-Conditional", "only-first-frame")
	}
	scope, err := requestprovider.New(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = scope.Close() })
	return scope
}
func TestPreparationHandoffNativeFactorial(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		t.Run(fmt.Sprint("lazy=", lazy), func(t *testing.T) {
			f := newHandoffFixture(t, lazy)
			for _, path := range []string{"/handoff", "/handoff-flight"} {
				for _, mode := range []string{"baseline", "nullable", "authority", "combined", "combined-nocache"} {
					t.Run(path+"/"+mode, func(t *testing.T) {
						target := f.target
						target.Route.Path = path
						for run := 0; run < 3; run++ {
							scope := handoffScope(t, mode, 1, "?fields=id&page=2")
							before := f.auth.Load()
							compiled := f.preparations.Load()
							started := time.Now()
							result, err := f.rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers(), PrepareQuery: true})
							require.NoError(t, err)
							prepared := result.(*dexec.PreparedQuery)
							require.Contains(t, prepared.SQL, "tenant")
							require.NotContains(t, strings.ToUpper(prepared.SQL), " LIMIT ")
							require.NotEmpty(t, prepared.Args)
							require.EqualValues(t, 1, f.auth.Load()-before, "same operation must not rebind Auth")
							wantCompile := int64(0)
							if mode == "combined-nocache" || run == 0 && (mode == "nullable" || mode == "combined") {
								wantCompile = 1
							}
							require.Equal(t, wantCompile, f.preparations.Load()-compiled, "only native cold/NoCache variants compile")
							require.Equal(t, 1, f.reader.lastInput().Auth.Tenant)
							require.Equal(t, "day", f.reader.lastInput().Period)
							require.Nil(t, f.reader.lastInput().Optional)
							rows, err := prepared.Projection.ReadProjection(t.Context(), dexec.ProjectionRequest{SQL: prepared.SQL, Args: prepared.Args, RowType: reflect.TypeFor[handoffRow]()})
							require.NoError(t, err)
							require.Len(t, reflect.ValueOf(rows).Interface(), 3)
							t.Logf("run=%d elapsed=%s auth=1 sqlArgs=%d", run, time.Since(started), len(prepared.Args))
						}
					})
				}
			}
		})
	}
}

// Mutations occur in protocol providers at the native engine bind boundary,
// after Contract selection and resolved receipt creation.
func TestPreparationHandoffSourceFreshnessDuringBinding(t *testing.T) {
	for _, phase := range []string{"public", "selector", "binding-error", "cancel", "deadline", "dependency", "closed"} {
		t.Run(phase, func(t *testing.T) {
			f := newHandoffFixture(t, false)
			scope := handoffScope(t, "combined", 1, "?fields=id")
			mode := "combined"
			changed := false
			captures := 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			marker := errors.New("original binding failure")
			header := handlerprovider.Named("header", func(ctx context.Context, typ reflect.Type, name string) (any, bool, error) {
				switch name {
				case "X-Policy":
					captures++
					if phase == "dependency" && changed {
						return nil, false, &response.Error{Code: 503, Cause: handoffUnavailable}
					}
					return mode, true, nil
				case "X-Tenant":
					return "1", true, nil
				case "X-Bind":
					if phase != "selector" {
						changed = true
						mode = "nullable"
						if phase == "closed" {
							ctx.Value(contractBindingsKey{}).(*contractBindings).close()
						}
						if phase == "binding-error" {
							return nil, false, marker
						}
						if phase == "cancel" {
							cancel()
						}
						if phase == "deadline" {
							return nil, false, context.DeadlineExceeded
						}
					}
					return "bind", true, nil
				}
				return nil, false, nil
			})
			providers := scope.Providers()
			for i, p := range providers {
				if p.Kind() == "header" {
					providers[i] = header
				}
			}
			if phase == "selector" {
				query := handlerprovider.Named("query", func(_ context.Context, typ reflect.Type, name string) (any, bool, error) {
					if name == "fields" {
						changed = true
						mode = "nullable"
						return []string{"id"}, true, nil
					}
					return nil, false, nil
				})
				for i, p := range providers {
					if p.Kind() == "query" {
						providers[i] = query
					}
				}
			}
			_, err := f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.target, Providers: providers, PrepareQuery: true})
			require.Error(t, err)
			require.Zero(t, f.reader.calls.Load(), "stale binding must fail before source SQL preparation")
			switch phase {
			case "binding-error":
				require.ErrorIs(t, err, marker)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			case "deadline":
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case "dependency":
				require.ErrorIs(t, err, handoffUnavailable)
				require.Equal(t, 503, response.ErrorStatusCode(err, 500))
			default:
				var binding *bindly.BindingError
				require.ErrorAs(t, err, &binding)
				require.Equal(t, 409, binding.Code)
			}
			require.True(t, changed)
			t.Logf("phase=%s source captures=%d auth=%d cause=%v", phase, captures, f.auth.Load(), err)
		})
	}
}

func TestPreparationHandoffFreshRequestsRevocation(t *testing.T) {
	f := newHandoffFixture(t, false)
	for _, state := range []string{"allowed", "revoked", "restored", "unavailable"} {
		f.revoked.Store(state == "revoked")
		f.unavailable.Store(state == "unavailable")
		scope := handoffScope(t, "combined", 1, "")
		before := f.auth.Load()
		_, err := f.rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: f.target, Providers: scope.Providers(), PrepareQuery: true})
		require.EqualValues(t, 1, f.auth.Load()-before)
		if state == "revoked" {
			require.Equal(t, 403, response.ErrorStatusCode(err, 500))
		} else if state == "unavailable" {
			require.ErrorIs(t, err, handoffUnavailable)
			require.Equal(t, 503, response.ErrorStatusCode(err, 500))
		} else {
			require.NoError(t, err)
		}
	}
}

func TestPreparationHandoffSameOperationFrames(t *testing.T) {
	f := newHandoffFixture(t, false)
	first := handoffScope(t, "combined", 1, "")
	ctx := dexec.CaptureContracts(t.Context())
	selected, err := f.rt.ResolveContract(ctx, f.target, first)
	require.NoError(t, err)
	bindings := contractBindingsFor(selected)
	require.True(t, bindings.claim())
	defer bindings.close()
	ctx = context.WithValue(ctx, contractBindingsKey{}, bindings)
	parent := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Parent"}, Route: spec.RouteRef{Method: "POST", Path: "/parent"}}
	ctx = context.WithValue(ctx, componentStackKey{}, []dexec.ComponentTarget{parent})
	before := f.auth.Load()
	for i := 0; i < 12; i++ {
		target := f.target
		if i%2 == 1 {
			target.Route.Path = "/handoff-flight"
		}
		scope := handoffScope(t, "combined", 1, fmt.Sprintf("?fields=id&page=%d", 1+i%3))
		result, err := f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers(), PrepareQuery: true})
		require.NoError(t, err)
		require.NotNil(t, result.(*dexec.PreparedQuery))
		require.Equal(t, 1, f.reader.lastInput().Auth.Tenant)
	}
	require.Zero(t, f.auth.Load()-before, "one operation reuses its already-proven Auth across repeated frames")
	changed := handoffScope(t, "combined", 2, "")
	_, err = f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.target, Providers: changed.Providers(), PrepareQuery: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, f.auth.Load()-before, "changed source requires exactly one fresh Auth")
	require.Equal(t, 2, f.reader.lastInput().Auth.Tenant)
	latest, ok := dexec.SelectedContract(ctx, f.target)
	require.True(t, ok)
	require.NotNil(t, latest.OutputRepresentation)
	row := latest.OutputRepresentation.FieldShape(0).Elem().Elem()
	require.Equal(t, "-", row.Field(1).Tag.Get("mcp"), "changed dependency reselects output visibility before preparation")
}

func TestPreparationHandoffConcurrentCallers(t *testing.T) {
	f := newHandoffFixture(t, true)
	scopes := []*requestprovider.Scope{handoffScope(t, "combined", 1, "?fields=id"), handoffScope(t, "combined", 2, "?fields=id")}
	var group sync.WaitGroup
	failures := make(chan error, 2)
	for index, scope := range scopes {
		group.Add(1)
		go func(tenant int, scope *requestprovider.Scope) {
			defer group.Done()
			for i := 0; i < 8; i++ {
				result, err := f.rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: f.target, Providers: scope.Providers(), PrepareQuery: true})
				if err != nil {
					failures <- err
					return
				}
				prepared := result.(*dexec.PreparedQuery)
				rows, err := prepared.Projection.ReadProjection(t.Context(), dexec.ProjectionRequest{SQL: prepared.SQL, Args: prepared.Args, RowType: reflect.TypeFor[handoffRow]()})
				if err != nil {
					failures <- err
					return
				}
				count := reflect.ValueOf(rows).Len()
				want := 3
				if tenant == 2 {
					want = 1
				}
				if count != want {
					failures <- fmt.Errorf("tenant %d got %d rows", tenant, count)
					return
				}
			}
		}(index+1, scope)
	}
	group.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.EqualValues(t, 16, f.auth.Load(), "authority is fresh for each caller operation")
}

// Construct from the actual native selected variant and populated receipt, then
// exercise the engine request boundary independently of Runtime's mint guard.
func handoffEngineRequest(t *testing.T, f *handoffFixture) (context.Context, handlerengine.Request, *preparationHandoff, func(string, string)) {
	scope := handoffScope(t, "combined", 1, "?fields=id")
	headers := map[string]string{"X-Policy": "combined", "X-Tenant": "1", "X-Bind": "bind"}
	header := handlerprovider.Named("header", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		value, found := headers[name]
		return value, found, nil
	})
	providers := scope.Providers()
	for i, p := range providers {
		if p.Kind() == "header" {
			providers[i] = header
		}
	}
	effectiveScope, err := handlerengine.ComposeScope(nil, providers...)
	require.NoError(t, err)
	selected, err := f.rt.ResolveContract(t.Context(), f.target, effectiveScope)
	require.NoError(t, err)
	bindings := contractBindingsFor(selected)
	require.True(t, bindings.claim())
	t.Cleanup(bindings.close)
	entry, err := f.rt.registeredComponent(t.Context(), f.target.Component)
	require.NoError(t, err)
	canonical, ok := entry.Input.ForRoute(f.target.Route)
	require.True(t, ok)
	prepared := contractSelectionFor(selected).preparedInputPlan(canonical, selected.InputRepresentation)
	require.NotNil(t, prepared)
	route := prepared.route
	var specs []bindly.BindingSpec
	for _, field := range route.Fields() {
		specs = append(specs, field.Binding())
	}
	seed, paths, receipt, err := bindings.resolved(t.Context(), route.Plan(), specs, &contractSourceScope{protocol: effectiveScope}, true, contractSelectionFor(selected))
	require.NoError(t, err)
	require.Equal(t, []string{"Auth"}, paths)
	evidence, valid := bindings.frameEvidence(f.target, selected)
	require.True(t, valid)
	frame := &preparationFrame{target: f.target}
	handoff := &preparationHandoff{selected: contractSelectionFor(selected), frame: frame, bindings: bindings, owner: f.rt, scope: &contractSourceScope{protocol: effectiveScope}, canonical: canonical, route: route, prepared: prepared, representation: selected.InputRepresentation, receipt: receipt, selection: evidence}
	ctx := context.WithValue(t.Context(), contractBindingsKey{}, bindings)
	ctx = context.WithValue(ctx, componentStackKey{}, []dexec.ComponentTarget{f.target})
	ctx = context.WithValue(ctx, preparationFrameKey{}, frame)
	handler, err := rreader.NewQueryHandler(f.reader, route)
	require.NoError(t, err)
	proof := selectorcompanion.NewPreparationBinding(handler, f.reader, route, handoff)
	require.NotNil(t, proof)
	return ctx, handlerengine.Request{Injector: f.rt.injector, Input: route, OutputType: entry.OutputType, Handler: handler, PreparationBinding: proof, Scope: effectiveScope, ResolvedInput: seed, ResolvedPaths: paths}, handoff, func(name, value string) { headers[name] = value }
}
func TestPreparationHandoffEngineExactReceiptAndLifetime(t *testing.T) {
	for _, change := range []string{"unchanged", "seed-field", "seed-root", "path-order", "foreign-scope", "foreign-frame", "closed", "foreign-runtime", "detached-route", "copied-prepared", "saved-map-replaced", "source-before"} {
		t.Run(change, func(t *testing.T) {
			f := newHandoffFixture(t, false)
			ctx, request, handoff, changeHeader := handoffEngineRequest(t, f)
			switch change {
			case "source-before":
				changeHeader("X-Tenant", "2")
			case "seed-field":
				request.ResolvedInput.(*handoffInput).Auth = &handoffAuth{Tenant: 2}
			case "seed-root":
				request.ResolvedInput = &handoffInput{Auth: request.ResolvedInput.(*handoffInput).Auth}
			case "path-order":
				request.ResolvedPaths = []string{"Other", "Auth"}
			case "foreign-scope":
				request.Scope = handoffScope(t, "combined", 1, "?fields=id")
			case "foreign-frame":
				ctx = context.WithValue(ctx, preparationFrameKey{}, &preparationFrame{target: f.target})
			case "closed":
				handoff.bindings.close()
			case "foreign-runtime":
				handoff.bindings.owner = new(Runtime)
			case "copied-prepared":
				copied := *handoff.prepared
				handoff.prepared = &copied
			case "detached-route":
				copied := *handoff.prepared
				copied.original = new(registry.RouteInputContract)
				handoff.prepared = &copied
			case "saved-map-replaced":
				handoff.bindings.mu.Lock()
				for key, value := range handoff.bindings.values {
					value.value = &handoffAuth{Tenant: 2}
					handoff.bindings.values[key] = value
				}
				handoff.bindings.mu.Unlock()
			}
			_, err := f.rt.invoker.Execute(ctx, request)
			if change == "unchanged" || change == "saved-map-replaced" {
				require.NoError(t, err)
				require.EqualValues(t, 1, f.reader.calls.Load())
				require.Equal(t, 1, f.reader.lastInput().Auth.Tenant)
			} else {
				require.Error(t, err)
				require.Zero(t, f.reader.calls.Load())
			}
			if change == "seed-field" || change == "closed" || change == "foreign-runtime" || change == "source-before" {
				var binding *bindly.BindingError
				require.ErrorAs(t, err, &binding)
				require.Equal(t, 409, binding.Code)
			}
		})
	}
}

func TestPreparationHandoffFrameEvidenceResetAndLocality(t *testing.T) {
	f := newHandoffFixture(t, false)
	scope := handoffScope(t, "combined-conditional", 1, "")
	selected, err := f.rt.ResolveContract(t.Context(), f.target, scope)
	require.NoError(t, err)
	bindings := contractBindingsFor(selected)
	require.True(t, bindings.claim())
	defer bindings.close()
	ctx := context.WithValue(t.Context(), contractBindingsKey{}, bindings)
	// Resolve the same target without the earlier conditional binding. Its
	// protocol evidence must replace that resolution's set, including omission.
	next := handoffScope(t, "combined", 1, "")
	selected, err = f.rt.ResolveContract(ctx, f.target, next)
	require.NoError(t, err)
	valid, err := bindings.frameMatches(ctx, f.target, selected, &contractSourceScope{protocol: next})
	require.NoError(t, err)
	require.True(t, valid)
	// Unrelated frame evidence is deliberately incompatible with this scope.
	other := f.target
	other.Route.Path = "/unrelated"
	bindings.mu.Lock()
	bindings.frames[other] = &contractFrameEvidence{target: other, unprovable: true}
	bindings.mu.Unlock()
	valid, err = bindings.frameMatches(ctx, f.target, selected, &contractSourceScope{protocol: next})
	require.NoError(t, err)
	require.True(t, valid, "current frame never scans unrelated evidence")
	require.False(t, bindings.closed, "borrowed operations stay active between frames")
	failed := handoffScope(t, "combined-fail", 1, "")
	_, err = f.rt.ResolveContract(ctx, f.target, failed)
	require.ErrorIs(t, err, handoffPolicyFailure)
	valid, err = bindings.frameMatches(ctx, f.target, selected, &contractSourceScope{protocol: failed})
	require.NoError(t, err)
	require.False(t, valid, "a failed policy hook cannot complete selection evidence")
}

func TestPreparationHandoffDefaultPresenceAndNullRows(t *testing.T) {
	f := newHandoffFixture(t, false)
	for _, tc := range []struct {
		query, period string
		optional      *string
	}{
		{"", "day", nil}, {"?period=&optional=", "", new(string)}, {"?period=day&optional=value", "day", func() *string { s := "value"; return &s }()},
	} {
		for _, mode := range []string{"baseline", "combined"} {
			scope := handoffScope(t, mode, 1, tc.query)
			result, err := f.rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: f.target, Providers: scope.Providers(), PrepareQuery: true})
			require.NoError(t, err)
			input := f.reader.lastInput()
			require.Equal(t, tc.period, input.Period)
			require.Equal(t, tc.optional, input.Optional)
			require.Equal(t, strings.Contains(tc.query, "optional="), input.Has.Optional)
			prepared := result.(*dexec.PreparedQuery)
			rows, err := prepared.Projection.ReadProjection(t.Context(), dexec.ProjectionRequest{SQL: prepared.SQL, Args: prepared.Args, RowType: reflect.TypeFor[handoffRow]()})
			require.NoError(t, err)
			if tc.period == "day" {
				require.Len(t, rows, 3)
			} else {
				require.Len(t, rows, 0)
			}
		}
	}
}

func TestPreparationHandoffFrameLocalCaptureBudget(t *testing.T) {
	f := newHandoffFixture(t, false)
	scope := handoffScope(t, "combined", 1, "")
	reads := map[string]int{}
	header := handlerprovider.Named("header", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		reads[name]++
		switch name {
		case "X-Policy":
			return "combined", true, nil
		case "X-Tenant":
			return "1", true, nil
		case "X-Bind":
			return "bind", true, nil
		case "X-Unrelated":
			return "unrelated", true, nil
		}
		return nil, false, nil
	})
	providers := scope.Providers()
	for i, p := range providers {
		if p.Kind() == "header" {
			providers[i] = header
		}
	}
	protocol, err := handlerengine.ComposeScope(nil, providers...)
	require.NoError(t, err)
	selected, err := f.rt.ResolveContract(t.Context(), f.target, protocol)
	require.NoError(t, err)
	bindings := contractBindingsFor(selected)
	require.True(t, bindings.claim())
	defer bindings.close()
	entry, err := f.rt.registeredComponent(t.Context(), f.target.Component)
	require.NoError(t, err)
	canonical, _ := entry.Input.ForRoute(f.target.Route)
	var binding bindly.BindingSpec
	for _, field := range canonical.Fields() {
		if field.Binding().Path == "Trigger" {
			binding = field.Binding()
		}
	}
	binding.Location.In = "X-Unrelated"
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	plan, err := injector.CompilePlan(reflect.TypeFor[struct{ Trigger string }](), binding)
	require.NoError(t, err)
	replayPlan, err := plan.Replay("Trigger")
	require.NoError(t, err)
	sourceScope := &contractSourceScope{protocol: protocol}
	raw, err := sourceScope.input()
	require.NoError(t, err)
	sources, err := replayPlan.CaptureSourcesIn(t.Context(), raw)
	require.NoError(t, err)
	proof := &contractBindingProof{replay: replayPlan, count: 1, paths: map[string]string{"Trigger": "Trigger"}}
	for i := 0; i < 100; i++ {
		other := f.target
		other.Route.Path = fmt.Sprintf("/unrelated-%d", i)
		bindings.frames[other] = &contractFrameEvidence{target: other, complete: true, selections: map[*bindly.ReplayPlan]*bindly.Replay{replayPlan: sources}}
		bindings.values[contractBindingKey{route: other.Route.String(), typ: reflect.TypeFor[*handoffAuth]()}] = contractBindingValue{path: "Auth", value: &handoffAuth{Tenant: 1}, proof: proof, sources: sources}
	}
	reads = map[string]int{}
	ctx := context.WithValue(dexec.CaptureContracts(t.Context()), contractBindingsKey{}, bindings)
	ctx = context.WithValue(ctx, componentStackKey{}, []dexec.ComponentTarget{{Component: spec.Key{Kind: spec.KindComponent, Name: "Parent"}, Route: spec.RouteRef{Method: "POST", Path: "/parent"}}})
	compiled := f.preparations.Load()
	authority := f.auth.Load()
	for i := 0; i < 12; i++ {
		_, err = f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.target, Providers: providers, PrepareQuery: true})
		require.NoError(t, err)
	}
	require.Zero(t, reads["X-Unrelated"], "preparation must never capture unrelated operation frames or saved entries")
	require.Equal(t, compiled, f.preparations.Load(), "warm frames reuse the native prepared variant")
	require.Equal(t, authority, f.auth.Load(), "freshness reads do not invoke Auth")
	evidence, valid := bindings.frameEvidence(f.target, selected)
	require.True(t, valid)
	require.Len(t, evidence, 3, "only current input/output selection and dependency evidence are visited")
	t.Logf("frames=12 unrelatedFrames=100 unrelatedValues=100 selectionEntries=%d modeReads=%d tenantReads=%d unrelatedReads=%d authCalls=0 compileCalls=0", len(evidence), reads["X-Policy"], reads["X-Tenant"], reads["X-Unrelated"])
}

// An unpublished resolution for the same target must never certify a separately
// published Contract. The receipt survives native capture's value copy exactly.
func TestPreparationHandoffCapturedSelectionResolutionIdentity(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		for _, mode := range []string{"combined", "combined-nocache"} {
			for _, order := range [][2]int{{1, 2}, {2, 1}, {1, 1}, {2, 2}} {
				t.Run(fmt.Sprintf("lazy=%v/%s/%d-to-%d", lazy, mode, order[0], order[1]), func(t *testing.T) {
					f := newHandoffFixture(t, lazy)
					ctx := dexec.CaptureContracts(t.Context())
					first := handoffScope(t, mode, order[0], "")
					captured, err := f.rt.ResolveContract(ctx, f.target, first)
					require.NoError(t, err)
					bindings := contractBindingsFor(captured)
					require.NotNil(t, bindings)
					require.True(t, bindings.claim())
					defer bindings.close()
					ctx = context.WithValue(ctx, contractBindingsKey{}, bindings)
					ctx = context.WithValue(ctx, componentStackKey{}, []dexec.ComponentTarget{{Component: spec.Key{Kind: spec.KindComponent, Name: "Parent"}, Route: spec.RouteRef{Method: "POST", Path: "/parent"}}})
					dexec.PublishContract(ctx, f.target, captured)
					scope := handoffScope(t, mode, order[1], "?fields=id&page=2")
					newer, err := f.rt.ResolveContract(ctx, f.target, scope)
					require.NoError(t, err)
					require.NotSame(t, captured.BindingState, newer.BindingState, "resolutions have independent native identities")
					assertHandoffVisibility(t, newer, order[1])
					before := f.auth.Load()
					compiled := f.preparations.Load()
					for run := 0; run < 4; run++ {
						// A fresh unpublished resolution under the opposite provider must not
						// overwrite the currently captured selection's own proof or values.
						if run > 0 {
							other := handoffScope(t, mode, 3-order[1], "")
							_, err = f.rt.ResolveContract(ctx, f.target, other)
							require.NoError(t, err)
							before = f.auth.Load()
							compiled = f.preparations.Load()
						}
						result, err := f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.target, Providers: scope.Providers(), PrepareQuery: true})
						require.NoError(t, err)
						require.Equal(t, order[1], f.reader.lastInput().Auth.Tenant)
						latest, ok := dexec.SelectedContract(ctx, f.target)
						require.True(t, ok)
						assertHandoffVisibility(t, &latest, order[1])
						require.Equal(t, before, f.auth.Load(), "correct selection/receipt must not rebind Auth")
						wantCompile := int64(0)
						if run == 0 && order[0] != order[1] && mode == "combined-nocache" {
							wantCompile = 1
						}
						require.Equal(t, wantCompile, f.preparations.Load()-compiled, "native named retention and operation-local NoCache selection stay intact")
						prepared := result.(*dexec.PreparedQuery)
						rows, err := prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: prepared.SQL, Args: prepared.Args, RowType: reflect.TypeFor[handoffRow]()})
						require.NoError(t, err)
						want := 3
						if order[1] == 2 {
							want = 1
						}
						require.Len(t, rows, want)
					}
				})
			}
		}
	}
}
func assertHandoffVisibility(t *testing.T, selected *dexec.EffectiveContract, tenant int) {
	t.Helper()
	if tenant == 1 {
		require.Nil(t, selected.OutputRepresentation, "tenant 1 keeps its original visible fields")
		return
	}
	require.NotNil(t, selected.OutputRepresentation, "tenant 2 requires Label hidden")
	require.Equal(t, "-", selected.OutputRepresentation.FieldShape(0).Elem().Elem().Field(1).Tag.Get("mcp"))
}

func TestPreparationHandoffRootCapturedSelectionIdentity(t *testing.T) {
	for _, order := range [][2]int{{1, 2}, {2, 1}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			f := newHandoffFixture(t, false)
			ctx := dexec.CaptureContracts(t.Context())
			first, err := f.rt.ResolveContract(ctx, f.target, handoffScope(t, "combined", order[0], ""))
			require.NoError(t, err)
			bindings := contractBindingsFor(first)
			ctx = context.WithValue(ctx, contractBindingsKey{}, bindings)
			dexec.PublishContract(ctx, f.target, first)
			scope := handoffScope(t, "combined", order[1], "?fields=id")
			_, err = f.rt.ResolveContract(ctx, f.target, scope)
			require.NoError(t, err)
			before := f.auth.Load()
			_, err = f.rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: f.target, Providers: scope.Providers(), PrepareQuery: true})
			require.NoError(t, err)
			latest, ok := dexec.SelectedContract(ctx, f.target)
			require.True(t, ok)
			assertHandoffVisibility(t, &latest, order[1])
			require.Equal(t, order[1], f.reader.lastInput().Auth.Tenant)
			require.Equal(t, before, f.auth.Load())
			require.True(t, bindings.closed, "the root closes its claimed operation")
		})
	}
}

type handoffDerivedHandler struct{ target dexec.ComponentTarget }

func (*handoffDerivedHandler) InputType() reflect.Type  { return reflect.TypeFor[struct{}]() }
func (*handoffDerivedHandler) OutputType() reflect.Type { return reflect.TypeFor[handoffOutput]() }

func (h *handoffDerivedHandler) SourceContractTarget() dexec.ComponentTarget { return h.target }
func (h *handoffDerivedHandler) PrepareSourceContract(*spec.Component, reflect.Type, reflect.Type) (func(*dexec.EffectiveContract) (*dexec.ContractProjection, error), error) {
	return func(*dexec.EffectiveContract) (*dexec.ContractProjection, error) {
		return &dexec.ContractProjection{Policy: &xcontract.Policy{}, Prepare: func(*shape.Representation) (any, error) { return struct{}{}, nil }}, nil
	}, nil
}
func (*handoffDerivedHandler) Execute(context.Context, rhandler.Invocation) (any, error) {
	return new(handoffOutput), nil
}
func TestPreparationHandoffDerivedSourceSelectionIdentity(t *testing.T) {
	f := newHandoffFixture(t, false)
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "HandoffDerived"}, Routes: []*spec.Route{{Method: "POST", Path: "/handoff-derived"}}}
	handler := &handoffDerivedHandler{target: f.target}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[handoffOutput](), Handler: handler})
	require.NoError(t, err)
	derived, err := artifact.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	entries := []*RegisteredComponent{derived}
	for _, entry := range f.rt.registered {
		entries = append(entries, entry)
	}
	rt, err := NewRuntime(entries)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "POST", Path: "/handoff-derived"}}
	ctx := dexec.CaptureContracts(xmcp.WithContext(t.Context(), runtimeMCPContext{}))
	first, err := rt.ResolveContract(ctx, target, handoffScope(t, "combined", 1, ""))
	require.NoError(t, err)
	bindings := contractBindingsFor(first)
	ctx = context.WithValue(ctx, contractBindingsKey{}, bindings)
	require.Empty(t, contractSelectionFor(first).frame.selections, "the derived local frame has no direct source proof")
	require.NotNil(t, contractSelectionFor(first).source, "the native source receipt governs projected metadata")
	dexec.PublishContract(ctx, target, first)
	scope := handoffScope(t, "combined", 2, "")
	newer, err := rt.ResolveContract(ctx, target, scope)
	require.NoError(t, err)
	assertHandoffVisibility(t, newer, 2)
	before := f.auth.Load()
	_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
	require.NoError(t, err)
	latest, ok := dexec.SelectedContract(ctx, target)
	require.True(t, ok)
	assertHandoffVisibility(t, &latest, 2)
	assertHandoffVisibility(t, latest.SourceContract, 2)
	require.Equal(t, before, f.auth.Load(), "source identity correction reuses its valid native Auth")
}

func TestPreparationHandoffCloseReleasesCapturedResolutionValues(t *testing.T) {
	f := newHandoffFixture(t, false)
	first, err := f.rt.ResolveContract(t.Context(), f.target, handoffScope(t, "combined", 1, ""))
	require.NoError(t, err)
	bindings := contractBindingsFor(first)
	require.True(t, bindings.claim())
	ctx := context.WithValue(t.Context(), contractBindingsKey{}, bindings)
	second, err := f.rt.ResolveContract(ctx, f.target, handoffScope(t, "combined", 2, ""))
	require.NoError(t, err)
	firstReceipt, secondReceipt := contractSelectionFor(first), contractSelectionFor(second)
	require.NotEmpty(t, firstReceipt.frame.values)
	require.NotEmpty(t, secondReceipt.frame.values)
	bindings.close()
	for _, selected := range []*contractSelection{firstReceipt, secondReceipt} {
		require.Nil(t, selected.frame.values)
		require.Nil(t, selected.frame.selections)
		require.Nil(t, selected.frame.previous)
		require.False(t, selected.frame.complete)
	}
	require.Nil(t, bindings.frames)
}
