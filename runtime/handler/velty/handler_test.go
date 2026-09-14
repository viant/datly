package velty

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	xhandler "github.com/viant/xdatly/handler"
	xmbus "github.com/viant/xdatly/mbus"
)

type capabilityLogger struct {
	info int
}

var veltyTestRoute = spec.RouteRef{Method: "TEST", Path: "/velty"}

func buildVeltyArtifact(t *testing.T, input bootstrap.ArtifactInput) *bootstrap.Artifact {
	t.Helper()
	component := input.Component.Clone()
	if component == nil {
		component = &spec.Component{}
	}
	component.Routes = []*spec.Route{{Method: veltyTestRoute.Method, Path: veltyTestRoute.Path}}
	input.Component = component
	artifact, err := bootstrap.BuildArtifact(input)
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	return artifact
}

func veltyRouteInput(t *testing.T, artifact *bootstrap.Artifact) *registry.RouteInputContract {
	t.Helper()
	if artifact == nil || artifact.Input == nil {
		t.Fatal("artifact route input contract is required")
	}
	result, ok := artifact.Input.ForRoute(veltyTestRoute)
	if !ok {
		t.Fatal("Velty test route input contract was not found")
	}
	return result
}

func (*capabilityLogger) Debug(string, ...any)  {}
func (l *capabilityLogger) Info(string, ...any) { l.info++ }
func (*capabilityLogger) Warn(string, ...any)   {}
func (*capabilityLogger) Error(string, ...any)  {}

type capabilityValidator struct {
	calls int
}

func (v *capabilityValidator) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	v.calls++
	return nil, nil
}

type capabilitySequencer struct {
	calls int
	ctx   context.Context
}

func (s *capabilitySequencer) Allocate(ctx context.Context, _ string, _ any, _ string) error {
	s.calls++
	s.ctx = ctx
	return nil
}

type capabilityBus struct {
	calls int
	ctx   context.Context
}

type trackingCapabilityBinder struct {
	values  map[xhandler.ValueKey]any
	lookups []xhandler.ValueKey
}

func (*trackingCapabilityBinder) Bind(context.Context, any) error {
	return nil
}

func (b *trackingCapabilityBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	b.lookups = append(b.lookups, key)
	value, ok := b.values[key]
	return value, ok, nil
}

type capabilityDML struct{}

func (capabilityDML) Insert(string, any) error     { return nil }
func (capabilityDML) Update(string, any) error     { return nil }
func (capabilityDML) Delete(string, any) error     { return nil }
func (capabilityDML) Execute(string, ...any) error { return nil }

func (b *capabilityBus) Push(ctx context.Context, message *xmbus.Message) (*xmbus.Confirmation, error) {
	b.calls++
	b.ctx = ctx
	return &xmbus.Confirmation{MessageID: message.Resource}, nil
}

func (*capabilityBus) Message(dest string, data interface{}, options ...xmbus.Option) *xmbus.Message {
	message := &xmbus.Message{Resource: dest, Data: data}
	xmbus.Options(options).Apply(message)
	return message
}

func TestHandlerExecutesTypedTemplateThroughSharedEngine(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		Name string
	}
	type output struct{}

	handler, err := New[input, output](Config{Template: `
#if($Input.Name == $Name)
$dml.Execute("INSERT INTO events(name) VALUES (?)", $Name)
#end`})
	if err != nil {
		t.Fatalf("create handler failed: %v", err)
	}
	param := &spec.Parameter{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}}
	artifact := buildVeltyArtifact(t, bootstrap.ArtifactInput{
		Component: &spec.Component{Parameters: []*spec.Parameter{param}}, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}),
	})
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input:      veltyRouteInput(t, artifact),
		Scope:      testharness.Request{}.WithQuery(url.Values{"name": []string{"Ada"}}),
		DataSource: sqldml.Source{DB: h.DB},
		Handler:    handler,
	})
	if err != nil {
		t.Fatalf("execute handler failed: %v", err)
	}
	if _, ok := actual.(*output); !ok {
		t.Fatalf("unexpected output type %T", actual)
	}
	var name string
	if err := h.DB.QueryRowContext(ctx, `SELECT name FROM events`).Scan(&name); err != nil {
		t.Fatalf("read inserted event failed: %v", err)
	}
	if name != "Ada" {
		t.Fatalf("expected inserted name Ada, got %q", name)
	}
}

func TestHandlerSaturatesExplicitCapabilityContext(t *testing.T) {
	type input struct {
		Name string
	}
	type output struct{}
	handler, err := New[input, output](Config{Template: `
$sequencer.Allocate("events", $Input, "Name")
$validator.Check($Input)
$messageBus.Publish("events", $Name)
$logger.Info("processed")`})
	if err != nil {
		t.Fatalf("create handler failed: %v", err)
	}
	log := &capabilityLogger{}
	validator := &capabilityValidator{}
	sequencer := &capabilitySequencer{}
	bus := &capabilityBus{}
	in := &input{Name: "Ada"}
	root, err := bindly.NewInjector()
	if err != nil {
		t.Fatalf("NewInjector() error = %v", err)
	}
	providers := handlerprovider.Capabilities(rhandler.InvocationCapabilities{
		Logger:     log,
		Validator:  validator,
		MessageBus: bus,
	})
	providers = append(providers, handlerprovider.Static(xhandler.SequencerKey, sequencer))
	scope, err := root.ForScope(providers...)
	if err != nil {
		t.Fatalf("ForScope() error = %v", err)
	}
	binder := rhandler.NewBinder(scope, in)
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "invocation")
	if _, err := handler.Execute(ctx, rhandler.Invocation{Input: in, Binder: binder}); err != nil {
		t.Fatalf("execute handler failed: %v", err)
	}
	if sequencer.calls != 1 || sequencer.ctx != ctx {
		t.Fatalf("unexpected sequencer invocation: calls=%d ctxMatch=%t", sequencer.calls, sequencer.ctx == ctx)
	}
	if validator.calls != 1 {
		t.Fatalf("expected validator call, got %d", validator.calls)
	}
	if bus.calls != 1 || bus.ctx != ctx {
		t.Fatalf("unexpected message bus invocation: calls=%d ctxMatch=%t", bus.calls, bus.ctx == ctx)
	}
	if log.info != 1 {
		t.Fatalf("expected logger call, got %d", log.info)
	}
}

func TestHandlerResolvesOnlyReferencedCapabilities(t *testing.T) {
	type input struct {
		Name string
	}
	type output struct {
		Name string
	}
	tests := []struct {
		name     string
		template string
		values   map[xhandler.ValueKey]any
		want     []xhandler.ValueKey
	}{
		{
			name: "no capabilities", template: `#set($Output.Name = $Input.Name)`,
		},
		{
			name: "DML only", template: `$dml.Execute("INSERT INTO audit(name) VALUES (?)", $Input.Name)`,
			values: map[xhandler.ValueKey]any{xhandler.DMLKey: capabilityDML{}}, want: []xhandler.ValueKey{xhandler.DMLKey},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			handler, err := New[input, output](Config{Template: testCase.template})
			if err != nil {
				t.Fatal(err)
			}
			binder := &trackingCapabilityBinder{values: testCase.values}
			actual, err := handler.Execute(context.Background(), rhandler.Invocation{
				Input: &input{Name: "Ada"}, Binder: binder,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := actual.(*output); !ok {
				t.Fatalf("output type = %T", actual)
			}
			if !reflect.DeepEqual(binder.lookups, testCase.want) {
				t.Fatalf("capability lookups = %v, want %v", binder.lookups, testCase.want)
			}
		})
	}
}

func TestHandlerBindsBodySliceAndBatchesTypedInserts(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type event struct {
		ID   int    `json:"id" sqlx:"id,primaryKey"`
		Name string `json:"name" sqlx:"name"`
	}
	type input struct {
		Events []*event
	}
	type output struct{}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}}}}
	artifact := buildVeltyArtifact(t, bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
	handler, err := New[input, output](Config{Template: `
#foreach($Event in $Events)
$dml.Insert("events", $Event)
#end`})
	if err != nil {
		t.Fatalf("create handler failed: %v", err)
	}
	if _, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input:      veltyRouteInput(t, artifact),
		Scope:      testharness.Request{}.WithBody([]byte(`{"Data":[{"id":1,"name":"one"},{"id":2,"name":"two"}]}`), "application/json"),
		DataSource: sqldml.Source{DB: h.DB},
		Handler:    handler,
	}); err != nil {
		t.Fatalf("execute handler failed: %v", err)
	}
	rows, err := h.DB.QueryContext(ctx, `SELECT id, name FROM events ORDER BY id`)
	if err != nil {
		t.Fatalf("query events failed: %v", err)
	}
	defer rows.Close()
	var actual []event
	for rows.Next() {
		var item event
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			t.Fatalf("scan event failed: %v", err)
		}
		actual = append(actual, item)
	}
	if len(actual) != 2 || actual[0].Name != "one" || actual[1].Name != "two" {
		t.Fatalf("unexpected inserted events: %+v", actual)
	}
}

func TestHandlerAllocatesSequencesAndInsertsComprehensiveMany(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "velty-sequencer.db"))
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type event struct {
		ID   int64  `json:"id" sqlx:"ID,primaryKey=true"`
		Name string `json:"name" sqlx:"NAME"`
	}
	type input struct {
		Events []*event
	}
	type output struct {
		Data []*event
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}}}}
	artifact := buildVeltyArtifact(t, bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
	handler, err := New[input, output](Config{Template: `
$sequencer.Allocate("EVENTS", $Events, "ID")
#foreach($Event in $Events)
$dml.Insert("EVENTS", $Event)
#end
#set($Output.Data = $Events)`})
	if err != nil {
		t.Fatalf("create handler failed: %v", err)
	}
	actual, err := handlerengine.New().Execute(ctx, handlerengine.Request{
		Input:      veltyRouteInput(t, artifact),
		Scope:      testharness.Request{}.WithBody([]byte(`{"Data":[{"name":"one"},{"name":"two"}]}`), "application/json"),
		DataSource: sqldml.Source{DB: db},
		Handler:    handler,
	})
	if err != nil {
		t.Fatalf("execute handler failed: %v", err)
	}
	result := actual.(*output)
	if len(result.Data) != 2 || result.Data[0].ID != 1 || result.Data[1].ID != 2 {
		t.Fatalf("unexpected allocated output: %+v", result.Data)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM EVENTS WHERE ID IN (1, 2)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("expected sequenced rows to flush, count=%d err=%v", count, err)
	}
}

func TestHandlerErrorDoesNotFlushQueuedDML(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		Name string
	}
	type output struct{}
	handler, err := New[input, output](Config{Template: `
$dml.Execute("INSERT INTO events(name) VALUES (?)", $Name)
$validator.Check($Input)`})
	if err != nil {
		t.Fatalf("create handler failed: %v", err)
	}
	param := &spec.Parameter{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}}
	artifact := buildVeltyArtifact(t, bootstrap.ArtifactInput{
		Component: &spec.Component{Parameters: []*spec.Parameter{param}}, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}),
	})
	_, err = handlerengine.New().Execute(ctx, handlerengine.Request{
		Input:      veltyRouteInput(t, artifact),
		Scope:      testharness.Request{}.WithQuery(url.Values{"name": []string{"Ada"}}),
		DataSource: sqldml.Source{DB: h.DB},
		Handler:    handler,
	})
	if err == nil {
		t.Fatal("expected missing validator error")
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("queued DML must not flush after handler error, count=%d err=%v", count, err)
	}
}

func TestHandlerReusesCompiledProgram(t *testing.T) {
	type input struct{ Name string }
	type output struct{}
	config := Config{Template: `$Input.Name`}
	first, err := New[input, output](config)
	if err != nil {
		t.Fatalf("first handler build failed: %v", err)
	}
	second, err := New[input, output](config)
	if err != nil {
		t.Fatalf("second handler build failed: %v", err)
	}
	if first.program != second.program {
		t.Fatal("expected registration-time compiled program cache reuse")
	}
}

func TestHandlerRejectsInvalidContracts(t *testing.T) {
	type input struct{}
	type output struct{}
	if _, err := New[*input, output](Config{Template: `$Input`}); err == nil {
		t.Fatal("expected pointer input contract rejection")
	}
	if _, err := New[input, *output](Config{Template: `$Output`}); err == nil {
		t.Fatal("expected pointer output contract rejection")
	}
}
