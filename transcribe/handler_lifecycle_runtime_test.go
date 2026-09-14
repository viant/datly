package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestTranscribedHandlersRunUnifiedLifecycleAndTransactions(t *testing.T) {
	tests := []struct {
		name   string
		target HandlerTarget
	}{
		{name: "generated Go", target: HandlerGo},
		{name: "generated Velty", target: HandlerVelty},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyTranscribedHandlerLifecycle(t, testCase.target)
		})
	}
}

func TestTranscribedHandlersCompleteTransactionAfterSuccessFinalizer(t *testing.T) {
	tests := []struct {
		name   string
		target HandlerTarget
	}{
		{name: "generated Go", target: HandlerGo},
		{name: "generated Velty", target: HandlerVelty},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			verifyTranscribedSuccessFinalizer(t, testCase.target)
		})
	}
}

func verifyTranscribedHandlerLifecycle(t *testing.T, target HandlerTarget) {
	t.Helper()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID TEXT PRIMARY KEY,
		NAME TEXT NOT NULL CHECK (NAME <> '')
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dql := `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`
	generated, err := NewCompiler().Transcribe(ctx, Request{
		Source: &Source{
			Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
			Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		},
		Destination: root,
		Options: Options{Handler: HandlerOptions{
			Target: target, Operation: WritePost,
		}},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	if target == HandlerGo && (generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.VeltyHandler != nil) {
		t.Fatalf("generated Go products = contract:%+v velty:%+v", generated.Result.Plan.ContractHandler, generated.Result.Plan.VeltyHandler)
	}
	if target == HandlerVelty && (generated.Result.Plan.VeltyHandler == nil || generated.Result.Plan.ContractHandler != nil) {
		t.Fatalf("generated Velty products = contract:%+v velty:%+v", generated.Result.Plan.ContractHandler, generated.Result.Plan.VeltyHandler)
	}
	testSource := generatedLifecycleRuntimeSource(target)
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_lifecycle_test.go"), []byte(testSource), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated %s lifecycle module failed: %v\n%s", target, runErr, output)
	}
}

func verifyTranscribedSuccessFinalizer(t *testing.T, target HandlerTarget, transform ...func(string, string) string) {
	t.Helper()
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE EVENTS (
		ID TEXT PRIMARY KEY,
		NAME TEXT NOT NULL CHECK (NAME <> '')
	)`); err != nil {
		t.Fatalf("create EVENTS: %v", err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	dql := `#setting($_ = $route('/events', 'POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`
	_, err := NewCompiler().Transcribe(ctx, Request{
		Source: &Source{
			Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
			Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
		},
		Destination: root,
		Options: Options{Handler: HandlerOptions{
			Target: target, Operation: WritePost,
		}},
	})
	if err != nil {
		t.Fatalf("Transcribe() error = %v", err)
	}
	source := generatedSuccessFinalizerRuntimeSource(target)
	for _, rewrite := range transform {
		source = rewrite(root, source)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "generated_success_finalizer_test.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated %s success-finalizer module failed: %v\n%s", target, runErr, output)
	}
}

func generatedLifecycleRuntimeSource(target HandlerTarget) string {
	customImport := ""
	handlerFactory := `handler, err := NewEventsHandler()
	if err != nil {
		t.Fatal(err)
	}
	return handler`
	if target == HandlerGo {
		customImport = `customhandler "github.com/viant/datly/runtime/handler/custom"`
		handlerFactory = `return customhandler.New[EventsInput, EventsOutput](NewEventsHandler())`
	}
	source := `package events

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	{{CUSTOM_IMPORT}}
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/structology"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

var (
	initializerCalls int
	finalizerCalls   int
	finalizerErr     error
	lifecycleOrder   []string
	finalizerProbe   func(context.Context, error) error
)

func (i *EventsInput) Init(context.Context) error {
	initializerCalls++
	lifecycleOrder = append(lifecycleOrder, "init")
	return nil
}


func (o *EventsOutput) Finalize(ctx context.Context, terminal error) error {
	finalizerCalls++
	finalizerErr = terminal
	lifecycleOrder = append(lifecycleOrder, "finalize")
	if finalizerProbe != nil {
		return finalizerProbe(ctx, terminal)
	}
	return nil
}

func resetLifecycle() {
	initializerCalls = 0
	finalizerCalls = 0
	finalizerErr = nil
	lifecycleOrder = nil
	finalizerProbe = nil
}

type countingScope struct {
	providers []locator.Provider
	values    *int
}

func newCountingScope(scope interface{ Providers() []locator.Provider }, values *int) *countingScope {
	return &countingScope{providers: scope.Providers(), values: values}
}

func (s *countingScope) Providers() []locator.Provider {
	result := append([]locator.Provider(nil), s.providers...)
	for index, provider := range result {
		if provider.Kind() == "body" {
			result[index] = &countingProvider{Provider: provider, values: s.values}
		}
	}
	return result
}

type countingProvider struct {
	locator.Provider
	values *int
}

func (p *countingProvider) Locate(state *structology.State) locator.Locator {
	return &countingLocator{Locator: p.Provider.Locate(state), values: p.values}
}

func (p *countingProvider) DefaultCacheable() bool {
	return false
}

type countingLocator struct {
	locator.Locator
	values *int
}

func (l *countingLocator) Value(ctx context.Context, target reflect.Type, name string) (any, bool, error) {
	(*l.values)++
	return l.Locator.Value(ctx, target, name)
}

func openLifecycleDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		"CREATE TABLE EVENTS (ID TEXT PRIMARY KEY, NAME TEXT NOT NULL CHECK (NAME <> ''))",
		"CREATE TABLE AUDIT (ID INTEGER PRIMARY KEY)",
	} {
		if _, err = db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func generatedArtifact(t *testing.T) *bootstrap.Artifact {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: generatedComponent(),
		InputType: reflect.TypeOf(EventsInput{}), OutputType: reflect.TypeOf(EventsOutput{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func generatedComponent() *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/generated/events", Name: "Events"},
		Name: "Events", Routes: []*spec.Route{{Method: "POST", Path: "/events"}},
		Parameters: []*spec.Parameter{
			{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}, Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, EmitOutput: true},
		},
	}
}

func newGeneratedHandler(t *testing.T) rhandler.Handler {
	t.Helper()
	{{HANDLER_FACTORY}}
}

func executeGenerated(t *testing.T, ctx context.Context, source dexec.DataSource, body string) (*EventsOutput, error, int) {
	t.Helper()
	request := httptest.NewRequest("POST", "/events", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	requestScope, err := requestprovider.New(request)
	if err != nil {
		t.Fatal(err)
	}
	bodyValues := 0
	component := generatedComponent()
	artifact := generatedArtifact(t)
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{
		Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(EventsOutput{}),
		DataSource: source, Handler: newGeneratedHandler(t),
	}})
	if err != nil {
		t.Fatal(err)
	}
	actual, executeErr := runtime.ExecuteRoute(ctx, "POST", "/events", newCountingScope(requestScope, &bodyValues))
	if actual == nil {
		return nil, executeErr, bodyValues
	}
	output, ok := actual.(*EventsOutput)
	if !ok {
		t.Fatalf("generated output type = %T", actual)
	}
	return output, executeErr, bodyValues
}

func TestGeneratedLifecycleSuccessAndFlushFailure(t *testing.T) {
	ctx := context.Background()
	t.Run("success finalizes after owned commit", func(t *testing.T) {
		resetLifecycle()
		db := openLifecycleDB(t)
		source := sqldml.Source{DB: db, OnCommit: func(context.Context) {
			lifecycleOrder = append(lifecycleOrder, "commit")
		}}
		output, err, bodyValues := executeGenerated(t, ctx, source, ` + "`" + `{"Data":[{"id":"one","name":"first"}]}` + "`" + `)
		if err != nil || output == nil {
			t.Fatalf("execute = (%+v, %v)", output, err)
		}
		if initializerCalls != 1 || finalizerCalls != 1 || finalizerErr != nil || bodyValues != 1 {
			t.Fatalf("lifecycle init=%d finalize=%d terminal=%v bodyValues=%d", initializerCalls, finalizerCalls, finalizerErr, bodyValues)
		}
		if got := strings.Join(lifecycleOrder, ","); got != "init,finalize,commit" {
			t.Fatalf("lifecycle order = %q", got)
		}
	})

	t.Run("flush failure rolls back and reaches terminal finalizer", func(t *testing.T) {
		resetLifecycle()
		db := openLifecycleDB(t)
		source := sqldml.Source{DB: db, OnCommit: func(context.Context) {
			lifecycleOrder = append(lifecycleOrder, "commit")
		}}
		_, err, bodyValues := executeGenerated(t, ctx, source, ` + "`" + `{"Data":[{"id":"bad","name":""}]}` + "`" + `)
		if err == nil {
			t.Fatal("expected flush failure")
		}
		if initializerCalls != 1 || finalizerCalls != 1 || finalizerErr == nil || bodyValues != 1 {
			t.Fatalf("lifecycle init=%d finalize=%d terminal=%v bodyValues=%d", initializerCalls, finalizerCalls, finalizerErr, bodyValues)
		}
		if got := strings.Join(lifecycleOrder, ","); got != "init,finalize" {
			t.Fatalf("lifecycle order = %q", got)
		}
		var count int
		if queryErr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM EVENTS").Scan(&count); queryErr != nil || count != 0 {
			t.Fatalf("failed flush persisted rows: count=%d err=%v", count, queryErr)
		}
	})
}

var errGeneratedHandler = errors.New("generated handler queue failure")

type failingDataSource struct {
	scope *failingDataScope
}

func (s *failingDataSource) Open(context.Context) (xhandler.Data, error) {
	s.scope = &failingDataScope{}
	return s.scope, nil
}

type failingDataScope struct {
	inserts int
	flushes int
}

func (s *failingDataScope) Insert(string, any) error {
	s.inserts++
	if s.inserts == 2 {
		return errGeneratedHandler
	}
	return nil
}

func (*failingDataScope) Update(string, any) error          { return nil }
func (*failingDataScope) Delete(string, any) error          { return nil }
func (*failingDataScope) Execute(string, ...any) error      { return nil }
func (s *failingDataScope) Flush(context.Context, string) error { s.flushes++; return nil }
func (*failingDataScope) Allocate(context.Context, string, any, string) error { return nil }

func TestGeneratedHandlerFailureSkipsFlushAndPersistsNoBufferedWrites(t *testing.T) {
	resetLifecycle()
	ctx := context.Background()
	db := openLifecycleDB(t)
	source := &failingDataSource{}
	_, err, bodyValues := executeGenerated(t, ctx, source, ` + "`" + `{"Data":[{"id":"one","name":"first"},{"id":"two","name":"second"}]}` + "`" + `)
	if !errors.Is(err, errGeneratedHandler) {
		t.Fatalf("handler error = %v", err)
	}
	if source.scope == nil || source.scope.inserts != 2 || source.scope.flushes != 0 {
		t.Fatalf("data scope = %+v", source.scope)
	}
	if initializerCalls != 1 || finalizerCalls != 1 || !errors.Is(finalizerErr, errGeneratedHandler) || bodyValues != 1 {
		t.Fatalf("lifecycle init=%d finalize=%d terminal=%v bodyValues=%d", initializerCalls, finalizerCalls, finalizerErr, bodyValues)
	}
	var count int
	if queryErr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM AUDIT").Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("buffered writes must not execute after handler failure: count=%d err=%v", count, queryErr)
	}
}

type externalDataSource struct {
	db *sql.DB
	tx *sql.Tx
}

func (s externalDataSource) Open(context.Context) (xhandler.Data, error) {
	return sqldml.NewData(s.db, sqldml.WithTx(s.tx)), nil
}

func TestGeneratedHandlerLeavesExternalTransactionOpen(t *testing.T) {
	resetLifecycle()
	ctx := context.Background()
	db := openLifecycleDB(t)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	finalizerProbe = func(ctx context.Context, terminal error) error {
		if terminal != nil {
			return terminal
		}
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM EVENTS WHERE ID = 'one'").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("transaction flushed before finalizer completed")
		}
		return nil
	}
	_, err, bodyValues := executeGenerated(t, ctx, externalDataSource{db: db, tx: tx}, ` + "`" + `{"Data":[{"id":"one","name":"first"}]}` + "`" + `)
	if err != nil {
		t.Fatalf("execute with external transaction: %v", err)
	}
	if initializerCalls != 1 || finalizerCalls != 1 || finalizerErr != nil || bodyValues != 1 {
		t.Fatalf("lifecycle init=%d finalize=%d terminal=%v bodyValues=%d", initializerCalls, finalizerCalls, finalizerErr, bodyValues)
	}
	var pending int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM EVENTS WHERE ID = 'one'").Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("external transaction pending rows=%d err=%v", pending, err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO EVENTS(ID, NAME) VALUES ('two', 'second')"); err != nil {
		t.Fatalf("external transaction was closed by engine: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	lifecycleOrder = append(lifecycleOrder, "rollback")
	if got := strings.Join(lifecycleOrder, ","); got != "init,finalize,rollback" {
		t.Fatalf("external lifecycle order = %q", got)
	}
	var count int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM EVENTS").Scan(&count); err != nil || count != 0 {
		t.Fatalf("caller rollback did not own transaction: count=%d err=%v", count, err)
	}
}

var (
	_ xhandler.Initializer  = (*EventsInput)(nil)
	_ xhandler.ErrorFinalizer = (*EventsOutput)(nil)
)
`
	return strings.NewReplacer(
		"{{CUSTOM_IMPORT}}", customImport,
		"{{HANDLER_FACTORY}}", handlerFactory,
	).Replace(source)
}
