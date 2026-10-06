package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

// Observes the exact native Data returned by the existing Source, without
// wrapping Data, substituting an owner, constructing a permit or changing keys.
type streaming49Source struct {
	source dml.Source
	mu     sync.Mutex
	native *dml.Data
}

func (s *streaming49Source) Open(ctx context.Context) (xh.Data, error) {
	v, e := s.source.Open(ctx)
	if e == nil {
		s.mu.Lock()
		s.native = v.(*dml.Data)
		s.mu.Unlock()
	}
	return v, e
}
func (s *streaming49Source) InvocationKey() any { return s.source.InvocationKey() }
func (s *streaming49Source) InvocationTransactionKey() any {
	return s.source.InvocationTransactionKey()
}
func (s *streaming49Source) Native() *dml.Data { s.mu.Lock(); defer s.mu.Unlock(); return s.native }

type streaming49Connectors struct{ sources map[string]*streaming49Source }

func (c streaming49Connectors) Connector(_ context.Context, name string) (*sql.DB, error) {
	s := c.sources[name]
	if s == nil {
		return nil, fmt.Errorf("unknown test connector %s", name)
	}
	return s.source.DB, nil
}
func (c streaming49Connectors) ConnectorDataSource(_ context.Context, name string) (dexec.DataSource, error) {
	s := c.sources[name]
	if s == nil {
		return nil, fmt.Errorf("unknown test connector %s", name)
	}
	return s, nil
}

type streaming49Handler struct {
	*writer.Handler
	run             func(context.Context, rh.Invocation) error
	canonicalResult any
	canonicalError  error
}

func (h *streaming49Handler) Execute(ctx context.Context, in rh.Invocation) (any, error) {
	if e := h.run(ctx, in); e != nil {
		return nil, e
	}
	h.canonicalResult, h.canonicalError = h.Handler.Execute(ctx, in)
	return h.canonicalResult, h.canonicalError
}

type streaming49Allocated struct {
	ID    int    `sqlx:"id,primaryKey=true,autoincrement=true"`
	Label string `sqlx:"label"`
}

func streaming49IDs(t *testing.T, tx *sql.Tx) []int {
	t.Helper()
	rows, e := tx.QueryContext(t.Context(), "SELECT id FROM records ORDER BY id")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if e = rows.Scan(&id); e != nil {
			t.Fatal(e)
		}
		ids = append(ids, id)
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	return ids
}

type streaming49Row struct {
	ID     int    `sqlx:"id,primaryKey=true" json:"id"`
	Label  string `sqlx:"label" json:"label"`
	Remove bool   `sqlx:"-" writer:"delete" json:"remove"`
}
type streaming49Input struct {
	Rows        []*streaming49Row `parameter:"Rows,kind=body,in=data" view:"Rows,table=records" json:"data"`
	CurrentRows []*streaming49Row `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=records" sql:"SELECT id,label FROM records WHERE 1=0" json:"-"`
}
type streaming49Output struct {
	Data []*streaming49Row `parameter:"Data,kind=output,in=body" json:"data"`
}

func streaming49Native(t *testing.T, db *sqlite.Harness) *registry.RegisteredComponent {
	t.Helper()
	s := componentSpec("LiveStreaming", "PATCH", "/live-streaming", nil)
	s.Settings = &spec.Settings{Mutation: "patch", ComponentCallPolicy: "buffered"}
	s.RootView = &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
	a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: s, InputType: reflect.TypeFor[streaming49Input](), OutputType: reflect.TypeFor[streaming49Output]()})
	if e != nil {
		t.Fatal(e)
	}
	h, e := writer.New(a.Component, reflect.TypeFor[streaming49Input](), reflect.TypeFor[streaming49Output](), "patch")
	if e != nil {
		t.Fatal(e)
	}
	v, e := a.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if e != nil {
		t.Fatal(e)
	}
	return &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[streaming49Output](), Handler: h, Providers: []locator.Provider{v}, DataSource: dml.Source{DB: db.DB}}
}

// The genuine captured insert-delete policy registers its execution guard in
// Engine before this preserved-ABI Execute wrapper. No test guard is installed.
func TestNativeStreaming49DesiredProxyToNativeLiveFailure(t *testing.T) {
	for _, scope := range []string{"same", "other"} {
		for _, operation := range []string{"nativeSQL", "nativeAllocate", "proxySQL"} {
			for _, callerOther := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/callerOther=%t", scope, operation, callerOther), func(t *testing.T) {
					mainDB, otherDB := activity49DB(t), activity49DB(t)
					ctx := t.Context()
					for _, db := range []*sql.DB{mainDB.DB, otherDB.DB} {
						if _, e := db.ExecContext(ctx, "INSERT INTO records VALUES(99,'prior')"); e != nil {
							t.Fatal(e)
						}
					}
					mainSource := &streaming49Source{source: dml.Source{DB: mainDB.DB}}
					otherSource := &streaming49Source{source: dml.Source{DB: otherDB.DB}}
					var caller *sql.Tx
					if callerOther {
						var e error
						caller, e = otherDB.DB.BeginTx(ctx, nil)
						if e != nil {
							t.Fatal(e)
						}
						t.Cleanup(func() { _ = caller.Rollback() })
						otherSource.source.Tx = caller
					}
					native := streaming49Native(t, mainDB)
					native.DataSource = mainSource
					native.Capabilities = rh.InvocationCapabilities{Connector: streaming49Connectors{sources: map[string]*streaming49Source{"same": mainSource, "other": otherSource}}}
					var streamError, attempt error
					var before, after []int
					allocated := &streaming49Allocated{Label: "late allocation"}
					native.Handler = &streaming49Handler{Handler: native.Handler.(*writer.Handler), run: func(ctx context.Context, in rh.Invocation) error {
						value, ok, e := in.Binder.Lookup(ctx, rh.TransactionSQLCapabilityKey)
						if e != nil || !ok {
							return fmt.Errorf("actual managed SQL provider found=%t: %w", ok, e)
						}
						provider := value.(rh.TransactionSQLProvider)
						sameProxy, e := provider.Connector(ctx, "same")
						if e != nil {
							return e
						}
						otherProxy, e := provider.Connector(ctx, "other")
						if e != nil {
							return e
						}
						selected := mainSource.Native()
						proxy := sameProxy
						if scope == "other" {
							selected = otherSource.Native()
							proxy = otherProxy
						}
						if selected == nil {
							return errors.New("selected native owner was not genuinely opened")
						}
						if e = selected.Start(ctx); e != nil {
							return e
						}
						_, tx := selected.InvocationTransaction()
						if tx == nil {
							return errors.New("selected native transaction missing")
						}
						before = streaming49IDs(t, tx)
						rows, e := sameProxy.QueryContext(ctx, "SELECT id FROM records")
						if rows != nil {
							rows.Close()
						}
						streamError = e
						if e == nil || !strings.Contains(e.Error(), "managed streaming SQL") {
							return fmt.Errorf("intended proxy streaming rejection not reached: %v", e)
						}
						switch operation {
						case "nativeSQL":
							_, attempt = selected.ExecContext(ctx, "INSERT INTO records VALUES(2,'late native SQL')")
						case "nativeAllocate":
							attempt = selected.Allocate(ctx, "records", allocated, "ID")
						case "proxySQL":
							_, attempt = proxy.ExecContext(ctx, "INSERT INTO records VALUES(2,'late proxy SQL')")
						}
						after = streaming49IDs(t, tx)
						if attempt == nil {
							t.Errorf("DESIRED_PROXY_NATIVE_CHANNEL_GAP scope=%s operation=%s admitted after streamfailure", scope, operation)
						}
						if !reflect.DeepEqual(before, after) || allocated.ID != 0 {
							t.Errorf("DESIRED_PROXY_NATIVE_EFFECT_GAP rows=%v -> %v allocatedID=%d", before, after, allocated.ID)
						}
						// Failure is deliberately caught; delegate the genuine captured program,
						// which must retain the original guard error before business persistence.
						return nil
					}}
					r := activity49Runtime(t, native)
					request := activity49Request(native, 1)
					request.Input = &streaming49Input{Rows: []*streaming49Row{{ID: 1, Label: "native-1"}}}
					result, rootError := r.InvokeComponent(ctx, request)
					if streamError == nil {
						t.Fatalf("proxy streaming stage not reached: root=%v", rootError)
					}
					if rootError == nil || !errors.Is(rootError, streamError) {
						t.Errorf("DESIRED_STREAMING_ROOT_GAP error=%v result=%T", rootError, result)
					}
					wrapped := native.Handler.(*streaming49Handler)
					if result != wrapped.canonicalResult {
						t.Errorf("canonical native error output identity changed root=%p delegated=%p", result, wrapped.canonicalResult)
					}
					if attempt != nil && !errors.Is(attempt, streamError) {
						t.Errorf("native/proxy rejection lost original streaming cause: %v", attempt)
					}
					for _, s := range []*streaming49Source{mainSource, otherSource} {
						if s.Native() == nil {
							t.Fatal("genuine DB unit missing")
						}
						out := s.Native().TransactionOutcome()
						if s == otherSource && callerOther {
							if out.State != xh.TransactionCallerPending {
								t.Fatalf("caller outcome=%+v", out)
							}
						} else if out.State != xh.TransactionRolledBack && out.State != xh.TransactionNone {
							t.Fatalf("owned failed native outcome=%+v", out)
						}
					}
					if caller != nil {
						if _, e := caller.ExecContext(ctx, "INSERT INTO records VALUES(101,'caller usable')"); e != nil {
							t.Fatalf("caller unusable: %v", e)
						}
						if e := caller.Rollback(); e != nil {
							t.Fatal(e)
						}
					}
					for _, db := range []*sql.DB{mainDB.DB, otherDB.DB} {
						rows, e := activity49Rows(ctx, db)
						if e != nil || !reflect.DeepEqual(rows, []int{99}) {
							t.Fatalf("actual full rollback guard rows=%v error=%v", rows, e)
						}
						var n int
						if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlx_sequence_reservations").Scan(&n); e != nil || n != 0 {
							t.Fatalf("allocator physical rollback guard n=%d error=%v", n, e)
						}
					}
					t.Logf("REAL_PROXY_TO_NATIVE_FAILURE scope=%s operation=%s callerOther=%t stream=%v attempt=%v before=%v after=%v allocatedID=%d root=%v", scope, operation, callerOther, streamError, attempt, before, after, allocated.ID, rootError)
				})
			}
		}
	}
}

// Ordinary policy-free native calls retain existing streaming/direct SQL and
// transient allocator behavior; no failed/protected ledger is constructed.
func TestNativeStreaming49OrdinaryNoPolicyControl(t *testing.T) {
	db := activity49DB(t)
	ctx := t.Context()
	if _, e := db.DB.ExecContext(ctx, "INSERT INTO records VALUES(99,'prior')"); e != nil {
		t.Fatal(e)
	}
	source := &streaming49Source{source: dml.Source{DB: db.DB}}
	native := activity49Native(t, "OrdinaryStreaming", "/ordinary-streaming", "", db)
	native.DataSource = source
	native.Capabilities = rh.InvocationCapabilities{Connector: streaming49Connectors{sources: map[string]*streaming49Source{"same": source}}}
	allocated := &streaming49Allocated{Label: "genuine ordinary allocation"}
	native.Handler = &streaming49Handler{Handler: native.Handler.(*writer.Handler), run: func(ctx context.Context, in rh.Invocation) error {
		value, ok, e := in.Binder.Lookup(ctx, rh.TransactionSQLCapabilityKey)
		if e != nil || !ok {
			return fmt.Errorf("actual provider found=%t: %w", ok, e)
		}
		proxy, e := value.(rh.TransactionSQLProvider).Connector(ctx, "same")
		if e != nil {
			return e
		}
		rows, e := proxy.QueryContext(ctx, "SELECT id FROM records")
		if e != nil {
			return e
		}
		rows.Close()
		actual := source.Native()
		if actual == nil {
			return errors.New("no actual opened native unit")
		}
		if _, e = actual.ExecContext(ctx, "INSERT INTO records VALUES(2,'ordinary immediate')"); e != nil {
			return e
		}
		if e = actual.Allocate(ctx, "records", allocated, "ID"); e != nil {
			return e
		}
		if allocated.ID == 0 {
			return errors.New("existing ordinary allocator returned zero ID")
		}
		return nil
	}}
	r := activity49Runtime(t, native)
	result, e := r.InvokeComponent(ctx, activity49Request(native, 1))
	if e != nil || result == nil {
		t.Fatalf("ordinary behavior changed result=%T error=%v", result, e)
	}
	rows, e := activity49Rows(ctx, db.DB)
	if e != nil || !reflect.DeepEqual(rows, []int{99, 2, 1}) {
		t.Fatalf("ordinary physical rows=%v error=%v", rows, e)
	}
	if source.Native().TransactionOutcome().State != xh.TransactionCommitted {
		t.Fatalf("ordinary ownership changed %+v", source.Native().TransactionOutcome())
	}
	t.Logf("GENUINE_ORDINARY_CONTROL actualRows=%v actualAllocatedID=%d", rows, allocated.ID)
}
