package engine

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"

	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
)

type guardProbe struct{ mutations, reads int }

func (p *guardProbe) Insert(string, any) error     { p.mutations++; return nil }
func (p *guardProbe) Update(string, any) error     { p.mutations++; return nil }
func (p *guardProbe) Delete(string, any) error     { p.mutations++; return nil }
func (p *guardProbe) Execute(string, ...any) error { p.mutations++; return nil }
func (p *guardProbe) UpdateWithOptions(string, any, ...xhandler.Option) error {
	p.mutations++
	return nil
}
func (p *guardProbe) DeleteWithOptions(string, any, ...xhandler.Option) error {
	p.mutations++
	return nil
}
func (p *guardProbe) UpdateWithCriteria(string, any, *sqlx.Criteria, ...xhandler.Option) error {
	p.mutations++
	return nil
}
func (p *guardProbe) DeleteWithCriteria(string, any, *sqlx.Criteria, ...xhandler.Option) error {
	p.mutations++
	return nil
}
func (p *guardProbe) Allocate(context.Context, string, any, string) error { p.mutations++; return nil }
func (p *guardProbe) Reserve(context.Context, string, any, string) error  { p.mutations++; return nil }
func (p *guardProbe) Flush(context.Context, string) error                 { p.mutations++; return nil }
func (p *guardProbe) Start(context.Context) error                         { p.mutations++; return nil }
func (p *guardProbe) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	p.mutations++
	return nil, nil
}
func (p *guardProbe) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	p.reads++
	return nil, nil
}
func (p *guardProbe) QueryRowContext(context.Context, string, ...any) *sql.Row { p.reads++; return nil }
func (p *guardProbe) Dialect(context.Context) (*info.Dialect, error)           { p.reads++; return nil, nil }

type guardSource struct {
	data  *guardProbe
	key   string
	opens int
}

func (s *guardSource) Open(context.Context) (xhandler.Data, error) { s.opens++; return s.data, nil }
func (s *guardSource) InvocationKey() any                          { return s.key }

type guardConnectors struct{ sources map[string]*guardSource }

func (g guardConnectors) Connector(context.Context, string) (*sql.DB, error) {
	panic("raw connector resolution")
}
func (g guardConnectors) ConnectorDataSource(_ context.Context, name string) (dexec.DataSource, error) {
	return g.sources[name], nil
}

func TestWriteEligibilityManagedCapabilitiesNeverDelegate(t *testing.T) {
	calls := []struct {
		name string
		key  xhandler.ValueKey
		run  func(any) error
	}{
		{"insert", xhandler.DMLKey, func(v any) error { return v.(xhandler.DML).Insert("records", nil) }},
		{"update", xhandler.DMLKey, func(v any) error { return v.(xhandler.DML).Update("records", nil) }},
		{"delete", xhandler.DMLKey, func(v any) error { return v.(xhandler.DML).Delete("records", nil) }},
		{"execute", xhandler.DMLKey, func(v any) error { return v.(xhandler.DML).Execute("unused") }},
		{"matched update", xhandler.DMLKey, func(v any) error { return v.(xhandler.MatchedDML).UpdateWithOptions("records", nil) }},
		{"matched delete", xhandler.DMLKey, func(v any) error { return v.(xhandler.MatchedDML).DeleteWithOptions("records", nil) }},
		{"criteria update", xhandler.DMLKey, func(v any) error { return v.(rhandler.CriteriaDML).UpdateWithCriteria("records", nil, nil) }},
		{"criteria delete", xhandler.DMLKey, func(v any) error { return v.(rhandler.CriteriaDML).DeleteWithCriteria("records", nil, nil) }},
		{"allocate", xhandler.SequencerKey, func(v any) error { return v.(xhandler.Sequencer).Allocate(context.Background(), "records", nil, "ID") }},
		{"reserve", xhandler.SequencerKey, func(v any) error {
			return v.(interface {
				Reserve(context.Context, string, any, string) error
			}).Reserve(context.Background(), "records", nil, "ID")
		}},
		{"flush", xhandler.FlusherKey, func(v any) error { return v.(xhandler.Flusher).Flush(context.Background(), "") }},
		{"start", xhandler.TransactionStarterKey, func(v any) error { return v.(xhandler.TransactionStarter).Start(context.Background()) }},
	}
	for _, aggregate := range []bool{false, true} {
		for _, call := range calls {
			if aggregate && call.key == xhandler.TransactionStarterKey {
				continue
			}
			t.Run(call.name+map[bool]string{false: "/focused", true: "/aggregate"}[aggregate], func(t *testing.T) {
				probe := &guardProbe{}
				source := &guardSource{data: probe, key: "main"}
				_, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{}, DataSource: source,
					Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
						key := call.key
						if aggregate {
							key = xhandler.DataKey
						}
						cached, found, err := inv.Binder.Lookup(ctx, key)
						if err != nil || !found {
							t.Fatalf("capability: %v %v", found, err)
						}
						finish, err := BeginWriteEligibility(ctx)
						if err != nil {
							return nil, err
						}
						defer finish()
						// Simulate a hook which discards the returned mutation error.
						if err := call.run(cached); !errors.Is(err, ErrWriteEligibilityMutation) {
							t.Fatalf("mutation error %v", err)
						}
						if probe.mutations != 0 {
							t.Fatal("managed delegate ran")
						}
						if err := finish(); !errors.Is(err, ErrWriteEligibilityMutation) {
							t.Fatalf("swallowed error not latched: %v", err)
						}
						if err := finish(); !errors.Is(err, ErrWriteEligibilityMutation) {
							t.Fatalf("idempotent finish: %v", err)
						}
						if err := call.run(cached); err != nil {
							t.Fatalf("later mutation: %v", err)
						}
						if probe.mutations != 1 {
							t.Fatal("unwind did not restore delegation")
						}
						return nil, finish()
					})})
				if !errors.Is(err, ErrWriteEligibilityMutation) {
					t.Fatalf("invocation error %v", err)
				}
			})
		}
	}
}

func TestWriteEligibilityRootGuardCoversConnectorUnitsAndDescendants(t *testing.T) {
	main := &guardSource{data: &guardProbe{}, key: "main"}
	other := &guardSource{data: &guardProbe{}, key: "other"}
	_, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{}, DataSource: main,
		Capabilities: rhandler.InvocationCapabilities{Connector: guardConnectors{sources: map[string]*guardSource{"main": main, "other": other}}},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
			value, _, err := inv.Binder.Lookup(ctx, rhandler.TransactionSQLCapabilityKey)
			if err != nil {
				return nil, err
			}
			provider := value.(rhandler.TransactionSQLProvider)
			cached, err := provider.Connector(ctx, "main")
			if err != nil {
				return nil, err
			}
			finish, err := BeginWriteEligibility(ctx)
			if err != nil {
				return nil, err
			}
			defer finish()
			for _, name := range []string{"main", "other"} {
				tx := cached
				if name == "other" {
					tx, err = provider.Connector(context.Background(), name)
					if err != nil {
						return nil, err
					}
				}
				if _, err := tx.ExecContext(context.Background(), "unused"); !errors.Is(err, ErrWriteEligibilityMutation) {
					t.Fatalf("%s SQL error %v", name, err)
				}
				_, _ = tx.QueryContext(ctx, "unused")
				_ = tx.QueryRowContext(ctx, "unused")
			}

			for _, descendantSource := range []*guardSource{main, other} {
				_, childErr := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{}, DataSource: descendantSource,
					Handler: rhandler.HandlerFunc(func(childCtx context.Context, child rhandler.Invocation) (any, error) {
						dml, found, err := child.Binder.Lookup(childCtx, xhandler.DMLKey)
						if err != nil || !found {
							t.Fatalf("descendant capability: %v %v", found, err)
						}
						return nil, dml.(xhandler.DML).Insert("records", nil)
					})})
				if !errors.Is(childErr, ErrWriteEligibilityMutation) {
					t.Fatalf("descendant mutation: %v", childErr)
				}
			}
			child, owned := invocationDataScope(ctx, other)
			if owned || child.mutationGuard() != mainScope(ctx).mutationGuard() {
				t.Fatal("child guard is not root guard")
			}
			if err := child.mutationGuard().check("child"); !errors.Is(err, ErrWriteEligibilityMutation) {
				t.Fatal(err)
			}
			if main.data.mutations != 0 || other.data.mutations != 0 {
				t.Fatal("SQL delegate ran")
			}
			if main.data.reads != 2 || other.data.reads != 2 {
				t.Fatal("SQL inspection calls blocked")
			}
			return nil, finish()
		})})
	if !errors.Is(err, ErrWriteEligibilityMutation) {
		t.Fatal(err)
	}
}
func mainScope(ctx context.Context) *dataScope { return ctx.Value(dataScopeContextKey{}).(*dataScope) }

func TestWriteEligibilityNestedCancellationPanicAndIsolation(t *testing.T) {
	if finish, err := BeginWriteEligibility(context.Background()); err == nil || finish != nil {
		t.Fatal("missing scope accepted")
	}
	scope := neutralDataScope()
	ctx := withDataScope(context.Background(), scope)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if finish, err := BeginWriteEligibility(canceled); !errors.Is(err, context.Canceled) || finish != nil {
		t.Fatalf("cancellation %v", err)
	}
	outer, _ := BeginWriteEligibility(ctx)
	inner, _ := BeginWriteEligibility(ctx)
	if err := inner(); err != nil {
		t.Fatal(err)
	}
	if err := scope.mutationGuard().check("still nested"); !errors.Is(err, ErrWriteEligibilityMutation) {
		t.Fatal(err)
	}
	if err := outer(); !errors.Is(err, ErrWriteEligibilityMutation) {
		t.Fatal(err)
	}
	if err := scope.mutationGuard().check("after"); err != nil {
		t.Fatal(err)
	}
	clean := withDataScope(context.Background(), neutralDataScope())
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic lost")
			}
		}()
		finish, _ := BeginWriteEligibility(clean)
		defer finish()
		panic("hook")
	}()
	if err := mainScope(clean).mutationGuard().check("after panic"); err != nil {
		t.Fatal(err)
	}
	active, cancel := context.WithCancel(clean)
	finish, _ := BeginWriteEligibility(active)
	cancel()
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := neutralDataScope()
			ctx := withDataScope(context.Background(), local)
			finish, _ := BeginWriteEligibility(ctx)
			defer finish()
			if err := local.mutationGuard().check("concurrent"); !errors.Is(err, ErrWriteEligibilityMutation) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := mainScope(clean).mutationGuard().check("independent"); err != nil {
		t.Fatal("invocations shared guard", err)
	}
	fresh, _ := BeginWriteEligibility(clean)
	if err := fresh(); err != nil {
		t.Fatal("attempt inherited violation", err)
	}
}

func TestWriteEligibilityRetainedAuthorityHasNoDefaultEffect(t *testing.T) {
	scope := neutralDataScope()
	ctx := withDataScope(context.Background(), scope)
	retain := RetainMutationAuthority(ctx)
	replaced := context.Background()
	if retain(replaced) != replaced {
		t.Fatal("inactive guard changed context")
	}
	finish, err := BeginWriteEligibility(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if mainScope(retain(replaced)) != scope {
		t.Fatal("active guard authority was lost")
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	if retain(replaced) != replaced {
		t.Fatal("unwound guard changed context")
	}
}

func TestWriteEligibilityConcurrentNativeInvocationsAreIndependent(t *testing.T) {
	input := testRouteInput(t, reflect.TypeOf(struct{}{}))
	var wg sync.WaitGroup
	for index := 0; index < 12; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			source := &guardSource{data: &guardProbe{}, key: "local"}
			_, err := New().Execute(context.Background(), Request{Input: input, BoundInput: &struct{}{}, DataSource: source,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					value, found, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil || !found {
						t.Errorf("lookup: %v %v", found, err)
						return nil, err
					}
					if index%2 == 0 {
						finish, err := BeginWriteEligibility(ctx)
						if err != nil {
							return nil, err
						}
						defer finish()
						_ = value.(xhandler.DML).Insert("records", nil)
						if source.data.mutations != 0 {
							t.Error("guarded concurrent invocation delegated")
						}
						return nil, finish()
					}
					return nil, value.(xhandler.DML).Insert("records", nil)
				})})
			if index%2 == 0 {
				if !errors.Is(err, ErrWriteEligibilityMutation) {
					t.Errorf("guarded error: %v", err)
				}
			} else if err != nil || source.data.mutations != 2 {
				t.Errorf("unguarded error %v delegates %d", err, source.data.mutations)
			}
		}(index)
	}
	wg.Wait()
}

type eligibilityLogger struct{ calls int }

func (*eligibilityLogger) Debug(string, ...any)  {}
func (l *eligibilityLogger) Info(string, ...any) { l.calls++ }
func (*eligibilityLogger) Warn(string, ...any)   {}
func (*eligibilityLogger) Error(string, ...any)  {}

func TestWriteEligibilityAllowsMetadataAndLogging(t *testing.T) {
	source := &guardSource{data: &guardProbe{}, key: "main"}
	sink := &eligibilityLogger{}
	_, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{}, DataSource: source,
		Capabilities: rhandler.InvocationCapabilities{Logger: sink},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
			value, found, err := inv.Binder.Lookup(ctx, xhandler.DMLKey)
			if err != nil || !found {
				t.Fatalf("DML lookup: %v %v", found, err)
			}
			log, found, err := inv.Binder.Lookup(ctx, xhandler.LoggerKey)
			if err != nil || !found {
				t.Fatalf("logger lookup: %v %v", found, err)
			}
			finish, err := BeginWriteEligibility(ctx)
			if err != nil {
				return nil, err
			}
			defer finish()
			if _, err := value.(rhandler.DialectProvider).Dialect(context.Background()); err != nil {
				return nil, err
			}
			log.(xhandler.Logger).Info("eligibility decision")
			if source.data.mutations != 0 || source.data.reads != 1 || sink.calls != 1 {
				t.Fatal("read-only capabilities changed")
			}
			return nil, finish()
		})})
	if err != nil {
		t.Fatal(err)
	}
}
