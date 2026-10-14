package engine

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

func TestInvokerErrorFinalizerGateVetoesOwnedCommitSQLite(t *testing.T) {
	for _, protected := range []bool{false, true} {
		for _, returnCause := range []bool{false, true} {
			t.Run(map[bool]string{false: "ordinary", true: "protected"}[protected]+map[bool]string{false: "/nil", true: "/cause"}[returnCause], func(t *testing.T) {
				db := testharness.NewSQLiteHarness(t)
				if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
				failure := errors.New("operation failed")
				providerCalls, commits, completions := 0, 0, 0
				out := &invokerErrorOutput{Value: "original", hook: func(_ context.Context, _ xhandler.ComponentInvoker, cause error) error {
					if cause != failure {
						t.Fatalf("exact operation cause=%v", cause)
					}
					if returnCause {
						return cause
					}
					return nil
				}}
				actual, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[invokerErrorOutput](), BufferedComponentCalls: protected, DataSource: dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }},
					ComponentInvoker: handlerprovider.Named("component_invoker", func(context.Context, reflect.Type, string) (any, bool, error) {
						providerCalls++
						return nil, false, errors.New("gate resolved provider")
					}),
					Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
						value, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
						if err != nil {
							return nil, err
						}
						if err = value.(xhandler.Data).Execute("INSERT INTO records VALUES(1)"); err != nil {
							return nil, err
						}
						return out, failure
					}), Completion: func(outcome xhandler.Outcome) {
						completions++
						if outcome.Error == nil || outcome.CommitConfirmed() {
							t.Fatalf("failure outcome=%+v", outcome)
						}
					},
				})
				var count int
				if scanErr := db.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records").Scan(&count); scanErr != nil {
					t.Fatal(scanErr)
				}
				if !errors.Is(err, failure) || actual != out || out.calls != 1 || out.Value != "original" || providerCalls != 0 || commits != 0 || completions != 1 || count != 0 {
					t.Fatalf("err=%v result=%T hooks=%d providers=%d commits=%d completions=%d rows=%d", err, actual, out.calls, providerCalls, commits, completions, count)
				}
			})
		}
	}
}

func TestInvokerErrorFinalizerCompositionOwnershipAndOrderSQLite(t *testing.T) {
	for _, mode := range []string{"ordinary", "protected", "neutral", "caller", "inherited", "outcome adapter", "child veto", "returned child veto", "hook veto", "late drain failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx := xmcp.WithContext(t.Context(), engineMCPContext{})
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			var tx *sql.Tx
			if mode == "caller" {
				var err error
				tx, err = db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
			}
			source := dml.Source{DB: db.DB, Tx: tx}
			var order []string
			commits, completions, children, outcomes := 0, 0, 0, 0
			source.OnCommit = func(context.Context) { commits++; order = append(order, "commit") }
			input := testRouteInput(t, reflect.TypeFor[struct{}]())
			var owner *dataScope
			var ownerTx *sql.Tx
			childFailure := errors.New("child veto")
			hookFailure := errors.New("hook veto")
			out := &invokerErrorOutput{hook: func(ctx context.Context, invoker xhandler.ComponentInvoker, cause error) error {
				order = append(order, "finalize")
				if cause != nil {
					t.Fatalf("successful boundary received %v", cause)
				}
				if owner.completionStarted || commits != 0 {
					t.Fatal("composition ran after completion")
				}
				if ownerTx != nil {
					var count int
					if err := ownerTx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
						t.Fatal(err)
					}
					want := 1
					if mode == "protected" || mode == "caller" || mode == "inherited" {
						want = 0
					}
					if count != want {
						t.Fatalf("pre-hook preparation count=%d want=%d", count, want)
					}
				}
				_, err := invoker.Invoke(context.Background(), xhandler.ComponentRequest{})
				if mode == "hook veto" {
					return hookFailure
				}
				if mode == "child veto" {
					return nil
				} // A swallowed failure still vetoes completion.
				return err
			}}
			adapter := rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
				order = append(order, "execute")
				owner = mainScope(ctx)
				if owner.root != nil {
					owner = owner.root
				}
				if mode != "neutral" {
					value, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
					if err != nil {
						return nil, err
					}
					native := mainScope(ctx).data.(*dml.Data)
					if err = native.Start(ctx); err != nil {
						return nil, err
					}
					_, ownerTx = native.InvocationTransaction()
					if err = value.(xhandler.Data).Execute("INSERT INTO records VALUES(1)"); err != nil {
						return nil, err
					}
				}
				return out, nil
			})
			request := Request{Input: input, OutputType: reflect.TypeFor[invokerErrorOutput](), Handler: adapter, BufferedComponentCalls: mode == "protected"}
			if mode != "neutral" {
				request.DataSource = source
			}
			request.ComponentInvoker = handlerprovider.Static(xhandler.ComponentInvokerKey, lifecycleInvoker(func(ctx context.Context, _ xhandler.ComponentRequest) (any, error) {
				children++
				order = append(order, "child")
				if _, ok := xmcp.LookupContext(ctx); !ok {
					t.Fatal("call context lost invocation providers")
				}
				root := mainScope(ctx)
				if root.root != nil {
					root = root.root
				}
				if root != owner {
					t.Fatal("invoker escaped root")
				}
				return New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: input, DataSource: source, Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
					value, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
					if err != nil {
						return nil, err
					}
					native := mainScope(ctx).data.(*dml.Data)
					if err = native.Start(ctx); err != nil {
						return nil, err
					}
					_, childTx := native.InvocationTransaction()
					if ownerTx != nil && childTx != ownerTx {
						t.Fatal("child did not share owning transaction")
					}
					if mode == "neutral" {
						ownerTx = childTx
					}
					statement := "INSERT INTO records VALUES(2)"
					if mode == "late drain failure" {
						statement = "INSERT INTO missing_records VALUES(2)"
					}
					if err = value.(xhandler.Data).Execute(statement); err != nil {
						return nil, err
					}
					if mode == "child veto" || mode == "returned child veto" {
						return nil, childFailure
					}
					return nil, nil
				})})
			}))
			request.Completion = func(outcome xhandler.Outcome) {
				completions++
				order = append(order, "complete")
				if (outcome.Error != nil) != (strings.Contains(mode, "veto") || mode == "late drain failure") {
					t.Fatalf("completion=%+v", outcome)
				}
			}
			if mode == "outcome adapter" {
				request.Handler = &outcomeAwareHandler{execute: adapter, finalize: func(_ context.Context, _ rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
					outcomes++
					order = append(order, "outcome")
					if !outcome.CommitConfirmed() {
						t.Fatalf("later outcome=%+v", outcome)
					}
					return nil
				}}
			}
			var actual any
			var err error
			if mode == "inherited" {
				nested := request
				nested.Completion = nil
				actual, err = New().Execute(ctx, Request{Input: input, DataSource: source, Completion: request.Completion, Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
					_, nestedErr := New().Execute(PrepareComponent(ctx, ComponentImperative, ""), nested)
					return nil, nestedErr
				})})
			} else {
				actual, err = New().Execute(ctx, request)
			}
			failed := strings.Contains(mode, "veto") || mode == "late drain failure"
			if (err != nil) != failed || out.calls != 1 || out.seen != nil || children != 1 || completions != 1 {
				t.Fatalf("error=%v output=%T hooks=%d children=%d completions=%d", err, actual, out.calls, children, completions)
			}
			if (mode == "child veto" || mode == "returned child veto") && !errors.Is(err, childFailure) {
				t.Fatalf("child cause lost: %v", err)
			}
			if mode == "hook veto" && !errors.Is(err, hookFailure) {
				t.Fatalf("hook cause lost: %v", err)
			}
			if mode == "late drain failure" && actual != nil {
				t.Fatal("completion failure did not suppress result")
			}
			wantCommits := 1
			if failed || mode == "caller" {
				wantCommits = 0
			}
			if commits != wantCommits {
				t.Fatalf("commits=%d want=%d order=%v", commits, wantCommits, order)
			}
			if mode == "outcome adapter" {
				if outcomes != 1 || out.mcpCalls != 0 {
					t.Fatal("duplicate adapter completion/MCP callback")
				}
			} else if out.mcpCalls != map[bool]int{true: 0, false: 1}[failed] {
				t.Fatalf("MCP calls=%d", out.mcpCalls)
			}
			if !failed && mode != "caller" {
				got := strings.Join(order, ",")
				want := "execute,finalize,child,commit,complete"
				if mode == "outcome adapter" {
					want = "execute,finalize,child,commit,outcome,complete"
				}
				if got != want {
					t.Fatalf("order=%s want=%s", got, want)
				}
			}
			var count int
			if tx != nil {
				if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 2 {
					t.Fatalf("caller pending rows=%d err=%v", count, err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatalf("caller transaction completed by engine: %v", err)
				}
			}
			if err := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
				t.Fatal(err)
			}
			wantRows := 2
			if mode == "neutral" {
				wantRows = 1
			}
			if failed || mode == "caller" {
				wantRows = 0
			}
			if count != wantRows {
				t.Fatalf("rows=%d want=%d", count, wantRows)
			}
		})
	}
}

func TestInvokerErrorFinalizerPreparationBoundarySQLite(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary sees preparation failure", true: "protected sees success before future preparation"}[protected], func(t *testing.T) {
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER CHECK(id>0))"); err != nil {
				t.Fatal(err)
			}
			commits, completions := 0, 0
			out := &invokerErrorOutput{hook: func(_ context.Context, _ xhandler.ComponentInvoker, cause error) error {
				if (cause != nil) == protected {
					t.Fatalf("protected=%v observed=%v", protected, cause)
				}
				return nil
			}}
			actual, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[invokerErrorOutput](), DataSource: dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}, BufferedComponentCalls: protected,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
					value, _, err := in.Binder.Lookup(ctx, xhandler.DataKey)
					if err != nil {
						return nil, err
					}
					if err = value.(xhandler.Data).Execute("INSERT INTO records VALUES(-1)"); err != nil {
						return nil, err
					}
					return out, nil
				}), Completion: func(outcome xhandler.Outcome) {
					completions++
					if outcome.Error == nil || outcome.CommitConfirmed() {
						t.Fatalf("prepare failure outcome=%+v", outcome)
					}
				},
			})
			var count int
			if scanErr := db.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records").Scan(&count); scanErr != nil {
				t.Fatal(scanErr)
			}
			if err == nil || actual != nil || out.calls != 1 || commits != 0 || completions != 1 || count != 0 {
				t.Fatalf("result=%T err=%v hook=%d commits=%d completions=%d rows=%d", actual, err, out.calls, commits, completions, count)
			}
			if !protected && !errors.Is(err, out.seen) {
				t.Fatalf("preparation cause lost: observed=%v err=%v", out.seen, err)
			}
		})
	}
}
