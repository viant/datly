package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

type bufferedTerminalOutput struct {
	hook func(context.Context, error) error
}

func (o *bufferedTerminalOutput) Finalize(ctx context.Context, cause error) error {
	return o.hook(ctx, cause)
}

type bufferedTerminalSource struct {
	db     *sql.DB
	native *dml.Data
}

func (s *bufferedTerminalSource) InvocationKey() any { return s.db }
func (s *bufferedTerminalSource) Open(ctx context.Context) (xh.Data, error) {
	value, err := (dml.Source{DB: s.db}).Open(ctx)
	if err != nil {
		return nil, err
	}
	s.native = value.(*dml.Data)
	return value, nil
}
func TestBufferedTerminalObserverAdmission(t *testing.T) {
	for _, mode := range []string{"observer success", "observer veto", "prepare failure", "ignored proxy write", "ignored native write", "ignored component invocation", "ignored native flush", "ignored native complete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			source := &bufferedTerminalSource{db: db.DB}
			callbacks := 0
			childCalls := 0
			var observed error
			var attempt error
			sentinel := errors.New("observer veto")
			childSpec := componentSpec("TerminalChild", "POST", "/terminal-child", nil)
			ca := componentArtifact(t, childSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
			child := &registry.RegisteredComponent{Component: ca.Component, Input: ca.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { childCalls++; return &struct{}{}, nil })}
			rootSpec := componentSpec("TerminalRoot", "POST", "/terminal-root", nil)
			rootSpec.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
			a := componentArtifact(t, rootSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[bufferedTerminalOutput]())
			root := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[bufferedTerminalOutput](), DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("data %v", err)
				}
				data := value.(xh.Data)
				sqlText := "INSERT INTO records VALUES(1,'root')"
				if mode == "prepare failure" {
					sqlText = "INSERT INTO missing_records VALUES(1)"
				}
				if err = data.Execute(sqlText); err != nil {
					return nil, err
				}
				value, found, err = inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("invoker %v", err)
				}
				invoker := value.(dexec.ComponentInvoker)
				return &bufferedTerminalOutput{hook: func(ctx context.Context, cause error) error {
					callbacks++
					observed = cause
					switch mode {
					case "observer veto":
						return sentinel
					case "ignored proxy write":
						attempt = data.Execute("INSERT INTO records VALUES(2,'late proxy')")
					case "ignored native write":
						attempt = source.native.Execute("INSERT INTO records VALUES(2,'late native')")
					case "ignored component invocation":
						_, attempt = invoker.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: childSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/terminal-child"}}, Input: &struct{}{}})
					case "ignored native flush":
						attempt = source.native.Flush(ctx, "")
					case "ignored native complete":
						attempt = source.native.Complete(ctx, nil)
					}
					return nil
				}}, nil
			})}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, child})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: rootSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/terminal-root"}}, Input: &struct{}{}})
			if callbacks != 1 {
				t.Fatalf("observer calls%d", callbacks)
			}
			if mode == "prepare failure" {
				if observed == nil || err == nil || !strings.Contains(observed.Error(), "missing_records") {
					t.Fatalf("preparation error lost: observed%v outcome%v", observed, err)
				}
			} else if observed != nil {
				t.Fatalf("unexpected initial cause %v", observed)
			}
			if mode == "observer veto" && !errors.Is(err, sentinel) {
				t.Fatalf("observer identity %v", err)
			}
			failure := mode != "observer success"
			if strings.HasPrefix(mode, "ignored") {
				if attempt == nil {
					t.Errorf("terminal operation unexpectedly admitted")
				}
				if err == nil {
					t.Errorf("ignored terminal operation permitted successful completion")
				}
			}
			if childCalls != 0 {
				t.Errorf("terminal component reached business %d", childCalls)
			}
			count := 0
			if scanErr := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); scanErr != nil {
				t.Fatal(scanErr)
			}
			want := 1
			if failure {
				want = 0
			}
			if count != want {
				t.Fatalf("terminal outcome committed rows%d want%d: %v", count, want, err)
			}
		})
	}
}
