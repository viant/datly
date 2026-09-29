package runtime

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/spec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type aliasTransactionInput struct{}
type aliasTransactionOutput struct{}

func TestConnectorAliasNestedWritesCommitOrRollbackTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollback=%t", fail), func(t *testing.T) {
			ctx := context.Background()
			set, err := connector.Open(ctx, []connector.Config{{Name: "studio", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "clone.sqlite")}, {Name: "authz", AliasOf: "studio"}}, "studio")
			if err != nil {
				t.Fatal(err)
			}
			defer set.Close()
			studio, err := set.ResolveDB(ctx, "studio")
			if err != nil {
				t.Fatal(err)
			}
			authz, err := set.ResolveDB(ctx, "authz")
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"versions", "policy_heads", "policy_history"} {
				if _, err := studio.ExecContext(ctx, "CREATE TABLE "+table+"(id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
			}
			childSpec := componentSpec("AliasPolicyWriter", "POST", "/alias/policy", nil)
			childArtifact := componentArtifact(t, childSpec, reflect.TypeFor[aliasTransactionInput](), reflect.TypeFor[aliasTransactionOutput]())
			child := &registry.RegisteredComponent{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeFor[aliasTransactionOutput](), DataSource: dml.Source{DB: authz}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("policy data unavailable: %v", err)
				}
				data := value.(xhandler.Data)
				if err = data.Execute("INSERT INTO policy_heads VALUES(1)"); err != nil {
					return nil, err
				}
				if err = data.Execute("INSERT INTO policy_history VALUES(1)"); err != nil {
					return nil, err
				}
				if err = data.Flush(ctx, ""); err != nil {
					return nil, err
				}
				return &aliasTransactionOutput{}, nil
			})}
			parentSpec := componentSpec("AliasVersionClone", "POST", "/alias/clone", nil)
			parentArtifact := componentArtifact(t, parentSpec, reflect.TypeFor[aliasTransactionInput](), reflect.TypeFor[aliasTransactionOutput]())
			parent := &registry.RegisteredComponent{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeFor[aliasTransactionOutput](), DataSource: dml.Source{DB: studio}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("version data unavailable: %v", err)
				}
				data := value.(xhandler.Data)
				if err = data.Execute("INSERT INTO versions VALUES(1)"); err != nil {
					return nil, err
				}
				if err = data.Flush(ctx, ""); err != nil {
					return nil, err
				}
				value, found, err = inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("component invoker unavailable: %v", err)
				}
				if _, err = value.(dexec.ComponentInvoker).InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: childSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/alias/policy"}}, Input: &aliasTransactionInput{}}); err != nil {
					return nil, err
				}
				if fail {
					return nil, errors.New("injected clone failure after policy flush")
				}
				return &aliasTransactionOutput{}, nil
			})}
			runtime, err := NewRuntime([]*registry.RegisteredComponent{parent, child})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Shutdown(ctx)
			_, err = runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/alias/clone"}}, Input: &aliasTransactionInput{}})
			if (err != nil) != fail {
				t.Fatalf("clone result: %v", err)
			}
			if fail && !strings.Contains(err.Error(), "injected clone failure after policy flush") {
				t.Fatalf("failure did not reach injected rollback: %v", err)
			}
			want := 1
			if fail {
				want = 0
			}
			for _, table := range []string{"versions", "policy_heads", "policy_history"} {
				var count int
				if err := studio.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != want {
					t.Fatalf("%s count=%d want=%d error=%v", table, count, want, err)
				}
			}
		})
	}
}
