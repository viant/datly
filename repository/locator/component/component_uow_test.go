package component

import (
	"context"
	"database/sql"
	"net/http"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/executor/uow"
	"github.com/viant/datly/view/state/kind/locator"
)

type componentTestOperation string

func (o componentTestOperation) TableName() string { return string(o) }

type componentScopeDispatcher struct {
	db    *sql.DB
	order *[]string
}

func (d *componentScopeDispatcher) Dispatch(ctx context.Context, path *contract.Path, _ ...contract.Option) (interface{}, error) {
	ctx, _, frame, _, err := uow.Enter(ctx, path.Method+" "+path.URI)
	if err != nil {
		return nil, err
	}
	defer frame.Seal()
	buffer := frame.NewBuffer(func(context.Context) (*sql.DB, error) { return d.db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			*d.order = append(*d.order, string(value.(componentTestOperation)))
			return nil
		})
	if err = buffer.Append(componentTestOperation(path.URI)); err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

func TestComponentLocatorCreatesOrderedBindingFrames(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	var order []string
	dispatcher := &componentScopeDispatcher{db: db, order: &order}
	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	componentLocator := &componentLocator{
		dispatch: dispatcher,
		getRequest: func() (*http.Request, error) {
			return request, nil
		},
	}
	for _, binding := range []struct {
		order string
		name  string
	}{{"00000001", "GET:/second"}, {"00000000", "GET:/first"}} {
		bindingCtx := uow.WithBindingOrder(ctx, binding.order)
		if _, found, err := componentLocator.Value(bindingCtx, reflect.TypeOf(""), binding.name); err != nil || !found {
			t.Fatalf("Value(%s) found=%v err=%v", binding.name, found, err)
		}
	}
	rootBuffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			order = append(order, string(value.(componentTestOperation)))
			return nil
		})
	if err := rootBuffer.Append(componentTestOperation("root")); err != nil {
		t.Fatal(err)
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"root", "/first", "/second"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestComponentLocatorRequiresInvocationDispatcher(t *testing.T) {
	if _, err := newComponentLocator(locator.WithConstants(nil)); err == nil {
		t.Fatal("expected missing dispatcher error")
	}
}
