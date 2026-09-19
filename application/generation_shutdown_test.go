package application_test

import (
	"context"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/typecatalog"
)

func TestManagerShutsDownRetiredAndActiveBuildResources(t *testing.T) {
	manager, err := application.New(typecatalog.NewCatalog())
	if err != nil {
		t.Fatal(err)
	}
	firstClosed := make(chan struct{}, 1)
	secondClosed := make(chan struct{}, 1)
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Shutdown: func(context.Context) error { firstClosed <- struct{}{}; return nil }}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 2, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Shutdown: func(context.Context) error { secondClosed <- struct{}{}; return nil }}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, firstClosed, "retired generation")
	select {
	case <-secondClosed:
		t.Fatal("active generation shut down before manager shutdown")
	default:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = manager.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, secondClosed, "active generation")
}

func TestManagerShutsDownRejectedBuildResources(t *testing.T) {
	manager, err := application.New(typecatalog.NewCatalog())
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{}, 1)
	err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{MCP: mcp.Config{Components: []*registry.RegisteredComponent{nil}}, Shutdown: func(context.Context) error { closed <- struct{}{}; return nil }}, nil
	}})
	if err == nil {
		t.Fatal("invalid stage was published")
	}
	awaitSignal(t, closed, "rejected stage")
}

func awaitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("%s resources were not shut down", name)
	}
}
