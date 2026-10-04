package application_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/typecatalog"
)

func TestServiceTimeReloadSQLite(t *testing.T) {
	ctx := context.Background()
	f := &reloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, step := range []struct {
		name, want string
		invalid    bool
	}{
		{"Datly-Service-Time", "Datly-Service-Time", false},
		{"bad name", "Datly-Service-Time", true},
		{"X-Execution-Time", "X-Execution-Time", false},
		{"", "", false},
	} {
		err = manager.Reload(ctx, application.Request{Revision: uint64(i + 1), Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(1)(ctx, types)
			if err != nil {
				return nil, err
			}
			built.HTTP.ServiceTimeHeader = step.name
			return built, nil
		}})
		if (err != nil) != step.invalid {
			t.Fatalf("reload %d: %v", i, err)
		}
		res := httptest.NewRecorder()
		manager.ServeHTTP(res, httptest.NewRequest("GET", "/records", nil))
		if res.Code != 200 {
			t.Fatalf("reload %d status=%d", i, res.Code)
		}
		for _, name := range []string{"Datly-Service-Time", "X-Execution-Time"} {
			if name == step.want {
				if _, err := time.ParseDuration(res.Header().Get(name)); err != nil {
					t.Fatal(err)
				}
			} else if res.Header().Get(name) != "" {
				t.Fatalf("stale header %s after reload %d", name, i)
			}
		}
	}
}
