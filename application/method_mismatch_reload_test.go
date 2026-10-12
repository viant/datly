package application_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/viant/datly/application"
	"github.com/viant/datly/typecatalog"
)

func TestMethodMismatchReloadSQLite(t *testing.T) {
	ctx := context.Background()
	f := &reloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, step := range []struct {
		policy  string
		want    int
		invalid bool
	}{{"", 405, false}, {"not-found", 404, false}, {"invalid", 404, true}, {"method-not-allowed", 405, false}} {
		err = manager.Reload(ctx, application.Request{Revision: uint64(i + 1), Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(1)(ctx, types)
			if err != nil {
				return nil, err
			}
			built.HTTP.MethodMismatchPolicy = step.policy
			return built, nil
		}})
		if (err != nil) != step.invalid {
			t.Fatalf("reload%d: %v", i, err)
		}
		res := httptest.NewRecorder()
		manager.ServeHTTP(res, httptest.NewRequest("DELETE", "/records", nil))
		if res.Code != step.want {
			t.Fatalf("reload%d status%d wanted%d", i, res.Code, step.want)
		}
	}
}
