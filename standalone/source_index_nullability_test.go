package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestIndexedReaderRetainsDatabaseNullabilityForScalarOutput(t *testing.T) {
	f := fixture.New(t)
	if err := f.DB.ExecStatements(context.Background(), "UPDATE records SET name=NULL WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	server.manager.ServeHTTP(res, httptest.NewRequest("GET", "/records/1", nil))
	var result struct {
		Rows []records.Record `json:"rows"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil || res.Code != 200 || len(result.Rows) != 1 || result.Rows[0].ID != 1 || result.Rows[0].Name != "" {
		t.Fatalf("nullable SQL column into original scalar output: %d %s (%v)", res.Code, res.Body.String(), err)
	}
}
