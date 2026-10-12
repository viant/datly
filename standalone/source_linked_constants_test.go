package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/linkedconstants"
)

func TestLinkedReaderDiscoversConstantProjectionsWithoutSources(t *testing.T) {
	for _, tc := range []struct{ route, label string }{
		{"bound", "publisher's constant"},
		{"unsafe", "publisher constant"},
	} {
		t.Run(tc.route, func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{"github.com/viant/datly/standalone/testdata/linkedconstants"}, "LinkedOnly": true}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			if err != nil {
				t.Fatal(err)
			}
			cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
			cfg.Const, err = constant.New(map[string]string{"Constant": tc.label, "Count": "7"})
			if err != nil {
				t.Fatal(err)
			}
			_ = linkedconstants.LinkedType
			server, err := New(ctx, Options{Config: cfg, RequireLinked: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err = server.Reload(ctx, 1); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest("GET", "/linked-constants/"+tc.route+"?Constant=request-override&Count=99", nil))
			var body struct {
				Rows []linkedconstants.Record `json:"rows"`
			}
			if err = json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != 200 || len(body.Rows) != 1 || body.Rows[0].ID != 1 || body.Rows[0].Label != tc.label || body.Rows[0].Count != 7 {
				t.Fatalf("linked constant response: %d %s (%v)", response.Code, response.Body.String(), err)
			}
		})
	}
}

func TestLinkedReaderDiscoversJSONHiddenComputedColumnsWithoutSources(t *testing.T) {
	for _, tc := range []struct {
		name            string
		empty, nullDate bool
	}{{name: "populated"}, {name: "empty", empty: true}, {name: "null-date", nullDate: true}} {
		t.Run(tc.name, func(t *testing.T) {
			empty := tc.empty
			ctx := context.Background()
			f := fixture.New(t)
			if err := f.DB.ExecStatements(ctx, "ALTER TABLE records ADD COLUMN created_at TEXT", "UPDATE records SET created_at=\"2026-10-12 07:00:00\""); err != nil {
				t.Fatal(err)
			}
			mode := "frozen"
			if tc.nullDate {
				mode = "null"
				if err := f.DB.ExecStatements(ctx, "UPDATE records SET created_at=NULL"); err != nil {
					t.Fatal(err)
				}
			}
			if empty {
				if err := f.DB.ExecStatements(ctx, "DELETE FROM records"); err != nil {
					t.Fatal(err)
				}
			}
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{"github.com/viant/datly/standalone/testdata/linkedconstants"}, "LinkedOnly": true}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			if err != nil {
				t.Fatal(err)
			}
			cfg.BaseDir = filepath.Join(t.TempDir(), "missing-source")
			cfg.Const, err = constant.New(map[string]string{"Constant": mode, "Count": "7"})
			if err != nil {
				t.Fatal(err)
			}
			_ = linkedconstants.LinkedType
			server, err := New(ctx, Options{Config: cfg, RequireLinked: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err = server.Reload(ctx, 1); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, httptest.NewRequest("GET", "/linked-constants/computed", nil))
			var body struct {
				Rows []map[string]any `json:"rows"`
			}
			if err = json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != 200 || (!empty && len(body.Rows) != 1) || (empty && len(body.Rows) != 0) {
				t.Fatalf("computed response: %d %s (%v)", response.Code, response.Body.String(), err)
			}
			if empty {
				return
			}
			row := body.Rows[0]
			if len(row) != 2 || row["id"] != float64(1) || row["readMode"] != mode {
				t.Fatalf("hidden columns leaked or result changed: %#v", row)
			}
		})
	}
}
