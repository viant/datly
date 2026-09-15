package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestInstanceFilesSameAuthoredArtifactSQLite(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	sqlPath := filepath.Join(f.Root, "records/queries/read.sql")
	authored := "SELECT id, name FROM `$project.ds.records` WHERE id=:ID"
	if err := os.WriteFile(sqlPath, []byte(authored), 0600); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"e2e", "prod"} {
		if err := f.DB.ExecStatements(ctx, "CREATE TABLE `"+stage+".ds.records` (id INTEGER PRIMARY KEY, name TEXT)", "INSERT INTO `"+stage+".ds.records` VALUES (1,'"+stage+"')"); err != nil {
			t.Fatal(err)
		}
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	var servers []*Server
	var configurations []*config.Config
	var snapshots [][]byte
	for _, stage := range []string{"e2e", "prod"} {
		extension, body := ".yaml", "project: e2e\n"
		if stage == "prod" {
			extension, body = ".json", `{"project":"prod"}`
		}
		file := filepath.Join(f.Root, stage+extension)
		if err := os.WriteFile(file, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		f.WriteConfig(t, func(c map[string]any) { c["ConstURL"] = stage + extension })
		cfg, err := (config.Loader{}).Load(ctx, f.Config)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, _ := json.Marshal(cfg)
		snapshots = append(snapshots, snapshot)
		configurations = append(configurations, cfg)
		server, err := New(ctx, Options{Config: cfg, Registry: exports})
		if err != nil {
			t.Fatal(err)
		}
		servers = append(servers, server)
		t.Cleanup(func() {
			if err := server.Shutdown(context.Background()); err != nil {
				t.Error(err)
			}
		})
		if err := server.Reload(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	for revision := uint64(2); revision <= 3; revision++ {
		var wg sync.WaitGroup
		for index, server := range servers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				want := []string{"e2e", "prod"}[index]
				for n := 0; n < 10; n++ {
					response := httptest.NewRecorder()
					server.ServeHTTP(response, httptest.NewRequest("GET", "/records/1?project=request-controlled", nil))
					if response.Code != 200 || !strings.Contains(response.Body.String(), `"name":"`+want+`"`) {
						t.Errorf("%s: %d %s", want, response.Code, response.Body.String())
					}
				}
				if err := server.Reload(ctx, revision); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	}
	bytes, _ := os.ReadFile(sqlPath)
	if string(bytes) != authored {
		t.Fatal("persisted SQL changed")
	}
	for index, cfg := range configurations {
		after, _ := json.Marshal(cfg)
		if string(after) != string(snapshots[index]) {
			t.Fatal("shared configuration mutated")
		}
	}
}

func TestInstanceMalformedIdentifierFailsBeforeDatabaseOpen(t *testing.T) {
	ctx := context.Background()
	for _, body := range []string{`{"project":"bad;sql"}`, `{}`} {
		t.Run(body, func(t *testing.T) {
			f := fixture.New(t)
			if err := os.WriteFile(filepath.Join(f.Root, "records/queries/read.sql"), []byte("SELECT id,name FROM `$project.ds.records` WHERE id=:ID"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.Root, "instance.json"), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			database := filepath.Join(f.Root, "must-not-open.db")
			f.WriteConfig(t, func(c map[string]any) {
				c["ConstURL"] = "instance.json"
				c["Connectors"] = []any{map[string]any{"Name": "main", "Driver": "sqlite3", "DSN": database}}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			if err != nil {
				t.Fatal(err)
			}
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			server, err := New(ctx, Options{Config: cfg, Registry: exports})
			if server != nil {
				_ = server.Shutdown(ctx)
			}
			if err == nil || !strings.Contains(err.Error(), "identifier constant") {
				t.Fatalf("wrong startup failure: %v", err)
			}
			if _, err := os.Stat(database); !os.IsNotExist(err) {
				t.Fatalf("database was opened before validation: %v", err)
			}
		})
	}
}
