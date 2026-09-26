package build_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/project/build"
)

func TestInitializedBinarySQLiteRefresh(t *testing.T) {
	root, rootErr := filepath.EvalSymlinks(t.TempDir())
	if rootErr != nil {
		t.Fatal(rootErr)
	}
	app := filepath.Join(root, "app")
	model := filepath.Join(root, "models")
	for _, dir := range []string{app, model} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	(testharness.GeneratedModule{Path: "example.com/buildapp"}).Write(t, app)
	(testharness.GeneratedModule{Path: "example.com/buildmodel"}).Write(t, model)
	for _, pair := range [][2]string{{"testdata/app/records", filepath.Join(app, "records")}, {"testdata/app/hooks", filepath.Join(app, "hooks")}, {"testdata/app/datlylink", filepath.Join(app, "internal/datlylink")}, {"testdata/app/models", model}} {
		if err := os.CopyFS(pair[1], os.DirFS(pair[0])); err != nil {
			t.Fatal(err)
		}
	}
	service := build.Service{}
	if err := service.Init(context.Background(), build.InitRequest{Dir: app, Local: map[string]string{"example.com/buildmodel": model}}); err != nil {
		t.Fatal(err)
	}
	// A real workspace must override the process-wide GOWORK=off used for Datly's tests.
	work := filepath.Join(root, "go.work")
	if err := os.WriteFile(work, []byte("go 1.25.8\nuse (\n./app\n./models\n)\n"), 0644); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "GOWORK="+work)
	(testharness.GeneratedModule{}).WriteSums(t, app)
	(testharness.GeneratedModule{}).WriteSums(t, model)
	originalMod, err := os.ReadFile(filepath.Join(app, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	originalHook, err := os.ReadFile(filepath.Join(app, "hooks/write.go"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	req := build.Request{Dir: app, Env: append(append([]string{}, env...), "DATLY_FACTORY_DISCOVERY_SENTINEL=1")}
	result, err := service.Build(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if result.Components != 2 {
		t.Fatalf("result %+v", result)
	}
	dsn := filepath.Join(root, "records.db")
	db := sqlite.New(t, sqlite.WithDSN(dsn))
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'first')"); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(app, "config.json")
	cfg := map[string]any{"BaseDir": app, "Endpoint": map[string]any{"Address": "127.0.0.1:0"}, "GoBootstrap": map[string]any{"Packages": []string{"example.com/buildapp/..."}}, "Connector": "main", "Connectors": []any{map[string]any{"Name": "main", "Driver": "sqlite3", "DSN": dsn}}}
	data, _ := json.Marshal(cfg)
	if err = os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	runBinary(t, result.Binary, config, func(base string) {
		response, err := http.Get(base + "/records/1")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != 200 || !bytes.Contains(body, []byte("first")) {
			t.Fatalf("reader %d %s", response.StatusCode, body)
		}
		response, err = http.Post(base+"/records", "application/json", strings.NewReader(`{"data":{"id":3,"name":"tcp"}}`))
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != 200 || !bytes.Contains(body, []byte(`"finalized":true`)) {
			t.Fatalf("mutation %d %s", response.StatusCode, body)
		}
	})
	var count int
	if err := db.DB.QueryRow("SELECT count(*) FROM records WHERE id=3 AND name='tcp'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("mutation persistence %d %v", count, err)
	}
	if err = service.Init(ctx, build.InitRequest{Dir: app}); err != nil {
		t.Fatal(err)
	}
	repeat, err := service.Build(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if repeat.SHA256 != result.SHA256 {
		t.Fatal("repeat build changed binary")
	}
	extraDir := filepath.Join(app, "extra")
	if err = os.MkdirAll(extraDir, 0755); err != nil {
		t.Fatal(err)
	}
	extra := filepath.Join(extraDir, "extra.go")
	added := "//go:build extra\n\npackage extra\nimport (\"reflect\"; xdatly \"github.com/viant/xdatly\"; records \"example.com/buildapp/records\")\ntype Extra struct{ Read xdatly.Component[records.Input,records.Output] `component:\"Extra,path=/extra/{id},method=GET,connector=main,view=records\"` }\nvar ComponentType = reflect.TypeOf((*Extra)(nil)).Elem()\n"
	if err = os.WriteFile(extra, []byte(added), 0644); err != nil {
		t.Fatal(err)
	}
	extraLink := filepath.Join(app, "internal/datlylink/extra.go")
	linked := "//go:build extra\n\npackage datlylink\nimport _ \"example.com/buildapp/extra\"\nfunc init(){}\n"
	if err = os.WriteFile(extraLink, []byte(linked), 0644); err != nil {
		t.Fatal(err)
	}
	req.Tags = "extra"
	result, err = service.Build(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if result.Components != 3 {
		t.Fatalf("add %+v", result)
	}
	runBinary(t, result.Binary, config, func(base string) {
		r, err := http.Get(base + "/extra/1")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			b, _ := io.ReadAll(r.Body)
			t.Fatalf("added %d %s", r.StatusCode, b)
		}
	})
	if err = os.RemoveAll(extraDir); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(extraLink); err != nil {
		t.Fatal(err)
	}
	req.Tags = ""
	result, err = service.Build(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if result.Components != 2 {
		t.Fatal("removed component retained")
	}
	runBinary(t, result.Binary, config, func(base string) {
		r, err := http.Get(base + "/extra/1")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		if r.StatusCode != 404 {
			t.Fatalf("removed %d", r.StatusCode)
		}
	})
	for _, pair := range []struct {
		path string
		want []byte
	}{{"go.mod", originalMod}, {"hooks/write.go", originalHook}} {
		actual, _ := os.ReadFile(filepath.Join(app, pair.path))
		if !bytes.Equal(pair.want, actual) {
			t.Fatalf("authored %s changed", pair.path)
		}
	}

	beforeBinary, _ := os.ReadFile(result.Binary)
	broken := filepath.Join(app, "records/compile_fail.go")
	if err = os.WriteFile(broken, []byte("package records\nvar InvalidAssignment string=123\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Build(ctx, req); err == nil || !strings.Contains(err.Error(), "compile linked project") {
		t.Fatalf("expected native compile error, got %v", err)
	}
	afterBinary, _ := os.ReadFile(result.Binary)
	if sha256.Sum256(beforeBinary) != sha256.Sum256(afterBinary) {
		t.Fatal("failed build changed previous binary")
	}
	if err = os.Remove(broken); err != nil {
		t.Fatal(err)
	}
	t.Logf("final binary SHA256=%x", sha256.Sum256(afterBinary))
	// Excluded malformed declarations must never reach the metadata parser.
	bad := filepath.Join(app, "records/excluded.go")
	if err = os.WriteFile(bad, []byte("//go:build forbidden\n\npackage records\ntype Bad struct{X int `component:\"broken\"`}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Build(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.Tags = "forbidden"
	if _, err = service.Build(ctx, req); err == nil {
		t.Fatal("malformed selected metadata accepted")
	}
}

func runBinary(t *testing.T, binary, config string, check func(string)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out, diag testharness.Output
	cmd := exec.Command(binary, "run", "-conf", config)
	cmd.Stdout = &out
	cmd.Stderr = &diag
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); cancel() }()
	defer func() { _ = cmd.Process.Kill(); <-done }()
	defer func() {
		if diag.String() != "" {
			t.Logf("binary diagnostics: %s", diag.String())
		}
	}()
	address, err := out.WaitLine(ctx, "HTTP listening on ")
	if err != nil {
		if strings.Contains(diag.String(), "bind: operation not permitted") {
			t.Logf("TCP acceptance unavailable in sandbox: %s", diag.String())
			return
		}
		t.Fatalf("startup %v %s", err, diag.String())
	}
	check("http://" + address)
}
