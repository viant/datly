package command_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestRunAuthoredPackageSQLite(t *testing.T) {
	for _, commandName := range []string{"run", "start"} {
		t.Run(commandName, func(t *testing.T) {
			f := fixture.New(t)
			f.WriteConfig(t, func(cfg map[string]any) {
				cfg["Info"] = map[string]any{"title": "Records API", "version": "1"}
				cfg["CORS"] = map[string]any{"AllowOrigins": []string{"https://client.example"}}
			})
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var out, diagnostics testharness.Output
			done := make(chan int, 1)
			go func() {
				done <- (command.Service{Registry: exports}).Run(ctx, []string{commandName, "-conf", f.Config}, &out, &diagnostics)
				cancel()
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("CLI failed to exit")
				}
			})
			address, err := out.WaitLine(ctx, "HTTP listening on ")
			if err != nil {
				t.Fatalf("startup: %v %s", err, diagnostics.String())
			}
			client := &http.Client{Timeout: 3 * time.Second}
			request, _ := http.NewRequest("GET", "http://"+address+"/records/1", nil)
			request.Header.Set("Origin", "https://client.example")
			res, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			var read struct {
				Rows []records.Record `json:"rows"`
			}
			err = json.NewDecoder(res.Body).Decode(&read)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || len(read.Rows) != 1 || read.Rows[0].Name != "first" {
				t.Fatalf("read=%+v status=%d err=%v", read, res.StatusCode, err)
			}
			if res.Header.Get("Access-Control-Allow-Origin") != "https://client.example" {
				t.Fatal(res.Header)
			}
			res, err = client.Post("http://"+address+"/records", "application/json", strings.NewReader(`{"data":{"id":2,"name":"created"}}`))
			if err != nil {
				t.Fatal(err)
			}
			var written records.WriteOutput
			err = json.NewDecoder(res.Body).Decode(&written)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || written.Data == nil || written.Data.ID != 2 || !written.Finalized {
				t.Fatalf("write=%+v status=%d err=%v", written, res.StatusCode, err)
			}
			f.DB.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []records.Record{{ID: 1, Name: "first"}, {ID: 2, Name: "created"}})
			res, err = client.Get("http://" + address + "/v1/api/meta/openapi")
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != 200 || !bytes.Contains(data, []byte("Records API")) {
				t.Fatalf("OpenAPI: %d %s", res.StatusCode, data)
			}
			cancel()
			select {
			case status := <-done:
				if status != 0 {
					t.Fatalf("shutdown status=%d %s", status, diagnostics.String())
				}
				done <- status
			case <-time.After(10 * time.Second):
				t.Fatal("shutdown hung")
			}
			connection, err := net.DialTimeout("tcp", address, time.Second)
			if err == nil {
				connection.Close()
				t.Fatal("listener remained open")
			}
		})
	}
}

func TestExecutableSQLiteAndSignal(t *testing.T) {
	f := fixture.New(t)
	binary := filepath.Join(t.TempDir(), "datly")
	build := exec.Command("go", "build", "-o", binary, "../../standalone/testdata/app/cmd")
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build executable: %v %s", err, output)
	}
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var out, diagnostics testharness.Output
			process := exec.Command(binary, "start", "-c", f.Config)
			process.Stdout = &out
			process.Stderr = &diagnostics
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- process.Wait(); cancel() }()
			t.Cleanup(func() {
				_ = process.Process.Kill()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("process did not exit")
				}
			})
			address, err := out.WaitLine(ctx, "HTTP listening on ")
			if err != nil {
				t.Fatalf("startup: %v %s", err, diagnostics.String())
			}
			client := &http.Client{Timeout: 3 * time.Second}
			res, err := client.Get("http://" + address + "/records/1")
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != 200 || !bytes.Contains(data, []byte("first")) {
				t.Fatalf("read %d %s", res.StatusCode, data)
			}
			id := 3
			if sig == syscall.SIGTERM {
				id = 4
			}
			body, _ := json.Marshal(map[string]any{"data": records.Record{ID: id, Name: "executable"}})
			res, err = client.Post("http://"+address+"/records", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			data, _ = io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != 200 || !bytes.Contains(data, []byte(`"finalized":true`)) {
				t.Fatalf("write %d %s", res.StatusCode, data)
			}
			if err := process.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("exit %v %s", err, diagnostics.String())
				}
				done <- err
			case <-ctx.Done():
				t.Fatal("signal did not stop process")
			}
		})
	}
	f.DB.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []records.Record{{ID: 1, Name: "first"}, {ID: 3, Name: "executable"}, {ID: 4, Name: "executable"}})
}

func TestRunStartupFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *fixture.Fixture)
	}{
		{"missing package", func(t *testing.T, f *fixture.Fixture) {
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/missing"}}
			})
		}},
		{"missing resource", func(t *testing.T, f *fixture.Fixture) {
			if err := os.Remove(filepath.Join(f.Root, "records/queries/read.sql")); err != nil {
				t.Fatal(err)
			}
		}},
		{"invalid connector", func(t *testing.T, f *fixture.Fixture) {
			f.WriteConfig(t, func(c map[string]any) {
				c["Connectors"] = []any{map[string]any{"Name": "main", "Driver": "unlinked", "DSN": "private-password"}}
			})
		}},
		{"occupied listener", func(t *testing.T, f *fixture.Fixture) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { ln.Close() })
			f.WriteConfig(t, func(c map[string]any) { c["Endpoint"] = map[string]any{"Address": ln.Addr().String()} })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture.New(t)
			tc.change(t, f)
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			var out, diag bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			status := (command.Service{Registry: exports}).Run(ctx, []string{"run", "-conf", f.Config}, &out, &diag)
			if status != 1 || out.Len() != 0 || diag.Len() == 0 || strings.Contains(diag.String(), "private-password") {
				t.Fatalf("status=%d stdout=%s stderr=%s", status, &out, &diag)
			}
		})
	}
}
