package command_test

import (
	"bytes"
	"context"
	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/internal/testharness"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCommandPathSemanticsOwnedListener(t *testing.T) {
	for _, mode := range []string{"", "escaped", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			f := fixture.New(t)
			f.WriteConfig(t, func(c map[string]any) { c["PathSemantics"] = mode })
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var out, diagnostics testharness.Output
			done := make(chan int, 1)
			go func() {
				done <- (command.Service{Registry: exports}).Run(ctx, []string{"run", "-conf", f.Config}, &out, &diagnostics)
			}()
			defer func() {
				cancel()
				select {
				case code := <-done:
					if code != 0 {
						t.Errorf("exit %d %s", code, diagnostics.String())
					}
				case <-time.After(10 * time.Second):
					t.Error("shutdown timeout")
				}
			}()
			address, err := out.WaitLine(ctx, "HTTP listening on ")
			if err != nil {
				t.Fatal(err, diagnostics.String())
			}
			client := &http.Client{Timeout: 3 * time.Second}
			res, err := client.Get("http://" + address + "/records/a%2Fb")
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			want := 400
			if mode == "decoded" {
				want = 404
			}
			if res.StatusCode != want {
				t.Fatalf("mode %q status %d wanted %d", mode, res.StatusCode, want)
			}
			res, err = client.Get("http://" + address + "/records/1")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != 200 || !strings.Contains(string(body), "first") {
				t.Fatal("normal path changed")
			}
		})
	}
}
func TestCommandRejectsInvalidPathSemanticsBeforeListener(t *testing.T) {
	f := fixture.New(t)
	f.WriteConfig(t, func(c map[string]any) { c["PathSemantics"] = "invalid" })
	var out, diagnostics bytes.Buffer
	code := (command.Service{}).Run(context.Background(), []string{"run", "-conf", f.Config}, &out, &diagnostics)
	if code != 1 || strings.Contains(out.String(), "listening on") {
		t.Fatal(code, out.String(), diagnostics.String())
	}
}
