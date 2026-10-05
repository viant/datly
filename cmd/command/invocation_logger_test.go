package command_test

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/internal/testharness"
	ndiffer "github.com/viant/datly/runtime/differ"
	fixture "github.com/viant/datly/standalone/testdata/loggerapp"
	"github.com/viant/datly/standalone/testdata/loggerapp/components"
	xdiffer "github.com/viant/xdatly/differ"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCommandHostInvocationLoggerReachesGoBootstrapChild(t *testing.T) {
	f := fixture.New(t)
	log := &fixture.Logger{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	var output, diagnostics testharness.Output
	status := make(chan int, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		status <- (command.Service{Holders: []any{components.Component{}}, InvocationLogger: log}).Run(ctx, []string{"run", "-conf", f.Config}, &output, &diagnostics)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("command host did not exit")
		}
	})
	address, err := output.WaitLine(ctx, "HTTP listening on ")
	if err != nil {
		t.Fatalf("host startup %v %s", err, diagnostics.String())
	}
	client := &http.Client{Timeout: 3 * time.Second}
	result, err := client.Get("http://" + address + "/parent/1")
	if err != nil {
		t.Fatal(err)
	}
	var body components.ParentOutput
	err = json.NewDecoder(result.Body).Decode(&body)
	_ = result.Body.Close()
	if err != nil || result.StatusCode != 200 || len(body.Rows) != 1 || body.Rows[0].ID != 1 {
		t.Fatalf("GoBootstrap child %d %#v %v", result.StatusCode, body, err)
	}
	entries := log.Snapshot()
	if len(entries) != 4 || entries[0].Message != "parent.begin" || entries[1].Message != "reader.input" || entries[2].Message != "parent.end" || entries[3].Message != "reader.output" {
		t.Fatalf("service dropped configured logger %#v", entries)
	}
	cancel()
	select {
	case code := <-status:
		if code != 0 {
			t.Fatalf("shutdown %d %s", code, diagnostics.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("host shutdown timed out")
	}
}

func commandSourceRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// The host owns this instance; its call count proves command forwarding retains
// the exact object rather than constructing a native replacement.
type commandDiffer struct {
	native xdiffer.Differ
	calls  atomic.Int32
}

func (d *commandDiffer) Diff(ctx context.Context, from, to any, opts ...xdiffer.Option) (*xdiffer.ChangeLog, error) {
	d.calls.Add(1)
	return d.native.Diff(ctx, from, to, opts...)
}
func TestCommandHostInvocationDifferReachesNativeWriter(t *testing.T) {
	f := fixture.New(t)
	differ := &commandDiffer{native: ndiffer.New()}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	var output, diagnostics testharness.Output
	status := make(chan int, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		status <- (command.Service{Holders: []any{components.Component{}}, InvocationDiffer: differ}).Run(ctx, []string{"run", "-conf", f.Config}, &output, &diagnostics)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("command host did not exit")
		}
	})
	address, err := output.WaitLine(ctx, "HTTP listening on ")
	if err != nil {
		t.Fatalf("host startup %v %s", err, diagnostics.String())
	}
	request, err := http.NewRequestWithContext(ctx, "PATCH", "http://"+address+"/differ", strings.NewReader(`{"data":[{"id":1,"name":"command"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	result, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var body components.PatchOutput
	err = json.NewDecoder(result.Body).Decode(&body)
	_ = result.Body.Close()
	if err != nil || result.StatusCode != 200 || len(body.Changes) != 1 || body.Changes[0].From != "first" || body.Changes[0].To != "command" || differ.calls.Load() != 1 {
		t.Fatalf("command Differ forwarding: status=%d output=%#v calls=%d err=%v diagnostics=%s", result.StatusCode, body, differ.calls.Load(), err, diagnostics.String())
	}
	cancel()
	select {
	case code := <-status:
		if code != 0 {
			t.Fatalf("shutdown %d %s", code, diagnostics.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("host shutdown timed out")
	}
}
