package standalone

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/readgroups"
	"github.com/viant/datly/standalone/testdata/app/seedgroups"
)

func TestReadOnlyDependencySeedProducer(t *testing.T) {
	for _, mode := range []string{"eager", "indexed", "linked"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			f := fixture.New(t)
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/seedgroups", fixture.Module + "/readgroups"}, "EagerComponents": mode == "eager", "LinkedOnly": mode == "linked"}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			require.NoError(t, err)
			if mode == "linked" {
				cfg.BaseDir = filepath.Join(t.TempDir(), "no-source")
			}
			_ = seedgroups.DatlyLinkedType
			_ = seedgroups.LoaderLinkedType
			server, err := New(ctx, Options{Config: cfg})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			require.NoError(t, server.Reload(ctx, 1))
			paths := []string{filepath.Join(t.TempDir(), "first.json"), filepath.Join(t.TempDir(), "second.json")}
			for i, path := range paths {
				data, err := json.Marshal([]seedgroups.SnapshotRow{{ID: i + 1, Mask: 7 + i}, {ID: i + 1, Mask: 7 + i}})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			readRequest := func(callCtx context.Context, path, name string, sequential bool) *httptest.ResponseRecorder {
				query := url.Values{"url": {path}, "case": {name}}
				if sequential {
					query.Set("sequential", "true")
				}
				r := httptest.NewRecorder()
				server.ServeHTTP(r, httptest.NewRequest("GET", "/seed-producer?"+query.Encode(), nil).WithContext(callCtx))
				return r
			}
			read := func(path, name string, sequential bool) *httptest.ResponseRecorder {
				return readRequest(ctx, path, name, sequential)
			}
			type operation struct {
				name, path string
				entered    chan string
				done       chan *httptest.ResponseRecorder
				release    chan struct{}
				once       sync.Once
			}
			ops := make([]*operation, 2)
			for i, path := range paths {
				op := &operation{name: mode + "-" + filepath.Base(path), path: path, entered: make(chan string, 2), done: make(chan *httptest.ResponseRecorder, 1), release: make(chan struct{})}
				ops[i] = op
				t.Cleanup(func() { op.once.Do(func() { close(op.release) }) })
				remove := readgroups.Install(op.name, &readgroups.Probe{Entered: op.entered, Release: op.release})
				defer remove()
				go func() { op.done <- read(op.path, op.name, false) }()
			}
			for _, op := range ops {
				branches := []string{}
				for range 2 {
					select {
					case branch := <-op.entered:
						branches = append(branches, branch)
					case r := <-op.done:
						t.Fatalf("returned before overlap: %d %s", r.Code, r.Body.String())
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				require.ElementsMatch(t, []string{"left", "right"}, branches)
			}
			for _, op := range ops {
				op.once.Do(func() { close(op.release) })
			}
			for i, op := range ops {
				var r *httptest.ResponseRecorder
				select {
				case r = <-op.done:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				require.Equal(t, 200, r.Code, r.Body.String())
				var got seedgroups.Output
				require.NoError(t, json.Unmarshal(r.Body.Bytes(), &got))
				require.Equal(t, paths[i], got.URL)
				require.Equal(t, []seedgroups.SnapshotRow{{ID: i + 1, Mask: 7 + i}, {ID: i + 1, Mask: 7 + i}}, got.Snapshot)
				require.Equal(t, map[string]int{"a": 1, "b": 1}, got.Lookups)
				sequential := read(op.path, "no-probe", true)
				require.Equal(t, 200, sequential.Code, sequential.Body.String())
				var want seedgroups.Output
				require.NoError(t, json.Unmarshal(sequential.Body.Bytes(), &want))
				require.Equal(t, want, got, "scheduling must not change complete output")
			}
			for _, failure := range []string{"cancellation", "child failure", "loader failure"} {
				t.Run(failure, func(t *testing.T) {
					if failure == "loader failure" {
						r := read(filepath.Join(t.TempDir(), "missing.json"), "no-probe", false)
						require.Equal(t, 500, r.Code, r.Body.String())
						return
					}
					callCtx, cancelCall := context.WithCancel(ctx)
					defer cancelCall()
					entered := make(chan string, 2)
					release := make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					defer unblock()
					probe := &readgroups.Probe{Entered: entered, Release: release}
					if failure == "child failure" {
						probe.Failure = errors.New("child failed")
					}
					name := mode + failure
					remove := readgroups.Install(name, probe)
					defer remove()
					done := make(chan *httptest.ResponseRecorder, 1)
					go func() { done <- readRequest(callCtx, paths[0], name, false) }()
					for range 2 {
						select {
						case <-entered:
						case r := <-done:
							t.Fatalf("early failure: %d %s", r.Code, r.Body.String())
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					if failure == "cancellation" {
						cancelCall()
					} else {
						unblock()
					}
					select {
					case r := <-done:
						require.Equal(t, 500, r.Code, r.Body.String())
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				})
			}
		})
	}
}
