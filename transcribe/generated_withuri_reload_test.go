package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

// Original Datly's repository/shape declaration pipeline is the source baseline
// (impl/149 and impl/152). This acceptance crosses discovery, persisted Go and
// resource generation, compiled holder reload, and the public HTTP/MCP owners.
func TestGeneratedWithURIPublicReloadSQLite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, uri, visibility string }{
		{"relative", "/{id}", ""},
		{"absolute", "/records/{id}", ""},
		{"hidden-base", "/{id}", ".WithMCP(false)"},
		{"private-alternative", "/{id}", ".WithPathMCP(false)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const module = "github.com/viant/datly/testfixture/withurireload"
			authored, root := t.TempDir(), t.TempDir()
			(testharness.GeneratedModule{Path: module}).Write(t, authored)
			if err := os.MkdirAll(filepath.Join(authored, "records"), 0755); err != nil {
				t.Fatal(err)
			}
			var stages []string
			var contracts map[string]string
			for revision := 1; revision <= 2; revision++ {
				stage := filepath.Join(root, fmt.Sprint(revision))
				if err := os.MkdirAll(stage, 0755); err != nil {
					t.Fatal(err)
				}
				if revision == 1 {
					(testharness.GeneratedModule{Path: module}).Write(t, stage)
				} else if err := os.CopyFS(stage, os.DirFS(stages[0])); err != nil {
					t.Fatal(err)
				}
				source := fmt.Sprintf(`#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('Records'))
#define($_ = $Id<*int>(path/id).WithURI('%s').WithTag('json:"id"').Required()%s)
SELECT id, CAST(records.id AS int) FROM records
WHERE id <= %d AND ($Id IS NULL OR id = $Id)
ORDER BY id`, tc.uri, tc.visibility, revision+2)
				if err := os.WriteFile(filepath.Join(authored, "records", "Records.sql"), []byte(source), 0644); err != nil {
					t.Fatal(err)
				}
				discovery := &Discovery{BaseDir: authored, Include: []string{module + "/records"}, Connector: "main"}
				project, err := discovery.Compile(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(project.Components) != 1 {
					t.Fatalf("discovered components=%d", len(project.Components))
				}
				compiled := project.Components[0]
				if len(compiled.Component.Routes) != 2 || compiled.Component.Routes[0].Path != "/records" || compiled.Component.Routes[1].Path != "/records/{id}" {
					t.Fatalf("authored WithURI routes: %+v", compiled.Component.Routes)
				}
				input, dir, err := generationInput(stage, "records", compiled)
				if err != nil {
					t.Fatal(err)
				}
				generated, err := NewCompiler().generateInputAt(ctx, stage, dir, compiled, input)
				if err != nil {
					t.Fatal(err)
				}
				plan := generated.Result.Plan
				if len(plan.Routes) != 2 {
					t.Fatalf("generation %d emitted %d routes", revision, len(plan.Routes))
				}
				got := map[string]string{}
				for _, name := range []string{plan.Input.Destination, plan.Output.Destination, plan.ViewDest, plan.RouterDest} {
					data, err := os.ReadFile(filepath.Join(stage, dir, name))
					if err != nil {
						t.Fatal(err)
					}
					got[name] = string(data)
				}
				if revision == 1 {
					contracts = got
				} else if !reflect.DeepEqual(contracts, got) {
					t.Fatal("regeneration changed stable generated Go contracts")
				}
				holders, err := bootstrap.DiscoverComponentsFromPackages(ctx, stage, []string{module + "/records"}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(holders) != 2 {
					t.Fatalf("generation %d holders=%d, want 2", revision, len(holders))
				}
				for index, holder := range holders {
					wantPath := []string{"/records", "/records/{id}"}[index]
					if holder.Tag.Path != wantPath || holder.Tag.Method != "GET" {
						t.Fatalf("generated holder route=%s %s, want GET %s", holder.Tag.Method, holder.Tag.Path, wantPath)
					}
					wantName := []string{"Records", "RecordsById"}[index]
					hidden := index == 0 && tc.name == "hidden-base" || index == 1 && tc.name == "private-alternative"
					if hidden {
						if len(holder.Tag.MCP) != 0 {
							t.Fatalf("generated hidden route %s advertises %+v", wantPath, holder.Tag.MCP)
						}
					} else if len(holder.Tag.MCP) != 1 || holder.Tag.MCP[0].Kind != spec.MCPExposureTool || holder.Tag.MCP[0].Name != wantName {
						t.Fatalf("generated route %s exposure=%+v, want %s", wantPath, holder.Tag.MCP, wantName)
					}
				}
				stages = append(stages, stage)
			}
			data, err := os.ReadFile("testdata/withuri_reload/reload_test.go.txt")
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(stages[0], "records", "reload_test.go"), data, 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "-count=1", "-timeout=120s", "-v", "-run", "^TestWithURIReload$", "./records")
			command.Dir = stages[0]
			command.Env = append(os.Environ(), "GOWORK=off", "DATLY_WITHURI_STAGE="+stages[1], "DATLY_WITHURI_VISIBILITY="+tc.name)
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("generated WithURI acceptance: %v\n%s", err, output)
			}
			for _, protocol := range []string{"http", "mcp-session", "mcp-stateless"} {
				if !strings.Contains(string(output), "--- PASS: TestWithURIReload/"+protocol) {
					t.Fatalf("consumer acceptance did not execute %s: %s", protocol, output)
				}
			}
			t.Logf("compiled generated consumer:\n%s", output)
		})
	}
}
