package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func TestMCPRouteNativeGenerationAndPackageReload(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL)"))
	for _, tc := range []struct {
		name, routes, exposures string
		want                    map[string]string
	}{
		{"one path exposed", `#setting($_ = $route('/records','GET'))
#setting($_ = $route('/records-alias','GET'))`, `#setting($_ = $mcp_route('GET:/records','Records','Base description','docs:tool.md'))`, map[string]string{"GET:/records": "Records", "GET:/records-alias": ""}},
		{"distinct methods", `#setting($_ = $route('/records','GET','POST'))`, `#setting($_ = $mcp_route('GET:/records','ReadRecords','Read records'))
#setting($_ = $mcp_route('POST:/records','PostRecords','Post records'))`, map[string]string{"GET:/records": "ReadRecords", "POST:/records": "PostRecords"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/routeexposure"}).Write(t, root)
			resources := resource.New()
			require.NoError(t, resources.Register("docs", fstest.MapFS{"tool.md": &fstest.MapFile{Data: []byte("Exact route detail")}}))
			text := tc.routes + "\n#setting($_ = $mcp_folder('docs','','docs://routes/'))\n" + tc.exposures + "\nSELECT id FROM records"
			generate := func() *GeneratedPackage {
				result, err := NewCompiler().Transcribe(ctx, Request{Destination: root, Source: &Source{Name: "Records", Scope: "example.com/routeexposure/records", Connector: "main", Text: text, Resources: resources, ColumnRefiner: column.New(column.Connections{"main": db.DB})}})
				require.NoError(t, err)
				return result
			}
			first := generate()
			plan := first.Result.Plan
			expected := map[string][]*spec.MCPExposure{}
			for _, route := range plan.Routes {
				expected[spec.RouteRef{Method: route.Method, Path: route.Path}.String()] = route.MCP
			}
			require.Len(t, plan.Routes, 2)
			snapshots := map[string][]byte{}
			require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					data, e := os.ReadFile(path)
					if e != nil {
						return e
					}
					snapshots[path] = data
				}
				return nil
			}))
			// The ordinary source reload reads generated tags and contracts, without DQL.
			holders, err := bootstrap.DiscoverComponentsFromPackages(ctx, root, []string{plan.Package}, nil)
			require.NoError(t, err)
			require.Len(t, holders, 2)
			for _, holder := range holders {
				identity := spec.RouteRef{Method: holder.Tag.Method, Path: holder.Tag.Path}.String()
				name, present := tc.want[identity]
				require.True(t, present, identity)
				if name == "" {
					require.Empty(t, holder.Tag.MCP)
				} else {
					require.Len(t, holder.Tag.MCP, 1)
					require.Equal(t, name, holder.Tag.MCP[0].Name)
				}
				if len(expected[identity]) > 0 {
					require.Equal(t, expected[identity], holder.Tag.MCP, identity)
				} else {
					require.Empty(t, holder.Tag.MCP)
				}
				require.Equal(t, holders[0].HolderType, holder.HolderType)
				require.Equal(t, holders[0].InputType, holder.InputType)
				require.Equal(t, holders[0].OutputType, holder.OutputType)
				if identity == "GET:/records" && tc.name == "one path exposed" {
					require.Equal(t, "docs:tool.md", holder.Tag.MCP[0].DescriptionPath)
				}
			}
			project, err := (&Discovery{BaseDir: root, Include: []string{plan.Package}}).Compile(ctx)
			require.NoError(t, err)
			require.Len(t, project.Components, 1)
			require.Len(t, project.Components[0].Component.Routes, 2)
			for _, route := range project.Components[0].Component.Routes {
				identity := spec.RouteRef{Method: route.Method, Path: route.Path}.String()
				if len(expected[identity]) > 0 {
					require.Equal(t, expected[identity], route.MCP, identity)
				} else {
					require.Empty(t, route.MCP)
				}
				name := tc.want[identity]
				if name == "" {
					require.Empty(t, route.MCP)
				} else {
					require.Len(t, route.MCP, 1)
					require.Equal(t, name, route.MCP[0].Name)
				}
			}
			second := generate()
			require.Equal(t, plan.Input.Type, second.Result.Plan.Input.Type)
			require.Equal(t, plan.Output.Type, second.Result.Plan.Output.Type)
			for path, want := range snapshots {
				actual, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, want, actual, "regeneration changed %s", path)
			}
			foundDescription := false
			for _, file := range plan.Resources.Files {
				if file.Content == "Exact route detail" {
					foundDescription = true
				}
			}
			require.True(t, foundDescription, "declared description folder must be packaged")
		})
	}
}
