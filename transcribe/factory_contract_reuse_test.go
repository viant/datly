package transcribe

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func sharedGeneratedFactorySource(t *testing.T, qualified bool) (string, map[string]*Source) {
	t.Helper()
	root, original := generatedPostFactoryFixture(t)
	sources := map[string]*Source{}
	for _, name := range []string{"Archive", "Restore"} {
		copy := *original
		copy.Name = strings.ToLower(name)
		copy.Text = strings.ReplaceAll(original.Text, "'Archive'", "'"+name+"'")
		copy.Text = strings.Replace(copy.Text, "'/archive'", "'/"+strings.ToLower(name)+"'", 1)
		if qualified {
			copy.Text = strings.ReplaceAll(copy.Text, "$input_type('ArchiveInput')", "$input_type('archive.ArchiveInput')")
			copy.Text = strings.ReplaceAll(copy.Text, "$output_type('ArchiveOutput')", "$output_type('archive.ArchiveOutput')")
		}
		copy.Text += fmt.Sprintf("\n#setting($_ = $file_prefix('%s_'))\n#setting($_ = $input_dest('input.go'))\n#setting($_ = $output_dest('output.go'))\n#setting($_ = $support_dest('input_setters','input_setters.go'))", strings.ToLower(name))
		sources[name] = &copy
	}
	return root, sources
}

func TestGeneratedPostFactorySharedContracts(t *testing.T) {
	for _, qualified := range []bool{false, true} {
		for _, first := range []string{"Archive", "Restore"} {
			t.Run(fmt.Sprintf("qualified_%v/first_%s", qualified, first), func(t *testing.T) {
				root, sources := sharedGeneratedFactorySource(t, qualified)
				second := "Restore"
				if first == second {
					second = "Archive"
				}
				var established map[string]string
				for _, name := range []string{first, second, first, second, second, first, second, first} {
					source := sources[name]
					compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
					require.NoError(t, err)
					_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
					require.NoError(t, err)
					snapshot := sourceHandlerSnapshot(t, root)
					if established != nil {
						require.Equal(t, established, snapshot)
					} else if name == second {
						established = snapshot
					}
				}
			})
		}
	}
}

func generateSharedFactory(t *testing.T, root string, source *Source) error {
	t.Helper()
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(t.Context(), source)
	if err != nil {
		return err
	}
	_, err = (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
	return err
}

// Mutations are negative fixtures, never a substitute for generated contracts.
func TestGeneratedPostFactorySharedContractMismatchesDoNotPublish(t *testing.T) {
	root, sources := sharedGeneratedFactorySource(t, false)
	require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
	originals := map[string]string{}
	for _, name := range []string{"input.go", "output.go", "input_setters.go", "archive_router.go"} {
		data, err := os.ReadFile(filepath.Join(root, "archive", name))
		require.NoError(t, err)
		originals[name] = string(data)
	}
	replace := func(old, new string) func(string) string {
		return func(s string) string { require.Contains(t, s, old); return strings.Replace(s, old, new, 1) }
	}
	fieldLine := regexp.MustCompile(`(?m)^\tData[^\n]+\n`)
	markerLine := regexp.MustCompile(`(?m)^\tHas[^\n]+\n`)
	cases := []struct {
		name, file string
		change     func(string) string
	}{
		{"field type", "input.go", replace("*ArchiveRequest", "*ResultItem")},
		{"named package identity", "input.go", func(s string) string {
			return strings.Replace(replace("*ArchiveRequest", "*time.Time")(s), "package archive", "package archive\nimport \"time\"", 1)
		}},
		{"pointer", "input.go", replace("*ArchiveRequest", "ArchiveRequest")},
		{"cardinality", "input.go", replace("*ArchiveRequest", "[]*ArchiveRequest")},
		{"field order", "input.go", func(s string) string {
			data, has := fieldLine.FindString(s), markerLine.FindString(s)
			require.NotEmpty(t, data)
			require.NotEmpty(t, has)
			return strings.Replace(strings.Replace(strings.Replace(s, data, "<data>\n", 1), has, data, 1), "<data>\n", has, 1)
		}},
		{"anonymous embedding", "input.go", func(s string) string {
			return fieldLine.ReplaceAllStringFunc(s, func(line string) string { return regexp.MustCompile(`Data\s+`).ReplaceAllString(line, "") })
		}},
		{"binding kind", "input.go", replace("kind=body", "kind=query")},
		{"binding name", "input.go", replace("in=data", "in=payload")},
		{"JSON tag", "input.go", func(s string) string { return strings.Replace(s, "`parameter:", "`json:\"changed\" parameter:", 1) }},
		{"SQLX tag", "input.go", func(s string) string { return strings.Replace(s, "`parameter:", "`sqlx:\"changed\" parameter:", 1) }},
		{"codec", "input.go", func(s string) string { return strings.Replace(s, "`parameter:", "`codec:\"changed\" parameter:", 1) }},
		{"requiredness", "input.go", replace("required=false", "required=true")},
		{"error code", "input.go", replace("required=false", "required=false,errorCode=422")},
		{"Has closure", "input.go", replace("Data bool", "Data string")},
		{"setter signature", "input_setters.go", replace("value *ArchiveRequest", "value ArchiveRequest")},
		{"setter body", "input_setters.go", replace("input.Has.Data = true", "input.Has.Data = false")},
		{"output independently", "output.go", func(s string) string {
			return regexp.MustCompile(`(?m)^(\tStatus\s+)string\b`).ReplaceAllString(s, "${1}int")
		}},
		{"build directive", "input.go", func(s string) string {
			return strings.Replace(s, "package archive", "//go:build never_selected\n\npackage archive", 1)
		}},
		{"foreign non-contract product", "archive_router.go", func(s string) string { return s }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(root, "archive", tc.file)
			if tc.name == "foreign non-contract product" {
				sources["Restore"].Text = strings.Replace(sources["Restore"].Text, "$file_prefix('restore_')", "$file_prefix('archive_')", 1)
				defer func() {
					sources["Restore"].Text = strings.Replace(sources["Restore"].Text, "$file_prefix('archive_')", "$file_prefix('restore_')", 1)
				}()
			} else {
				changed := tc.change(originals[tc.file])
				require.NotEqual(t, originals[tc.file], changed)
				require.NoError(t, os.WriteFile(file, []byte(changed), 0600))
				defer func() { require.NoError(t, os.WriteFile(file, []byte(originals[tc.file]), 0600)) }()
			}
			before := sourceHandlerSnapshot(t, root)
			require.Error(t, generateSharedFactory(t, root, sources["Restore"]))
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
	t.Run("wrong destination", func(t *testing.T) {
		source := *sources["Restore"]
		source.Text = strings.Replace(source.Text, "$input_dest('input.go')", "$input_dest('other_input.go')", 1)
		before := sourceHandlerSnapshot(t, root)
		require.Error(t, generateSharedFactory(t, root, &source))
		require.Equal(t, before, sourceHandlerSnapshot(t, root))
	})
	t.Run("selected duplicate", func(t *testing.T) {
		writeSourceHandlerFile(t, root, "archive/duplicate.go", "package archive\ntype ArchiveInput struct{}\n")
		defer os.Remove(filepath.Join(root, "archive/duplicate.go"))
		before := sourceHandlerSnapshot(t, root)
		require.ErrorContains(t, generateSharedFactory(t, root, sources["Restore"]), "more than once")
		require.Equal(t, before, sourceHandlerSnapshot(t, root))
	})
	t.Run("unowned", func(t *testing.T) {
		file := filepath.Join(root, "archive/input.go")
		current := originals["input.go"]
		current = strings.SplitN(current, "\n", 2)[1]
		require.NoError(t, os.WriteFile(file, []byte(current), 0600))
		defer os.WriteFile(file, []byte(originals["input.go"]), 0600)
		before := sourceHandlerSnapshot(t, root)
		require.Error(t, generateSharedFactory(t, root, sources["Restore"]))
		require.Equal(t, before, sourceHandlerSnapshot(t, root))
	})
	t.Run("late build failure", func(t *testing.T) {
		file := filepath.Join(root, "archive/business.go")
		current, err := os.ReadFile(file)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(file, append(current, []byte("\nvar broken = unknownSymbol\n")...), 0600))
		defer os.WriteFile(file, current, 0600)
		before := sourceHandlerSnapshot(t, root)
		require.Error(t, generateSharedFactory(t, root, sources["Restore"]))
		require.Equal(t, before, sourceHandlerSnapshot(t, root))
	})
}

func TestGeneratedPostFactorySharedContractJointDiscovery(t *testing.T) {
	for _, mode := range []string{"MCP identities", "HTTP API key"} {
		t.Run(mode, func(t *testing.T) {
			root, sources := sharedGeneratedFactorySource(t, false)
			businessFile := filepath.Join(root, "archive/business.go")
			business, err := os.ReadFile(businessFile)
			require.NoError(t, err)
			code := strings.Replace(string(business), "type ArchiveRequest struct { Ids []string `json:\"ids\"` }", "type ArchiveRequest []string", 1)
			code = strings.Replace(code, "range in.Data.Ids", "range *in.Data", 1)
			code = strings.Replace(code, "\"os\"", "\"os\"\n \"fmt\"", 1)
			code = strings.Replace(code, `out.Status = "ok"`, `out.Status = in.Uri`, 1)
			code = strings.Replace(code, `Id:id,Status:"archived"`, `Id:fmt.Sprintf("%s:%v",id,in.Ids),Status:in.Uri`, 1)
			require.NoError(t, os.WriteFile(businessFile, []byte(code), 0600))
			for name, source := range sources {
				source.Text = strings.Replace(source.Text, "(body/data)", "(body/)", 1)
				source.Text += "\n#define($_ = $Ids<[]int>(query/ids).Optional())\n#define($_ = $Uri<string>(http_request/uri))"
				if mode == "MCP identities" {
					source.Text += fmt.Sprintf("\n#setting($_ = $mcp('%s.shared','%s shared'))", strings.ToLower(name), name)
				} else {
					source.Text += "\n#setting($_ = $api_key('X-Key','fixture-key'))"
				}
			}
			for _, name := range []string{"Archive", "Restore"} {
				require.NoError(t, generateSharedFactory(t, root, sources[name]))
			}
			runtimeTest := sharedFactoryRuntimeTest
			if mode == "HTTP API key" {
				runtimeTest = strings.Replace(runtimeTest, "const requireAPIKey = false", "const requireAPIKey = true", 1)
			}
			writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtimeTest)
			cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-timeout=2m", "./archive")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			require.Contains(t, string(output), "--- PASS: TestSharedFactoryRuntime")
			t.Logf("%s", output)
		})
	}
}

const sharedFactoryRuntimeTest = `package archive
import (
 "context"
 "io"
 "os"
 "path/filepath"
 "net/http/httptest"
 "strings"
 "testing"
 "reflect"
 "fmt"
 "github.com/stretchr/testify/require"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"
)
const requireAPIKey = false
func TestSharedFactoryRuntime(t *testing.T) {
 cwd,err:=os.Getwd();require.NoError(t,err)
 sources,err:=bootstrap.DiscoverComponentsFromPackages(t.Context(),filepath.Dir(cwd),[]string{"github.com/viant/datly/handlerfixture/archive"},nil);require.NoError(t,err);require.Len(t,sources,2)
 keys:=map[string]spec.Key{};mcpNames:=map[string]bool{}
 for _,source:=range sources {require.NoError(t,source.ValidateContractTypes(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]()));component,err:=source.Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err);keys[component.Name]=component.Key;require.Len(t,component.Routes,1);if !requireAPIKey {require.Len(t,component.Routes[0].MCP,1);mcpNames[component.Routes[0].MCP[0].Name]=true}};if !requireAPIKey {require.Equal(t,map[string]bool{"archive.shared":true,"restore.shared":true},mcpNames)}
 for _,eager:=range []bool{false,true} {
 ctx,cancel:=context.WithCancel(t.Context())
 server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:filepath.Dir(cwd),GoBootstrap:&config.Packages{Packages:[]string{"github.com/viant/datly/handlerfixture/archive"},EagerComponents:eager},Endpoint:config.Endpoint{Address:"127.0.0.1:0"}},Holders:[]any{ArchiveDatly,RestoreDatly}});require.NoError(t,err)
 defer func(){cancel();_ = server.Shutdown(context.Background())}()
 done:=make(chan error,1);go func(){done<-server.Serve(ctx,io.Discard)}();_,err=server.WaitReady(ctx);if err!=nil {t.Fatalf("readiness: %v; serve: %v",err,<-done)}
 for _,route:=range []struct{name,path string}{{"Archive","/archive"},{"Restore","/restore"}} {
 for _,key:=range []string{"","wrong","fixture-key"} {
 uri:=route.path+"?ids=11,12";request:=httptest.NewRequest("POST",uri,strings.NewReader("[\"7\"]"));request.Header.Set("Content-Type","application/json");request.Header.Set("X-Key",key);response:=httptest.NewRecorder();server.ServeHTTP(response,request)
 if requireAPIKey && key!="fixture-key" {require.Equal(t,403,response.Code,response.Body.String());continue};require.Equal(t,200,response.Code,response.Body.String());require.JSONEq(t,fmt.Sprintf("{\"status\":%q,\"results\":[{\"id\":\"7:[11 12]\",\"status\":%q}]}",uri,uri),response.Body.String())
 }
 typed,err:=server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:keys[route.name],Route:spec.RouteRef{Method:"POST",Path:route.path}},Input:&ArchiveInput{Data:&ArchiveRequest{"typed"},Ids:[]int{21,22},Uri:route.path}});require.NoError(t,err);require.Equal(t,"typed:[21 22]",typed.(*ArchiveOutput).Results[0].Id);require.Equal(t,route.path,typed.(*ArchiveOutput).Status)
 }
 cancel();require.NoError(t,server.Shutdown(context.Background()));require.NoError(t,<-done)
 }
}
`

func TestGeneratedPostFactorySharedContractBuildSelection(t *testing.T) {
	for _, tc := range []struct {
		name, file, source, env, tags string
		reject                        bool
	}{
		{"excluded authored setter", "custom.go", "//go:build reuse_custom\n\npackage archive\nfunc(*ArchiveInput) SetData(value *ArchiveRequest){}\n", "", "", false},
		{"selected authored setter", "custom.go", "//go:build reuse_custom\n\npackage archive\nfunc(*ArchiveInput) SetData(value *ArchiveRequest){}\n", "", "reuse_custom", true},
		{"GOOS excluded declaration", "extra_windows.go", "package archive\nfunc(*ArchiveInput) SetData(value *ArchiveRequest){}\n", "GOOS=linux", "", false},
		{"GOOS selected declaration", "extra_linux.go", "package archive\ntype ArchiveInput struct{}\n", "GOOS=linux", "", true},
		{"GOARCH excluded declaration", "extra_amd64.go", "package archive\nfunc(*ArchiveInput) SetData(value *ArchiveRequest){}\n", "GOARCH=arm64", "", false},
		{"GOARCH selected declaration", "extra_amd64.go", "package archive\ntype ArchiveInput struct{}\n", "GOARCH=amd64", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, sources := sharedGeneratedFactorySource(t, false)
			require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
			setter := filepath.Join(root, "archive/input_setters.go")
			expected, err := os.ReadFile(setter)
			require.NoError(t, err)
			writeSourceHandlerFile(t, root, "archive/"+tc.file, tc.source)
			source := *sources["Restore"]
			source.GoBuild = source.GoBuild.Clone()
			source.GoBuild.Tags = tc.tags
			if tc.env != "" {
				source.GoBuild.Env = append(source.GoBuild.Env, tc.env, "CGO_ENABLED=0")
			}
			before := sourceHandlerSnapshot(t, root)
			err = generateSharedFactory(t, root, &source)
			if tc.reject {
				require.Error(t, err)
				require.Equal(t, before, sourceHandlerSnapshot(t, root))
			} else {
				require.NoError(t, err)
				actual, err := os.ReadFile(setter)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func TestGeneratedPostFactorySharedContractModfileAndOverlay(t *testing.T) {
	for _, kind := range []string{"modfile readonly", "inherited dependency overlay"} {
		t.Run(kind, func(t *testing.T) {
			root, sources := sharedGeneratedFactorySource(t, false)
			require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
			source := *sources["Restore"]
			source.GoBuild = source.GoBuild.Clone()
			if kind == "modfile readonly" {
				for _, extension := range []string{"mod", "sum"} {
					content, err := os.ReadFile(filepath.Join(root, "go."+extension))
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(filepath.Join(root, "selected."+extension), content, 0600))
				}
				source.GoBuild.Env = append(source.GoBuild.Env, "GOFLAGS=-mod=readonly -modfile=selected.mod")
			} else {
				writeSourceHandlerFile(t, root, "dependency/value.go", "package dependency\nconst Value = missing\n")
				writeSourceHandlerFile(t, root, "overlay/value.go", "package dependency\nconst Value = 42\n")
				writeSourceHandlerFile(t, root, "overlay.json", `{"Replace":{"dependency/value.go":"overlay/value.go"}}`)
				file := filepath.Join(root, "archive/business.go")
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				code := strings.Replace(string(data), `"os"`, `"os"`+"\n dependency \""+handlerFixtureModule+"/dependency\"", 1) + "\nvar _=dependency.Value\n"
				require.NoError(t, os.WriteFile(file, []byte(code), 0600))
				source.GoBuild.Env = append(source.GoBuild.Env, "GOFLAGS=-overlay=overlay.json")
			}
			before := sourceHandlerSnapshot(t, root)
			require.NoError(t, generateSharedFactory(t, root, &source))
			after := sourceHandlerSnapshot(t, root)
			for name, value := range before {
				require.Equal(t, value, after[name], name)
			}
		})
	}
}

func TestGeneratedPostFactorySharedContractExcludedCollision(t *testing.T) {
	root, sources := sharedGeneratedFactorySource(t, false)
	for _, source := range sources {
		source.Text = strings.Replace(source.Text, "'input_setters.go'", "'input_setters_windows.go'", 1)
		source.GoBuild = source.GoBuild.Clone()
		source.GoBuild.Env = append(source.GoBuild.Env, "GOOS=linux", "CGO_ENABLED=0")
	}
	require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
	before := sourceHandlerSnapshot(t, root)
	require.ErrorContains(t, generateSharedFactory(t, root, sources["Restore"]), "not build-selected")
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
}

func TestGeneratedPostFactorySharedContractIndependentDQLMismatch(t *testing.T) {
	root, sources := sharedGeneratedFactorySource(t, false)
	require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
	for _, tc := range []struct{ name, old, replacement string }{
		{"input pointer", "Data<*archive.ArchiveRequest>", "Data<archive.ArchiveRequest>"},
		{"input binding", "(body/data)", "(body/payload)"},
		{"input required", "(body/data).Optional()", "(body/data).Required()"},
		{"output tag", `json:"results"`, `json:"items"`},
		{"output field", "$Status<string>", "$State<string>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := *sources["Restore"]
			source.Text = strings.Replace(source.Text, tc.old, tc.replacement, 1)
			require.NotEqual(t, sources["Restore"].Text, source.Text)
			before := sourceHandlerSnapshot(t, root)
			require.ErrorContains(t, generateSharedFactory(t, root, &source), "independently rendered proposal")
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func TestGeneratedPostFactorySharedContractSameOwnerRepair(t *testing.T) {
	root, sources := sharedGeneratedFactorySource(t, false)
	require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
	file := filepath.Join(root, "archive/input.go")
	original, err := os.ReadFile(file)
	require.NoError(t, err)
	for _, replacement := range []string{"ArchiveRequest", "[]string"} {
		t.Run(replacement, func(t *testing.T) {
			set := token.NewFileSet()
			tree, err := parser.ParseFile(set, file, original, 0)
			require.NoError(t, err)
			var typ *ast.TypeSpec
			for _, decl := range tree.Decls {
				if group, ok := decl.(*ast.GenDecl); ok {
					for _, item := range group.Specs {
						if candidate, ok := item.(*ast.TypeSpec); ok && candidate.Name.Name == "ArchiveInput" {
							typ = candidate
						}
					}
				}
			}
			require.NotNil(t, typ)
			start, end := set.Position(typ.Pos()).Offset, set.Position(typ.End()).Offset
			changed := string(original[:start]) + "ArchiveInput = " + replacement + string(original[end:])
			require.NoError(t, os.WriteFile(file, []byte(changed), 0600))
			before := sourceHandlerSnapshot(t, root)
			require.Error(t, generateSharedFactory(t, root, sources["Restore"]))
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
			require.NoError(t, generateSharedFactory(t, root, sources["Archive"]))
			repaired, err := os.ReadFile(file)
			require.NoError(t, err)
			require.Equal(t, original, repaired)
		})
	}
}
