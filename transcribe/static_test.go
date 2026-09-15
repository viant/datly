package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/internal/testharness"
)

func TestStaticDQLGeneratedBinary(t *testing.T) {
	ctx := context.Background()
	authored, generated := t.TempDir(), t.TempDir()
	(testharness.GeneratedModule{}).Write(t, generated)
	for name, data := range map[string]string{"public/index.html": "<html>embedded</html>", "public/nested/data.txt": "abcdef", "public/.well-known/resource": "hidden-file", "public/binary.bin": "\x00\xff\x01", "public/private/secret.txt": "private-data", "outside.txt": "not-published"} {
		file := filepath.Join(authored, name)
		if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(authored)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	snapshot, err := (packageasset.Snapshotter{Source: root.FS()}).Folder(ctx, ".")
	if err != nil {
		t.Fatal(err)
	}
	store := resource.New()
	if err = store.Register("site", snapshot); err != nil {
		t.Fatal(err)
	}
	source := &Source{Scope: "example.com/site", Name: "Generated", Resources: store, Text: `#setting($_ = $route('/site','GET'))
#setting($_ = $api_key('X-Static','secret'))
#setting($_ = $static_resource('site','public'))`}
	result, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: generated})
	if err != nil {
		t.Fatal(err)
	}
	if result.Result.Plan.Static == nil || result.Result.Plan.Input.Type != "" || result.Result.Plan.Output.Type != "" || len(result.Result.Plan.Views) != 0 || result.Result.Plan.GoHandler != nil {
		t.Fatal("static generation emitted an executable contract")
	}
	pkg := filepath.Join(generated, "generated")
	fixture, err := os.ReadFile("testdata/static_binary/embedded_test.go.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(pkg, "embedded_test.go"), fixture, 0644); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "static.test")
	command := exec.Command("go", "test", "-mod=mod", "-race", "-c", "-o", binary, "./generated")
	command.Dir = generated
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=go1.25.8")
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build generated binary: %v\n%s", err, out)
	}
	if destination := os.Getenv("DATLY_STATIC_BINARY_OUTPUT"); destination != "" {
		data, err := os.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(destination, data, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.RemoveAll(authored); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(generated); err != nil {
		t.Fatal(err)
	}
	command = exec.Command(binary, "-test.v", "-test.run=^TestEmbeddedStaticHTTP$")
	command.Dir = t.TempDir()
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run binary after source removal: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "--- PASS: TestEmbeddedStaticHTTP") {
		t.Fatalf("acceptance did not execute: %s", out)
	}
	t.Logf("DQL -> generated embed.FS -> built binary -> source and generated tree deletion -> standalone HTTP and reload:\n%s", out)
}

func TestStaticDQLRejectsExecution(t *testing.T) {
	for _, tail := range []string{"SELECT 1", `#define($_ = $Id<int>(query/id))`, `#setting($_ = $connector('db'))`, `#setting($_ = $mcp('tool'))`, `#set($x = $Unsafe.Run())`} {
		_, err := NewCompiler().Compile(context.Background(), &Source{Name: "Static", Text: "#setting($_ = $route('/static','GET'))\n#setting($_ = $static_resource('site','.'))\n" + tail})
		if err == nil {
			t.Fatalf("accepted executable/mixed static source %s", tail)
		}
	}
}

func TestStaticRegenerationRemovesObsoleteAssets(t *testing.T) {
	ctx := context.Background()
	destination := t.TempDir()
	(testharness.GeneratedModule{}).Write(t, destination)
	source := &Source{Name: "Generated", Scope: "example.com/static", Text: "#setting($_ = $route('/site','GET'))\n#setting($_ = $static_resource('site','public'))"}
	for revision := 0; revision < 2; revision++ {
		tree := fstest.MapFS{"public/index.html": &fstest.MapFile{Data: []byte("index")}}
		if revision == 0 {
			tree["public/obsolete.txt"] = &fstest.MapFile{Data: []byte("obsolete")}
		}
		source.Resources = resource.New()
		if err := source.Resources.Register("site", tree); err != nil {
			t.Fatal(err)
		}
		if _, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: destination}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(destination, "generated", "datly_assets", "static", "obsolete.txt")); !os.IsNotExist(err) {
		t.Fatalf("obsolete asset retained: %v", err)
	}
}

func TestStaticFilenameControls(t *testing.T) {
	for _, tc := range []struct{ name, settings, router, resources string }{
		{"default", "", "router.go", "resources.go"},
		{"prefix", "#setting($_ = $file_prefix('site_'))", "site_router.go", "site_resources.go"},
		{"override", "#setting($_ = $file_prefix('site_'))\n#setting($_ = $router_dest('routes.go'))\n#setting($_ = $resources_dest('assets.go'))", "routes.go", "assets.go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			store := resource.New()
			if err := store.Register("site", fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("content")}}); err != nil {
				t.Fatal(err)
			}
			source := &Source{Name: "Site", Resources: store, Text: "#setting($_ = $route('/site','GET'))\n#setting($_ = $static_resource('site','.'))\n" + tc.settings}
			if _, err := NewCompiler().Transcribe(context.Background(), Request{Source: source, Destination: root}); err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{tc.router, tc.resources} {
				if _, err := os.Stat(filepath.Join(root, "generated", file)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
