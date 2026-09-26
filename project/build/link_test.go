package build

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestSyncLinksAddsOnlyMissingBlankImports(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/linkapp"}).Write(t, root)
	for _, name := range []string{"alpha", "beta"} {
		writeLinkTestComponent(t, root, name, true)
	}
	linkDir := filepath.Join(root, "internal", "datlylink")
	if err := os.MkdirAll(linkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(linkDir, "link.go")
	original := "// keep this authored comment\npackage datlylink\n\nimport _ \"example.com/linkapp/alpha\"\n\nconst Authored = \"keep\"\n"
	if err := os.WriteFile(linkPath, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Added) != 1 || result.Added[0] != "example.com/linkapp/beta" {
		t.Fatalf("added=%v", result.Added)
	}
	updated, err := os.ReadFile(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(updated), "// keep this authored comment") || !strings.Contains(string(updated), `const Authored = "keep"`) ||
		strings.Count(string(updated), `"example.com/linkapp/alpha"`) != 1 || strings.Count(string(updated), `"example.com/linkapp/beta"`) != 1 {
		t.Fatalf("link sync removed or duplicated authored content: %s", updated)
	}
	result, err = (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root})
	if err != nil || len(result.Added) != 0 {
		t.Fatalf("second sync=%+v err=%v", result, err)
	}
	again, err := os.ReadFile(linkPath)
	if err != nil || string(again) != string(updated) {
		t.Fatalf("second sync rewrote link file: %v", err)
	}
	writeLinkTestComponent(t, root, "gamma", false)
	writeLinkTestBareComponent(t, root, "delta")
	writeLinkTestInterface(t, root, "predicate")
	writeLinkTestInterface(t, root, "codec")
	fakeDir := filepath.Join(root, "unrelated")
	if err := os.MkdirAll(fakeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeDir, "unrelated.go"), []byte("package unrelated\nimport \"reflect\"\ntype Linked struct{}\nvar Marker = reflect.TypeFor[Linked]()\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ignoreDir := filepath.Join(root, "unlinked")
	if err := os.MkdirAll(ignoreDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ignoreDir, "init.go"), []byte("package unlinked\nfunc init() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err = (Service{}).SyncLinks(context.Background(), LinkRequest{Dir: root})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(result.Added, ",") != "example.com/linkapp/codec,example.com/linkapp/delta,example.com/linkapp/gamma,example.com/linkapp/predicate" {
		t.Fatalf("all linked types were not added: %v", result.Added)
	}
	again, err = os.ReadFile(linkPath)
	if err != nil || !strings.Contains(string(again), `"example.com/linkapp/gamma"`) ||
		!strings.Contains(string(again), `"example.com/linkapp/predicate"`) || !strings.Contains(string(again), `"example.com/linkapp/codec"`) ||
		!strings.Contains(string(again), `"example.com/linkapp/delta"`) || strings.Contains(string(again), `"example.com/linkapp/unlinked"`) || strings.Contains(string(again), `"example.com/linkapp/unrelated"`) {
		t.Fatalf("link sync candidate selection: %v %s", err, again)
	}
	for _, name := range []string{"gamma", "delta", "predicate", "codec"} {
		support, err := os.ReadFile(filepath.Join(root, name, "datly_link_sync.go"))
		if err != nil || !strings.Contains(string(support), "func init()") {
			t.Fatalf("%s link support=%s err=%v", name, support, err)
		}
		if name == "delta" && !strings.Contains(string(support), "reflect.TypeFor[Component]()") {
			t.Fatalf("unanchored component did not get type reachability: %s", support)
		}
	}
	probe := `package datlylink
import (
    "testing"
    "github.com/viant/datly/bootstrap"
)
func TestUnanchoredComponentIsReachable(t *testing.T) {
    if bootstrap.LinkedHolder(nil, "example.com/linkapp/delta", "Component") == nil {
        t.Fatal("link sync did not retain the component holder")
    }
}`
	if err := os.WriteFile(filepath.Join(linkDir, "link_probe_test.go"), []byte(probe), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(context.Background(), "go", "test", "./internal/datlylink")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("linked project did not compile and discover: %v\n%s", err, output)
	}
}

func writeLinkTestComponent(t *testing.T, root, name string, withInit bool) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "package " + name + "\n\nimport (\"reflect\"; xdatly \"github.com/viant/xdatly\")\n" +
		"type Input struct{}\ntype Output struct{}\n" +
		"type Component struct { Contract xdatly.Component[Input,Output] `component:\"read,path=/" + name + ",method=GET,connector=main\"` }\n" +
		"var componentType = reflect.TypeFor[Component]()\n"
	if withInit {
		source += "func init() {}\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "component.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeLinkTestInterface(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := ""
	if name == "predicate" {
		source = `package predicate
import ("context"; xpredicate "github.com/viant/xdatly/predicate")
type Linked struct{}
func (*Linked) Compute(context.Context, any) (*xpredicate.Criteria, error) { return nil, nil }
`
	} else {
		source = `package codec
import ("context"; xcodec "github.com/viant/xdatly/codec")
type Linked struct{}
func (*Linked) Value(context.Context, interface{}, ...xcodec.Option) (interface{}, error) { return nil, nil }
`
	}
	if err := os.WriteFile(filepath.Join(dir, "linked.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeLinkTestBareComponent(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "package " + name + "\nimport xdatly \"github.com/viant/xdatly\"\n" +
		"type Input struct{}\ntype Output struct{}\n" +
		"type Component struct { Contract xdatly.Component[Input,Output] `component:\"read,path=/" + name + ",method=GET,connector=main\"` }\n"
	if err := os.WriteFile(filepath.Join(dir, "component.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
}
