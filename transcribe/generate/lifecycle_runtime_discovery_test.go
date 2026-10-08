package generate

import (
	"github.com/viant/datly/internal/testharness"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Build separate executables so references in the test binary cannot retain the
// lifecycle accidentally. Interface checks select types but do not link them.
func TestLifecycleReachabilityRuntimeScan(t *testing.T) {
	for _, tc := range []struct {
		name        string
		anchored    bool
		declaration string
		reference   string
		want        string
	}{
		{name: "blank_typefor", reference: "var _ = reflect.TypeFor[Lifecycle]()\n", want: "missing\n"},
		{name: "init_blank_typefor", reference: "func init() { _ = reflect.TypeFor[Lifecycle]() }\n", want: "missing\n"},
		{name: "without_anchor", want: "missing\n"},
		{name: "with_generated_anchor", anchored: true, want: "found\n"},
		{name: "blank_interface_assertion", declaration: "type IFace interface { Run() }; var _ IFace = &Lifecycle{}", want: "missing\n"},
		{name: "blank_interface_typed_nil", declaration: "type IFace interface { Run() }; var _ IFace = (*Lifecycle)(nil)", want: "missing\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/reachability"}).Write(t, root)
			if err := os.MkdirAll(filepath.Join(root, "generated"), 0755); err != nil {
				t.Fatal(err)
			}
			plan := &Plan{Package: "example.com/reachability/generated", ComponentName: "Orders", Holder: "OrdersComponent", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), LifecycleTypes: []string{"Lifecycle"}}
			source, err := componentFileText("generated", plan)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.anchored {
				var retained []string
				for _, line := range strings.Split(source, "\n") {
					if !strings.Contains(line, "reflect.TypeFor[Lifecycle]()") {
						retained = append(retained, line)
					}
				}
				source = strings.Join(retained, "\n")
			}
			source += "\n" + tc.reference
			files := map[string]string{
				"generated/component.go": source,
				"generated/hooks.go":     "package generated\ntype Input struct{}\ntype Output struct{}\ntype Lifecycle struct{}\nfunc (*Lifecycle) Run() {}\n" + tc.declaration,
				"main.go": `package main
import (
 "fmt"
 "reflect"
 _ "example.com/reachability/generated"
 "github.com/viant/xunsafe"
)
type runner interface { Run() }
func main() {
 for _, candidate := range xunsafe.PackageTypes("example.com/reachability/generated") {
  if candidate.Kind() == reflect.Pointer { candidate = candidate.Elem() }
  if candidate.Name() == "Lifecycle" && reflect.PointerTo(candidate).Implements(reflect.TypeFor[runner]()) {
   fmt.Println("found")
   return
  }
 }
 fmt.Println("missing")
}
`,
			}
			for path, content := range files {
				if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0644); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("go", "run", "-mod=readonly", ".")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			output, err := command.CombinedOutput()
			if err != nil || string(output) != tc.want {
				t.Fatalf("runtime scan: got %q, want %q, error %v", output, tc.want, err)
			}
		})
	}
}
