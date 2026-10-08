package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

// Separate binaries prevent test references from retaining the component.
func TestComponentReachabilityRuntimeScan(t *testing.T) {
	for _, tc := range []struct{ name, declarations, want string }{
		{"neither", "", "missing\n"},
		{"instance_only", "var SessionDatly = new(SessionComponent)", "missing\n"},
		{"type_anchor_only", "var _anchorSessionComponent = reflect.TypeFor[SessionComponent]()", "found\n"},
		{"init_named_anchor", "var _anchorSessionComponent reflect.Type\nfunc init() { _anchorSessionComponent = reflect.TypeFor[SessionComponent]() }", "found\n"},
		{"both", "var SessionDatly = new(SessionComponent)\nvar _anchorSessionComponent = reflect.TypeFor[SessionComponent]()", "found\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "example.com/componentreachability"}).Write(t, root)
			if err := os.MkdirAll(filepath.Join(root, "generated"), 0755); err != nil {
				t.Fatal(err)
			}
			source := `package generated
import (
 "reflect"
 "github.com/viant/xdatly"
)
type Input struct{}
type Output struct{}
type SessionComponent struct { Contract xdatly.Component[Input,Output] ` + "`component:\"Session,path=/session,method=GET\"`" + ` }
func SessionDatlyType() reflect.Type { return reflect.TypeOf((*SessionComponent)(nil)).Elem() }
` + tc.declarations
			main := `package main
import (
 "fmt"
 "reflect"
 _ "example.com/componentreachability/generated"
 "github.com/viant/xunsafe"
)
func main(){
 for _,candidate:=range xunsafe.PackageTypes("example.com/componentreachability/generated"){
  if candidate.Kind()==reflect.Pointer {candidate=candidate.Elem()}
  if candidate.Name()=="SessionComponent" {fmt.Println("found");return}
 }
 fmt.Println("missing")
}`
			for name, body := range map[string]string{"generated/component.go": source, "main.go": main} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0644); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("go", "run", "-mod=readonly", ".")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			output, err := command.CombinedOutput()
			if err != nil || string(output) != tc.want {
				t.Fatalf("runtime scan got %q, want %q: %v", output, tc.want, err)
			}
		})
	}
}
