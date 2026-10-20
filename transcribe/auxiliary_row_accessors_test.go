package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGeneratorAuxiliaryRowAccessorsAndNamedDependency(t *testing.T) {
	text := strings.ReplaceAll(auxiliaryOwnerDQL, "'model','github.com/viant/datly/auxownerfixture/model'", "'self','github.com/viant/datly/auxownerfixture/generated'")
	text = strings.ReplaceAll(text, "model.Caller", "self.Caller")
	text = strings.Replace(text, "SELECT r.*", "#define($_ = $Callers<?>(view/Callers).Cardinality('Many').TypeName('Caller').WithColumnType('id','int') /* SELECT id,type_id FROM records */)\nSELECT r.*", 1)
	root, source := auxiliaryOwnerFixture(t, text)
	var previous map[string]string
	for pass := 0; pass < 3; pass++ {
		generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			writeSourceFile(t, root, "generated/lifecycle.go", auxiliaryOwnerHooks)
			continue
		}
		current := map[string]string{}
		for _, file := range generated.Result.Files {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			current[file.Path] = string(data)
		}
		if previous != nil && !reflect.DeepEqual(previous, current) {
			t.Fatal("auxiliary accessor regeneration drift")
		}
		previous = current
	}
	entities, err := os.ReadFile(filepath.Join(root, "generated", "entities.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, signature := range []string{"func (entity *Record) GetId() int", "func (entity *Record) SetId(value int)", "func (entity *Record) GetCaller() *Caller", "func (entity *Record) SetCaller(value *Caller)", "func (entity *Caller) GetId() int", "func (entity *Caller) SetId(value int)"} {
		if strings.Count(string(entities), signature) != 1 {
			t.Fatalf("missing or duplicate owned accessor %s:\n%s", signature, entities)
		}
	}
	for _, physical := range []string{"OriginalInput", "CaptureInput", "synchronize", "OnInsert", "OnUpdate"} {
		if strings.Contains(string(entities), physical) {
			t.Fatalf("auxiliary accessors acquired physical support %s", physical)
		}
	}
	writeSourceFile(t, root, "generated/accessors_runtime_test.go", `package generated
import "testing"
func TestAuxiliarySetters(t *testing.T){
 row:=&Record{};row.SetId(17);row.SetNote("body-only")
 if row.Has==nil||!row.Has.Id||!row.Has.Note||row.GetId()!=17||row.GetNote()!="body-only"{t.Fatal("root accessor presence")}
 caller:=&Caller{};caller.SetId(23)
 if caller.Has==nil||!caller.Has.Id||caller.GetId()!=23{t.Fatal("named dependency accessor presence")}
 row.SetCaller(caller);if row.GetCaller()!=caller||!row.Has.Caller{t.Fatal("named pointer setter")}
 row.SetCaller(nil);if row.GetCaller()!=nil||!row.Has.Caller{t.Fatal("nil pointer presence")}
}
`)
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated auxiliary accessor module: %v\n%s", err, output)
	}
}
