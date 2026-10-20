package golang

import (
	"bytes"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestPureEntityAccessorsCompileAndMarkPresence(t *testing.T) {
	row := &plan.EntityPlan{Owned: true, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "RowHas"}, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "int"}, Writable: true},
		{Name: "Name", Path: plan.FieldPath{"Fields", "Name"}, Type: spec.TypeRef{Name: "*string"}, Writable: true},
		{Name: "Created", Type: spec.TypeRef{Name: "time.Time"}, Writable: true},
		{Name: "Child", Type: spec.TypeRef{Name: "*Child"}, Relation: true},
	}}
	child := &plan.EntityPlan{Owned: true, MarkerField: "Has", MarkerType: spec.TypeRef{Name: "ChildHas"}, Fields: []plan.EntityField{{Name: "Enabled", Type: spec.TypeRef{Name: "bool"}, Writable: true}}}
	asset, err := EntityAccessors([]AccessorRecord{{Type: "Row", Entity: row}, {Type: "Child", Entity: child}, {Type: "Row", Entity: row}, {Type: "foreign.Row", Entity: &plan.EntityPlan{}}}, Config{Package: "rows", Imports: []spec.ImportSpec{{Alias: "time", Package: "time"}}})
	if err != nil {
		t.Fatal(err)
	}
	if asset.CaptureFunction != "" || asset.SnapshotType != "" || len(asset.Associations) != 0 || len(asset.Methods) != 9 {
		t.Fatalf("pure accessor asset acquired physical support or duplicate methods: %+v", asset)
	}
	var source bytes.Buffer
	if err = format.Node(&source, token.NewFileSet(), asset.File); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(source.String(), "SetChild") || strings.Count(source.String(), "func (entity *Row) SetId") != 1 {
		t.Fatalf("relation/dedup accessor contract: %s", source.String())
	}
	dir := t.TempDir()
	for name, content := range map[string]string{"go.mod": "module example.com/rows\n\ngo 1.24\n", "entities.go": source.String(), "entities_test.go": `package rows
import("testing";"time")
type RowHas struct{Id,Name,Created,Child bool}
type Row struct{Id int;Fields struct{Name *string};Created time.Time;Child *Child;Has *RowHas}
type ChildHas struct{Enabled bool}
type Child struct{Enabled bool;Has ChildHas}
func TestPresence(t *testing.T){
 row:=&Row{};row.SetId(7)
 if row.Has==nil||!row.Has.Id||row.GetId()!=7{t.Fatal("nil marker setter")}
 marker:=row.Has;name:="name";row.SetName(&name);now:=time.Now();row.SetCreated(now)
 if row.Has!=marker||!row.Has.Id||!row.Has.Name||!row.Has.Created||row.GetName()!=&name||row.GetCreated()!=now{t.Fatal("pointer/value signatures or presence")}
 row.SetName(nil);if row.GetName()!=nil||!row.Has.Name{t.Fatal("explicit nil presence")}
 row.Child=&Child{};row.GetChild().SetEnabled(false)
 if !row.Child.Has.Enabled||row.Child.GetEnabled(){t.Fatal("value marker setter")}
}
`} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated accessor runtime: %v\n%s\n%s", err, output, source.String())
	}
}

func TestPureEntityAccessorsRejectConflictingSignatures(t *testing.T) {
	entity := func(typ string) *plan.EntityPlan {
		return &plan.EntityPlan{Owned: true, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: typ}}}}
	}
	if _, err := EntityAccessors([]AccessorRecord{{Type: "Row", Entity: entity("int")}, {Type: "Row", Entity: entity("*int")}}, Config{Package: "rows"}); err == nil || !strings.Contains(err.Error(), "conflicting field types") {
		t.Fatalf("conflicting accessor signature accepted: %v", err)
	}
}
