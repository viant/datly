package generate

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/transcribe/gobuild"
)

func TestAuthoredFieldAccessorOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, source, method, value      string
		getter, accessor, gated, invalid bool
	}{
		{"pointer convenience", `func(r *Record)SetLabel(v string){r.Label=&v}`, "SetLabel", "*string", false, true, true, false},
		{"pointer getter", `func(r Record)GetCount()*int{return &r.Count}`, "GetCount", "int", true, true, true, false},
		{"named domain setter", `func(r *Record)SetState(v State){r.State=stateOrdinal(v)}`, "SetState", "int", false, true, true, false},
		{"named domain getter", `func(r *Record)GetState()State{return ordinalState(r.State)}`, "GetState", "int", true, true, true, false},
		{"fluent setter", `func(r *Record)SetLabel(v string)*Record{return r}`, "SetLabel", "*string", false, true, true, false},
		{"nonfactory still strict", `func(r *Record)SetLabel(v string){}`, "SetLabel", "*string", false, true, false, true},
		{"helper still strict", `func(r *Record)HasScheduleChanges()State{return ""}`, "HasScheduleChanges", "bool", true, false, true, true},
		{"value receiver setter", `func(r Record)SetLabel(v string){}`, "SetLabel", "*string", false, true, true, true},
		{"missing setter parameter", `func(r *Record)SetLabel(){}`, "SetLabel", "*string", false, true, true, true},
		{"two setter parameters", `func(r *Record)SetLabel(v string, n int){}`, "SetLabel", "*string", false, true, true, true},
		{"variadic setter", `func(r *Record)SetLabel(v ...string){}`, "SetLabel", "*string", false, true, true, true},
		{"wrong fluent result", `func(r *Record)SetLabel(v string)Record{return Record{}}`, "SetLabel", "*string", false, true, true, true},
		{"getter argument", `func(r *Record)GetLabel(v int)string{return ""}`, "GetLabel", "*string", true, true, true, true},
		{"getter missing result", `func(r *Record)GetLabel(){}`, "GetLabel", "*string", true, true, true, true},
		{"getter extra result", `func(r *Record)GetLabel()(string,error){return "",nil}`, "GetLabel", "*string", true, true, true, true},
		{"duplicate owner", `func(r *Record)SetLabel(v string){};func(r *Record)SetLabel(v string){}`, "SetLabel", "*string", false, true, true, true},
		{"wrong package", `package other;func(r *Record)SetLabel(v string){}`, "SetLabel", "*string", false, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := "package records\n" + tc.source
			if tc.name == "wrong package" {
				src = tc.source
			}
			file, err := parser.ParseFile(token.NewFileSet(), "custom.go", src, 0)
			if err != nil {
				t.Fatal(err)
			}
			key := "Record." + tc.method
			p := &entityMethodOwnership{expected: map[string]EntityMethod{key: {Receiver: "Record", Name: tc.method, ValueType: tc.value, Getter: tc.getter, FieldAccessor: tc.accessor}}, claimed: map[string]string{}, packageName: "records", allowAuthoredAccessorTypes: tc.gated}
			err = p.inspect(file, "custom.go")
			if (err != nil) != tc.invalid {
				t.Fatalf("inspect=%v, invalid=%v", err, tc.invalid)
			}
			if !tc.invalid && p.claimed[key] != "custom.go" {
				t.Fatal("authored declaration did not claim accessor")
			}
		})
	}
}

// The setter package is a sibling of both lookup packages. The preview must
// use the proposed declarations and the final gate must read the private stage.
func TestNativeAccessorSplitValidationBoundaries(t *testing.T) {
	root := t.TempDir()
	const module = "example.test/billing"
	write := func(path, source string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "go.mod"), "module "+module+"\ngo 1.25\n")
	build := &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS="}}
	set := &packageSet{}
	const current = "package model\ntype Row struct{ ID *int }"
	const invalid = "package model\ntype Row struct{ ID *string }"
	for _, entry := range []struct{ pkg, source string }{
		{"component", `package component
import idx "example.test/billing/index"
type Fee struct{ Row idx.Row }
func(f *Fee) SetID(value int){ f.Row.ID=&value }
`},
		{"index", `package index
import "example.test/billing/model"
type Row = model.Row
`},
		{"model", current},
	} {
		dir := filepath.Join(root, entry.pkg)
		stage := filepath.Join(t.TempDir(), entry.pkg)
		path := filepath.Join(dir, "type.go")
		write(path, entry.source)
		write(filepath.Join(stage, "type.go"), entry.source)
		plan := &Plan{Package: module + "/" + entry.pkg}
		set.plans = append(set.plans, plan)
		set.dirs = append(set.dirs, dir)
		set.readRoots = append(set.readRoots, stage)
		set.files = append(set.files, []EmittedFile{{Path: path, Content: entry.source}})
		set.removals = append(set.removals, nil)
		set.templates = append(set.templates, &scaffoldPersistence{dir: dir, owner: "Fee", plan: plan})
	}
	set.plans[0].accessorBuild = build
	set.plans[0].accessorValidationNeeded = true
	write(filepath.Join(set.dirs[2], "type.go"), invalid)
	write(filepath.Join(set.dirs[2], "retired.go"), "package model\ntype Row int")
	set.removals[2] = map[string]bool{"retired.go": true}
	stages := set.readRoots
	set.readRoots = nil
	if err := set.validateProjections(false); err != nil {
		t.Fatalf("preview used stale model: %v", err)
	}
	set.readRoots = stages
	if err := set.validateProjected(); err != nil {
		t.Fatalf("valid stage: %v", err)
	}
	write(filepath.Join(stages[2], "type.go"), invalid)
	if err := set.validateProjected(); err == nil || !strings.Contains(err.Error(), "cannot use") {
		t.Fatalf("invalid staged model: %v", err)
	}
	write(filepath.Join(stages[2], "type.go"), "package model\nimport _ \"example.test/billing/component\"\ntype Row struct{ ID *int }")
	if err := set.validateProjected(); err == nil || !strings.Contains(err.Error(), "import cycle") {
		t.Fatalf("staged import cycle: %v", err)
	}
}
