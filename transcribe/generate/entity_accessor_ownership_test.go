package generate

import (
	"go/parser"
	"go/token"
	"testing"
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
