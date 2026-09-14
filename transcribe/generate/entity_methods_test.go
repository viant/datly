package generate

import (
	"go/parser"
	"go/token"
	"testing"
)

func TestEntityHelperMethodOwnership(t *testing.T) {
	for _, test := range []struct {
		name, source string
		invalid      bool
	}{
		{"matching", `func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields renamed.FieldSet) error{return nil}`, false},
		{"value receiver", `func (e Record) BackfillScheduleIfNeeded(prior *Record, fields renamed.FieldSet) error{return nil}`, true},
		{"missing projection", `func (e *Record) BackfillScheduleIfNeeded(prior *Record) error{return nil}`, true},
		{"wrong result", `func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields renamed.FieldSet) bool{return false}`, true},
		{"wrong canonical package", `func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields other.FieldSet) error{return nil}`, true},
		{"variadic", `func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields ...renamed.FieldSet) error{return nil}`, true},
		{"duplicate", `func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields renamed.FieldSet) error{return nil}
func (e *Record) BackfillScheduleIfNeeded(prior *Record, fields renamed.FieldSet) error{return nil}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "custom.go", "package records\nimport renamed \"github.com/viant/xdatly/handler\"\nimport other \"example.com/other\"\n"+test.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			policy := &entityMethodOwnership{expected: map[string]EntityMethod{"Record.BackfillScheduleIfNeeded": {Receiver: "Record", Name: "BackfillScheduleIfNeeded", Signature: "func(*Record, handler.FieldSet) error"}}, claimed: map[string]string{}, imports: map[string]string{"handler": "github.com/viant/xdatly/handler"}, packageName: "records"}
			err = policy.inspect(file, "custom.go")
			if (err != nil) != test.invalid {
				t.Fatalf("inspect = %v", err)
			}
			if !test.invalid && policy.claimed["Record.BackfillScheduleIfNeeded"] != "custom.go" {
				t.Fatal("authored helper not retained")
			}
		})
	}
}
